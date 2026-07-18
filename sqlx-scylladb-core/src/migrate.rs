use std::{str::FromStr, time::Instant};

use futures_core::future::BoxFuture;
use sqlx::{
    Acquire, AssertSqlSafe, ConnectOptions, Error,
    migrate::{AppliedMigration, Migrate, MigrateDatabase, MigrateError},
};
use sqlx::{Executor, query_as};

use crate::{
    ScyllaDB, ScyllaDBConnectOptions, ScyllaDBConnection, ScyllaDBError,
    ScyllaDBReplicationStrategy,
};

fn parse_for_maintenance(
    url: &str,
) -> Result<
    (
        ScyllaDBConnectOptions,
        (ScyllaDBReplicationStrategy, usize),
        String,
    ),
    Error,
> {
    let mut options = ScyllaDBConnectOptions::from_str(url)?;

    let replication_options = if let Some(replication_strategy) = options.replication_strategy {
        (replication_strategy, options.replication_factor)
    } else {
        return Err(Error::Configuration(
            "replication_strategy is required.".into(),
        ));
    };

    let keyspace = if let Some(keyspace) = &options.keyspace {
        keyspace.clone()
    } else {
        return Err(Error::Configuration("keyspace is required.".into()));
    };

    options.keyspace = None;

    Ok((options, replication_options, keyspace))
}

impl MigrateDatabase for ScyllaDB {
    async fn create_database(url: &str) -> Result<(), sqlx::Error> {
        let (options, (replication_strategy, replication_factor), keyspace) =
            parse_for_maintenance(url)?;
        let mut conn = options.connect().await?;

        const QUERY: &'static str = r#"
                CREATE KEYSPACE IF NOT EXISTS %KEYSPACE_NAME%
                WITH replication = {'class': '%REPLICATION_STRATEGY%', 'replication_factor' : %REPLICATION_FACTOR%}
            "#;
        let query = QUERY
            .replace("%KEYSPACE_NAME%", &keyspace)
            .replace(
                "%REPLICATION_STRATEGY%",
                replication_strategy.to_string().as_str(),
            )
            .replace(
                "%REPLICATION_FACTOR%",
                replication_factor.to_string().as_str(),
            );
        let query = AssertSqlSafe(query);
        let _ = conn.execute(query).await?;

        Ok(())
    }

    async fn database_exists(url: &str) -> Result<bool, sqlx::Error> {
        let (options, _, keyspace) = parse_for_maintenance(url)?;
        let mut conn = options.connect().await?;

        let row: Option<(String,)> =
            query_as("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?")
                .bind(keyspace)
                .fetch_optional(&mut conn)
                .await?;

        let exists = row.is_some();

        Ok(exists)
    }

    async fn drop_database(url: &str) -> Result<(), sqlx::Error> {
        let (options, _, keyspace) = parse_for_maintenance(url)?;
        let mut conn = options.connect().await?;

        const QUERY: &'static str = "DROP KEYSPACE IF EXISTS %KEYSPACE_NAME%";
        let query = QUERY.replace("%KEYSPACE_NAME%", &keyspace);
        let query = AssertSqlSafe(query);
        let _ = conn.execute(query).await?;

        Ok(())
    }
}

impl Migrate for ScyllaDBConnection {
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let row: Option<(String,)> = query_as(
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
            )
            .bind(schema_name)
            .fetch_optional(&mut *self)
            .await?;

            if row.is_some() {
                return Ok(());
            }

            Err(MigrateError::CreateSchemasNotSupported(format!(
                "cannot create new keyspace {schema_name}"
            )))
        })
    }

    fn ensure_migrations_table<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<(), sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            let query = AssertSqlSafe(format!(
                r#"
CREATE TABLE IF NOT EXISTS {table_name} (
    version BIGINT PRIMARY KEY,
    description TEXT,
    installed_on TIMESTAMP,
    success BOOLEAN,
    checksum BLOB,
    execution_time BIGINT
)
                "#
            ));
            self.execute(query).await?;

            Ok(())
        })
    }

    fn dirty_version<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Option<i64>, sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            let query = AssertSqlSafe(format!("SELECT version, success FROM {table_name}"));
            let migrations = query_as::<_, (i64, bool)>(query).fetch_all(self).await?;
            let dirty_migration = migrations
                .iter()
                .filter(|migration| !migration.1)
                .min_by_key(|migration| migration.0)
                .map(|migration| migration.0);

            Ok(dirty_migration)
        })
    }

    fn list_applied_migrations<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Vec<sqlx::migrate::AppliedMigration>, sqlx::migrate::MigrateError>>
    {
        Box::pin(async move {
            let query = AssertSqlSafe(format!("SELECT version, checksum FROM {table_name}"));
            let rows: Vec<(i64, Vec<u8>)> = query_as(query).fetch_all(self).await?;

            let mut migrations: Vec<AppliedMigration> = rows
                .into_iter()
                .map(|(version, checksum)| AppliedMigration {
                    version,
                    checksum: checksum.into(),
                })
                .collect();
            migrations.sort_by_key(|migration| migration.version);

            Ok(migrations)
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), sqlx::migrate::MigrateError>> {
        Box::pin(async {
            const CREATE_LOCK_TABLE_QUERY: &'static str = r#"
                CREATE TABLE IF NOT EXISTS sqlx_advisory_lock (
                    lock_id BIGINT PRIMARY KEY,
                    keyspace_name TEXT
                )
            "#;
            self.execute(CREATE_LOCK_TABLE_QUERY).await?;

            let keyspace = self
                .get_keyspace()
                .ok_or_else(|| Error::Configuration("keyspace is required.".into()))?;

            let lock_id = generate_lock_id(&keyspace);

            const INSERT_LOCK_QUERY: &'static str = r#"
                INSERT INTO sqlx_advisory_lock (lock_id, keyspace_name)
                VALUES (?, ?)
                IF NOT EXISTS
            "#;
            let (applied,): (bool,) = sqlx::query_as(INSERT_LOCK_QUERY)
                .bind(lock_id)
                .bind(keyspace)
                .fetch_one(self)
                .await?;
            if applied {
                Ok(())
            } else {
                Err(sqlx::migrate::MigrateError::Execute(sqlx::Error::Database(
                    Box::new(ScyllaDBError::MigrationLockError),
                )))
            }
        })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), sqlx::migrate::MigrateError>> {
        Box::pin(async {
            let keyspace = self
                .get_keyspace()
                .ok_or_else(|| Error::Configuration("keyspace is required.".into()))?;

            let lock_id = generate_lock_id(&keyspace);

            sqlx::query("DELETE FROM sqlx_advisory_lock WHERE lock_id = ?")
                .bind(lock_id)
                .execute(self)
                .await?;

            Ok(())
        })
    }

    fn apply<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e sqlx::migrate::Migration,
    ) -> BoxFuture<'e, Result<std::time::Duration, sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            let start = Instant::now();

            let conn = self.acquire().await?;

            let query = AssertSqlSafe(format!(
                r#"
INSERT INTO {table_name} ( version, description, success, checksum, execution_time )
VALUES ( ?, ?, FALSE, ?, -1 )
                "#
            ));
            let _ = sqlx::query(query)
                .bind(migration.version)
                .bind(&*migration.description)
                .bind(&*migration.checksum)
                .execute(&mut *conn)
                .await?;

            let _ = conn
                .execute(migration.sql.clone())
                .await
                .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

            let query = AssertSqlSafe(format!(
                "UPDATE {table_name} SET success = TRUE WHERE version = ?"
            ));
            let _ = sqlx::query(query)
                .bind(migration.version)
                .execute(&mut *conn)
                .await?;

            let elapsed = start.elapsed();

            let query = AssertSqlSafe(format!(
                "UPDATE {table_name} SET execution_time = ? WHERE version = ?"
            ));
            let _ = sqlx::query(query)
                .bind(elapsed.as_nanos() as i64)
                .bind(migration.version)
                .execute(&mut *conn)
                .await?;

            Ok(elapsed)
        })
    }

    fn revert<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e sqlx::migrate::Migration,
    ) -> BoxFuture<'e, Result<std::time::Duration, sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            let start = Instant::now();

            let query = AssertSqlSafe(format!(
                "UPDATE {table_name} SET success = FALSE WHERE version = ?"
            ));
            let _ = sqlx::query(query)
                .bind(migration.version)
                .execute(&mut *self)
                .await?;

            let _ = self.execute(migration.sql.clone()).await?;

            let query = AssertSqlSafe(format!("DELETE FROM {table_name} WHERE version = ?"));
            let _ = sqlx::query(query)
                .bind(migration.version)
                .execute(&mut *self)
                .await?;

            let elapsed = start.elapsed();

            Ok(elapsed)
        })
    }
}

// inspired from rails: https://github.com/rails/rails/blob/6e49cc77ab3d16c06e12f93158eaf3e507d4120e/activerecord/lib/active_record/migration.rb#L1308
fn generate_lock_id(keyspace: &str) -> i64 {
    const CRC_IEEE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
    // 0x3d32ad9e chosen by fair dice roll
    0x3d32ad9e * (CRC_IEEE.checksum(keyspace.as_bytes()) as i64)
}
