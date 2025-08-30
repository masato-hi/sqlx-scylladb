use std::{str::FromStr, time::Instant};

use futures_core::future::BoxFuture;
use sqlx::{
    Acquire, ConnectOptions, Error,
    migrate::{AppliedMigration, Migrate, MigrateDatabase, MigrateError},
};
use sqlx::{Executor, query_as};

use crate::{
    ScyllaDB, ScyllaDBConnectOptions, ScyllaDBConnection, ScyllaDBError,
    options::ScyllaDBReplicationOptions,
};

fn parse_for_maintenance(
    url: &str,
) -> Result<(ScyllaDBConnectOptions, ScyllaDBReplicationOptions, String), Error> {
    let mut options = ScyllaDBConnectOptions::from_str(url)?;

    let replication_options = options.replication_options.clone().unwrap();

    let keyspace = options.keyspace.clone().unwrap();
    options.keyspace = None;

    Ok((options, replication_options, keyspace))
}

impl MigrateDatabase for ScyllaDB {
    fn create_database(url: &str) -> BoxFuture<'_, Result<(), sqlx::Error>> {
        Box::pin(async move {
            let (options, replication_options, keyspace) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            const QUERY: &'static str = r#"
                CREATE KEYSPACE IF NOT EXISTS %KEYSPACE_NAME%
                WITH replication = {'class': '%REPLICATION_STRATEGY%', 'replication_factor' : %REPLICATION_FACTOR%}
            "#;
            let query = QUERY
                .replace("%KEYSPACE_NAME%", &keyspace)
                .replace(
                    "%REPLICATION_STRATEGY%",
                    replication_options.strategy.to_string().as_str(),
                )
                .replace(
                    "%REPLICATION_FACTOR%",
                    replication_options.replication_factor.to_string().as_str(),
                );
            let _ = conn.execute(query.as_str()).await?;

            Ok(())
        })
    }

    fn database_exists(url: &str) -> BoxFuture<'_, Result<bool, sqlx::Error>> {
        Box::pin(async move {
            let (options, _, keyspace) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            let row: Option<(String,)> = query_as(
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
            )
            .bind(keyspace)
            .fetch_optional(&mut conn)
            .await?;

            let exists = row.is_some();

            Ok(exists)
        })
    }

    fn drop_database(url: &str) -> BoxFuture<'_, Result<(), sqlx::Error>> {
        Box::pin(async move {
            let (options, _, keyspace) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            const QUERY: &'static str = "DROP KEYSPACE IF EXISTS %KEYSPACE_NAME%";
            let query = QUERY.replace("%KEYSPACE_NAME%", &keyspace);
            let _ = conn.execute(query.as_str()).await?;

            Ok(())
        })
    }
}

impl Migrate for ScyllaDBConnection {
    fn ensure_migrations_table(
        &mut self,
    ) -> BoxFuture<'_, Result<(), sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            const QUERY: &'static str = r#"
                CREATE TABLE IF NOT EXISTS sqlx_migrations (
                    version BIGINT PRIMARY KEY,
                    description TEXT,
                    installed_on TIMESTAMP,
                    success BOOLEAN,
                    checksum BLOB,
                    execution_time BIGINT
                )
            "#;
            self.execute(QUERY).await?;

            Ok(())
        })
    }

    fn dirty_version(&mut self) -> BoxFuture<'_, Result<Option<i64>, sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            let migrations =
                query_as::<_, (i64, bool)>("SELECT version, success FROM sqlx_migrations")
                    .fetch_all(self)
                    .await?;
            let dirty_migration = migrations
                .iter()
                .filter(|migration| !migration.1)
                .min_by_key(|migration| migration.0)
                .map(|migration| migration.0);

            Ok(dirty_migration)
        })
    }

    fn list_applied_migrations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<sqlx::migrate::AppliedMigration>, sqlx::migrate::MigrateError>>
    {
        Box::pin(async move {
            let rows: Vec<(i64, Vec<u8>)> =
                query_as("SELECT version, checksum FROM sqlx_migrations")
                    .fetch_all(self)
                    .await?;

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

            let keyspace = self.get_keyspace().unwrap();

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
            let keyspace = self.get_keyspace().unwrap();

            let lock_id = generate_lock_id(&keyspace);

            sqlx::query("DELETE FROM sqlx_advisory_lock WHERE lock_id = ?")
                .bind(lock_id)
                .execute(self)
                .await?;

            Ok(())
        })
    }

    fn apply<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m sqlx::migrate::Migration,
    ) -> BoxFuture<'m, Result<std::time::Duration, sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            let start = Instant::now();

            let conn = self.acquire().await?;

            const QUERY: &'static str = r#"
                INSERT INTO sqlx_migrations ( version, description, success, checksum, execution_time )
                VALUES ( ?, ?, FALSE, ?, -1 )
            "#;
            let _ = sqlx::query(QUERY)
                .bind(migration.version)
                .bind(&*migration.description)
                .bind(&*migration.checksum)
                .execute(&mut *conn)
                .await?;

            let _ = conn
                .execute(&*migration.sql)
                .await
                .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

            let _ = sqlx::query("UPDATE sqlx_migrations SET success = TRUE WHERE version = ?")
                .bind(migration.version)
                .execute(&mut *conn)
                .await?;

            let elapsed = start.elapsed();

            let _ = sqlx::query("UPDATE sqlx_migrations SET execution_time = ? WHERE version = ?")
                .bind(elapsed.as_nanos() as i64)
                .bind(migration.version)
                .execute(&mut *conn)
                .await?;

            Ok(elapsed)
        })
    }

    fn revert<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m sqlx::migrate::Migration,
    ) -> BoxFuture<'m, Result<std::time::Duration, sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            let start = Instant::now();

            let _ = sqlx::query("UPDATE sqlx_migrations SET success = FALSE WHERE version = ?")
                .bind(migration.version)
                .execute(&mut *self)
                .await?;

            let _ = self.execute(&*migration.sql).await?;

            let _ = sqlx::query("DELETE FROM sqlx_migrations WHERE version = ?")
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
