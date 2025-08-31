use std::fmt::Write;
use std::time::SystemTime;
use std::{ops::Deref, str::FromStr, sync::OnceLock, time::Duration};

use base64::{Engine, prelude::BASE64_URL_SAFE};
use futures_core::future::BoxFuture;
use scylla::value::CqlTimestamp;
use sha2::{Digest, Sha512};
use sqlx::{Connection as _, Error, Executor, Pool, pool::PoolOptions};
use sqlx_core::testing::{FixtureSnapshot, TestArgs, TestContext, TestSupport};

use crate::{ScyllaDB, ScyllaDBConnectOptions, ScyllaDBConnection};

// Using a blocking `OnceLock` here because the critical sections are short.
static MASTER_POOL: OnceLock<Pool<ScyllaDB>> = OnceLock::new();

impl TestSupport for ScyllaDB {
    fn test_context(args: &TestArgs) -> BoxFuture<'_, Result<TestContext<Self>, Error>> {
        Box::pin(async move { test_context(args).await })
    }

    fn cleanup_test(db_name: &str) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut conn = MASTER_POOL
                .get()
                .expect("cleanup_test() invoked outside `#[sqlx::test]`")
                .acquire()
                .await?;

            do_cleanup(&mut conn, db_name).await
        })
    }

    fn cleanup_test_dbs() -> BoxFuture<'static, Result<Option<usize>, Error>> {
        Box::pin(async move { cleanup_test_dbs().await })
    }

    fn snapshot(
        _conn: &mut Self::Connection,
    ) -> BoxFuture<'_, Result<FixtureSnapshot<Self>, Error>> {
        todo!()
    }

    fn db_name(args: &TestArgs) -> String {
        let mut hasher = Sha512::new();
        hasher.update(args.test_path.as_bytes());
        let hash = hasher.finalize();
        let hash = BASE64_URL_SAFE.encode(&hash[..27]);
        // Keyspace name is supported lower and less than 48 characters.
        let db_name = format!("sqlx_test_{}", hash)
            .replace('-', "_")
            .to_lowercase();
        debug_assert!(db_name.len() <= 48);
        db_name
    }
}

async fn test_context(args: &TestArgs) -> Result<TestContext<ScyllaDB>, Error> {
    let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let master_opts = ScyllaDBConnectOptions::from_str(&url).expect("failed to parse DATABASE_URL");

    let pool = PoolOptions::new()
        .max_connections(20)
        .after_release(|_conn, _| Box::pin(async move { Ok(false) }))
        .connect_lazy_with(master_opts);

    let master_pool = match once_lock_try_insert_polyfill(&MASTER_POOL, pool) {
        Ok(inserted) => inserted,
        Err((existing, pool)) => {
            assert_eq!(
                existing.connect_options().nodes,
                pool.connect_options().nodes,
                "DATABASE_URL changed at runtime, host differs"
            );

            assert_eq!(
                existing.connect_options().keyspace,
                pool.connect_options().keyspace,
                "DATABASE_URL changed at runtime, database differs"
            );

            existing
        }
    };

    let mut conn = master_pool.acquire().await?;

    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS sqlx_test_databases (
            db_name TEXT PRIMARY KEY,
            test_path TEXT,
            created_at TIMESTAMP
        )
    "#,
    )
    .await?;

    let db_name = ScyllaDB::db_name(args);
    do_cleanup(&mut conn, &db_name).await?;

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System clock is before unix epoch.")
        .as_millis() as i64;
    let timestamp = CqlTimestamp(timestamp);

    sqlx::query("INSERT INTO sqlx_test_databases(db_name, test_path, created_at) values (?, ?, ?)")
        .bind(&db_name)
        .bind(args.test_path)
        .bind(timestamp)
        .execute(&mut *conn)
        .await?;

    conn.execute(format!("CREATE KEYSPACE IF NOT EXISTS {db_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}").as_str()).await?;

    eprintln!("CREATED KEYSPACE {db_name}");

    Ok(TestContext {
        pool_opts: PoolOptions::new()
            .max_connections(5)
            .idle_timeout(Some(Duration::from_secs(1)))
            .parent(master_pool.clone()),
        connect_opts: master_pool
            .connect_options()
            .deref()
            .clone()
            .keyspace(&db_name),
        db_name,
    })
}

async fn do_cleanup(conn: &mut ScyllaDBConnection, db_name: &str) -> Result<(), Error> {
    let delete_db_command = format!("DROP KEYSPACE IF EXISTS {db_name};");
    conn.execute(delete_db_command.as_str()).await?;
    sqlx::query("DELETE FROM sqlx_test_databases WHERE db_name = ?")
        .bind(db_name)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

async fn cleanup_test_dbs() -> Result<Option<usize>, Error> {
    let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let mut conn = ScyllaDBConnection::connect(&url).await?;

    let delete_db_names: Vec<String> =
        sqlx::query_scalar("SELECT db_name from sqlx_test_databases")
            .fetch_all(&mut conn)
            .await?;

    if delete_db_names.is_empty() {
        return Ok(None);
    }

    let mut deleted_db_names = Vec::with_capacity(delete_db_names.len());

    let mut command = String::new();

    for db_name in &delete_db_names {
        command.clear();

        writeln!(command, "drop database if exists {db_name};").ok();
        match conn.execute(&*command).await {
            Ok(_deleted) => {
                deleted_db_names.push(db_name);
            }
            // Assume a database error just means the DB is still in use.
            Err(Error::Database(dbe)) => {
                eprintln!("could not clean test database {db_name:?}: {dbe}")
            }
            // Bubble up other errors
            Err(e) => return Err(e),
        }
    }

    if deleted_db_names.is_empty() {
        return Ok(None);
    }

    sqlx::query("DELETE FROM sqlx_test_databases WHERE db_name IN(?)")
        .bind(delete_db_names.as_slice())
        .execute(&mut conn)
        .await?;

    let _ = conn.close().await;

    Ok(Some(delete_db_names.len()))
}

fn once_lock_try_insert_polyfill<T>(this: &OnceLock<T>, value: T) -> Result<&T, (&T, T)> {
    let mut value = Some(value);
    let res = this.get_or_init(|| value.take().unwrap());
    match value {
        None => Ok(res),
        Some(value) => Err((res, value)),
    }
}
