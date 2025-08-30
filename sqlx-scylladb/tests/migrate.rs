use std::env;

use sqlx::{
    Pool,
    migrate::{Migrate, MigrateDatabase, Migration, Migrator},
};
use sqlx_scylladb::{ScyllaDB, ScyllaDBPoolOptions};

static MIGRATOR: Migrator = sqlx::migrate!("./tests/migrations");

async fn setup_pool() -> anyhow::Result<Pool<ScyllaDB>> {
    let _ = dotenvy::dotenv();
    let _ = env_logger::builder().is_test(true).try_init();
    let database_url = env::var("DATABASE_URL")?;

    ScyllaDB::drop_database(&database_url).await?;
    ScyllaDB::create_database(&database_url).await?;

    let pool = ScyllaDBPoolOptions::new()
        .min_connections(5)
        .max_connections(5)
        .connect(&database_url)
        .await?;

    Ok(pool)
}

#[tokio::test]
async fn up_all() -> anyhow::Result<()> {
    let pool = setup_pool().await?;
    let mut conn = pool.acquire().await?;

    let _ = conn.ensure_migrations_table().await?;

    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(0, applied_migrations.len());

    MIGRATOR.run(&mut conn).await?;

    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(5, applied_migrations.len());
    assert_eq!(20250726180345, applied_migrations.get(0).unwrap().version);
    assert_eq!(20250729124557, applied_migrations.get(1).unwrap().version);
    assert_eq!(20250808122513, applied_migrations.get(2).unwrap().version);
    assert_eq!(20250808122704, applied_migrations.get(3).unwrap().version);
    assert_eq!(20250808122707, applied_migrations.get(4).unwrap().version);

    Ok(())
}

#[tokio::test]
async fn apply_each() -> anyhow::Result<()> {
    let pool = setup_pool().await?;
    let mut conn = pool.acquire().await?;

    let _ = conn.ensure_migrations_table().await?;

    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(0, applied_migrations.len());

    let up_migrations = MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration());

    for (i, migration) in up_migrations.enumerate() {
        conn.apply(migration).await?;
        let applied_migrations = conn.list_applied_migrations().await?;
        assert_eq!(1 + i, applied_migrations.len());
    }
    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(5, applied_migrations.len());

    assert_eq!(20250726180345, applied_migrations.get(0).unwrap().version);
    assert_eq!(20250729124557, applied_migrations.get(1).unwrap().version);
    assert_eq!(20250808122513, applied_migrations.get(2).unwrap().version);
    assert_eq!(20250808122704, applied_migrations.get(3).unwrap().version);
    assert_eq!(20250808122707, applied_migrations.get(4).unwrap().version);

    Ok(())
}

#[tokio::test]
async fn revert_each() -> anyhow::Result<()> {
    let pool = setup_pool().await?;
    let mut conn = pool.acquire().await?;

    let _ = conn.ensure_migrations_table().await?;

    MIGRATOR.run(&mut conn).await?;

    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(5, applied_migrations.len());

    let mut down_migrations: Vec<Migration> = MIGRATOR
        .iter()
        .cloned()
        .filter(|migration| migration.migration_type.is_down_migration())
        .collect();
    down_migrations.reverse();

    for (i, migration) in down_migrations.iter().enumerate() {
        conn.revert(migration).await?;
        let applied_migrations = conn.list_applied_migrations().await?;
        assert_eq!(4 - i, applied_migrations.len());
    }

    Ok(())
}
