use sqlx::migrate::{Migrate, Migration, Migrator};
use sqlx_scylladb::ScyllaDBPool;

static MIGRATOR: Migrator = sqlx::migrate!("./tests/migrations");
// Set same value of migrate.table_name in sqlx.toml
const MIGRATION_SCHEMA_NAME: &'static str = "sqlx_migrations";

#[sqlx::test(migrations = false)]
async fn it_can_apply_all_migrations(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    let _ = conn.ensure_migrations_table(MIGRATION_SCHEMA_NAME).await?;

    let applied_migrations = conn.list_applied_migrations(MIGRATION_SCHEMA_NAME).await?;
    assert_eq!(0, applied_migrations.len());

    MIGRATOR.run(&mut conn).await?;

    let applied_migrations = conn.list_applied_migrations(MIGRATION_SCHEMA_NAME).await?;
    assert_eq!(2, applied_migrations.len());
    assert_eq!(20250831061325, applied_migrations.get(0).unwrap().version);
    assert_eq!(20250831061514, applied_migrations.get(1).unwrap().version);

    Ok(())
}

#[sqlx::test(migrations = false)]
async fn it_can_apply_each_migrations(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    let _ = conn.ensure_migrations_table(MIGRATION_SCHEMA_NAME).await?;

    let applied_migrations = conn.list_applied_migrations(MIGRATION_SCHEMA_NAME).await?;
    assert_eq!(0, applied_migrations.len());

    let up_migrations = MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration());

    for (i, migration) in up_migrations.enumerate() {
        conn.apply(MIGRATION_SCHEMA_NAME, migration).await?;
        let applied_migrations = conn.list_applied_migrations(MIGRATION_SCHEMA_NAME).await?;
        assert_eq!(1 + i, applied_migrations.len());
    }

    let applied_migrations = conn.list_applied_migrations(MIGRATION_SCHEMA_NAME).await?;
    assert_eq!(2, applied_migrations.len());
    assert_eq!(20250831061325, applied_migrations.get(0).unwrap().version);
    assert_eq!(20250831061514, applied_migrations.get(1).unwrap().version);

    Ok(())
}

#[sqlx::test(migrations = false)]
async fn it_can_revert_each_migrations(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    let _ = conn.ensure_migrations_table(MIGRATION_SCHEMA_NAME).await?;

    MIGRATOR.run(&mut conn).await?;

    let applied_migrations = conn.list_applied_migrations(MIGRATION_SCHEMA_NAME).await?;
    assert_eq!(2, applied_migrations.len());

    let mut down_migrations: Vec<Migration> = MIGRATOR
        .iter()
        .cloned()
        .filter(|migration| migration.migration_type.is_down_migration())
        .collect();
    down_migrations.reverse();

    for (i, migration) in down_migrations.iter().enumerate() {
        conn.revert(MIGRATION_SCHEMA_NAME, migration).await?;
        let applied_migrations = conn.list_applied_migrations(MIGRATION_SCHEMA_NAME).await?;
        assert_eq!(1 - i, applied_migrations.len());
    }

    let applied_migrations = conn.list_applied_migrations(MIGRATION_SCHEMA_NAME).await?;
    assert_eq!(0, applied_migrations.len());

    Ok(())
}
