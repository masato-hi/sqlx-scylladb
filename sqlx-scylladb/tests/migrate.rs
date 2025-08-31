use sqlx::migrate::{Migrate, Migration, Migrator};
use sqlx_scylladb::ScyllaDBPool;

static MIGRATOR: Migrator = sqlx::migrate!("./tests/migrations");

#[sqlx::test(migrations = false)]
async fn up_all(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    let _ = conn.ensure_migrations_table().await?;

    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(0, applied_migrations.len());

    MIGRATOR.run(&mut conn).await?;

    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(2, applied_migrations.len());
    assert_eq!(20250831061325, applied_migrations.get(0).unwrap().version);
    assert_eq!(20250831061514, applied_migrations.get(1).unwrap().version);

    Ok(())
}

#[sqlx::test(migrations = false)]
async fn apply_each(pool: ScyllaDBPool) -> anyhow::Result<()> {
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
    assert_eq!(2, applied_migrations.len());
    assert_eq!(20250831061325, applied_migrations.get(0).unwrap().version);
    assert_eq!(20250831061514, applied_migrations.get(1).unwrap().version);

    Ok(())
}

#[sqlx::test(migrations = false)]
async fn revert_each(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    let _ = conn.ensure_migrations_table().await?;

    MIGRATOR.run(&mut conn).await?;

    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(2, applied_migrations.len());

    let mut down_migrations: Vec<Migration> = MIGRATOR
        .iter()
        .cloned()
        .filter(|migration| migration.migration_type.is_down_migration())
        .collect();
    down_migrations.reverse();

    for (i, migration) in down_migrations.iter().enumerate() {
        conn.revert(migration).await?;
        let applied_migrations = conn.list_applied_migrations().await?;
        assert_eq!(1 - i, applied_migrations.len());
    }

    let applied_migrations = conn.list_applied_migrations().await?;
    assert_eq!(0, applied_migrations.len());

    Ok(())
}
