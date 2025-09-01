use std::env;

use sqlx::{
    FromRow,
    migrate::{MigrateDatabase, Migrator},
};
use sqlx_scylladb::{ScyllaDB, ScyllaDBExecutor, ScyllaDBPoolOptions};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(FromRow)]
struct User {
    id: i64,
    name: String,
}

async fn create_user(
    conn: impl ScyllaDBExecutor<'_>,
    id: i64,
    name: impl Into<String>,
) -> anyhow::Result<User> {
    let user = User {
        id,
        name: name.into(),
    };

    sqlx::query("INSERT INTO users(id, name) VALUES(?, ?)")
        .bind(id)
        .bind(user.name.as_str())
        .execute(conn)
        .await?;

    Ok(user)
}

async fn find_user(conn: impl ScyllaDBExecutor<'_>, id: i64) -> anyhow::Result<User> {
    let user = sqlx::query_as::<_, User>("SELECT id, name FROM users WHERE id = ?")
        .bind(id)
        .fetch_one(conn)
        .await?;

    Ok(user)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = env::var("SCYLLADB_URL")?;

    ScyllaDB::create_database(&database_url).await?;

    let pool = ScyllaDBPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    MIGRATOR.run(&pool).await?;

    let _ = create_user(&pool, 1, "Alice").await?;

    let user = find_user(&pool, 1).await?;

    println!("id: {}, name: {}", user.id, user.name);

    Ok(())
}
