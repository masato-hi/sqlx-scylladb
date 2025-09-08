pub mod text;
pub mod uuid;

use std::env;

use scylla::client::{
    caching_session::{CachingSession, CachingSessionBuilder},
    session_builder::SessionBuilder,
};
use sqlx_scylladb_core::{ScyllaDBPool, ScyllaDBPoolOptions};

pub(crate) async fn setup_scylla_session() -> anyhow::Result<CachingSession> {
    let _ = dotenvy::dotenv();
    let scylla_uri = env::var("SCYLLA_URI")?;
    let keyspace = env::var("SCYLLA_KEYSPACE")?;

    let session = SessionBuilder::new()
        .known_node(scylla_uri)
        .use_keyspace(keyspace, true)
        .build()
        .await?;
    let session = CachingSessionBuilder::new(session).build();

    Ok(session)
}

pub(crate) async fn setup_sqlx_scylladb_pool() -> anyhow::Result<ScyllaDBPool> {
    let _ = dotenvy::dotenv();
    let database_url = env::var("SCYLLADB_URL")?;

    let pool = ScyllaDBPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await?;

    Ok(pool)
}
