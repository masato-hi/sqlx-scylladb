use std::env;

use sqlx::{
    Acquire, Column, Executor, Pool, TypeInfo,
    migrate::{MigrateDatabase, Migrator},
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

    let mut conn = pool.acquire().await?;
    MIGRATOR.run(&mut conn).await?;

    Ok(pool)
}

#[tokio::test]
async fn describe_natives() -> anyhow::Result<()> {
    let pool = setup_pool().await?;
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    const QUERY: &'static str = r#"
        SELECT
            my_test_id,
            my_boolean,
            my_tinyint,
            my_smallint,
            my_int,
            my_bigint,
            my_float,
            my_double,
            my_ascii,
            my_text,
            my_blob,
            my_inet,
            my_uuid,
            my_timeuuid,
            my_date,
            my_time,
            my_timestamp,
            my_decimal
        FROM test_native
    "#;
    let d = conn.describe(QUERY).await?;

    assert_eq!("my_test_id", d.columns()[0].name());
    assert_eq!("my_boolean", d.columns()[1].name());
    assert_eq!("my_tinyint", d.columns()[2].name());
    assert_eq!("my_smallint", d.columns()[3].name());
    assert_eq!("my_int", d.columns()[4].name());
    assert_eq!("my_bigint", d.columns()[5].name());
    assert_eq!("my_float", d.columns()[6].name());
    assert_eq!("my_double", d.columns()[7].name());
    assert_eq!("my_ascii", d.columns()[8].name());
    assert_eq!("my_text", d.columns()[9].name());
    assert_eq!("my_blob", d.columns()[10].name());
    assert_eq!("my_inet", d.columns()[11].name());
    assert_eq!("my_uuid", d.columns()[12].name());
    assert_eq!("my_timeuuid", d.columns()[13].name());
    assert_eq!("my_date", d.columns()[14].name());
    assert_eq!("my_time", d.columns()[15].name());
    assert_eq!("my_timestamp", d.columns()[16].name());
    assert_eq!("my_decimal", d.columns()[17].name());

    assert_eq!("UUID", d.columns()[0].type_info().name());
    assert_eq!("BOOLEAN", d.columns()[1].type_info().name());
    assert_eq!("TINYINT", d.columns()[2].type_info().name());
    assert_eq!("SMALLINT", d.columns()[3].type_info().name());
    assert_eq!("INT", d.columns()[4].type_info().name());
    assert_eq!("BIGINT", d.columns()[5].type_info().name());
    assert_eq!("FLOAT", d.columns()[6].type_info().name());
    assert_eq!("DOUBLE", d.columns()[7].type_info().name());
    assert_eq!("ASCII", d.columns()[8].type_info().name());
    assert_eq!("TEXT", d.columns()[9].type_info().name());
    assert_eq!("BLOB", d.columns()[10].type_info().name());
    assert_eq!("INET", d.columns()[11].type_info().name());
    assert_eq!("UUID", d.columns()[12].type_info().name());
    assert_eq!("TIMEUUID", d.columns()[13].type_info().name());
    assert_eq!("DATE", d.columns()[14].type_info().name());
    assert_eq!("TIME", d.columns()[15].type_info().name());
    assert_eq!("TIMESTAMP", d.columns()[16].type_info().name());
    assert_eq!("DECIMAL", d.columns()[17].type_info().name());

    Ok(())
}

#[tokio::test]
async fn describe_collections() -> anyhow::Result<()> {
    let pool = setup_pool().await?;
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    const QUERY: &'static str = r#"
        SELECT
            my_test_id,
            my_list,
            my_set,
            my_map,
            my_tuple
        FROM test_collection
    "#;
    let d = conn.describe(QUERY).await?;

    assert_eq!("my_test_id", d.columns()[0].name());
    assert_eq!("my_list", d.columns()[1].name());
    assert_eq!("my_set", d.columns()[2].name());
    assert_eq!("my_map", d.columns()[3].name());
    assert_eq!("my_tuple", d.columns()[4].name());

    assert_eq!("UUID", d.columns()[0].type_info().name());
    assert_eq!("LIST", d.columns()[1].type_info().name());
    assert_eq!("SET", d.columns()[2].type_info().name());
    assert_eq!("MAP", d.columns()[3].type_info().name());
    assert_eq!("TUPLE", d.columns()[4].type_info().name());

    Ok(())
}

#[tokio::test]
async fn describe_user_defined_types() -> anyhow::Result<()> {
    let pool = setup_pool().await?;
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    const QUERY: &'static str = r#"
        SELECT
            my_test_id,
            my_udt,
            my_udt_set
        FROM test_udt
    "#;
    let d = conn.describe(QUERY).await?;

    assert_eq!("my_test_id", d.columns()[0].name());
    assert_eq!("my_udt", d.columns()[1].name());
    assert_eq!("my_udt_set", d.columns()[2].name());

    assert_eq!("UUID", d.columns()[0].type_info().name());
    assert_eq!("UDT", d.columns()[1].type_info().name());
    assert_eq!("SET", d.columns()[2].type_info().name());

    Ok(())
}
