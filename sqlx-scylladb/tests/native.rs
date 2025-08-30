use std::{
    env,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
};

use chrono_04::NaiveDate;
use scylla::value::CqlTimeuuid;
use sqlx::{
    Pool,
    migrate::{MigrateDatabase, Migrator},
};
use sqlx_scylladb::{ScyllaDB, ScyllaDBPoolOptions};
use uuid::Uuid;

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
async fn it_can_select_boolean() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_boolean) VALUES(?, ?)")
        .bind(id)
        .bind(true)
        .execute(&pool)
        .await?;

    let (my_test_id, my_boolean): (Uuid, bool) =
        sqlx::query_as("SELECT my_test_id, my_boolean FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert!(my_boolean);

    Ok(())
}

#[tokio::test]
async fn it_can_select_boolean_array() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_collection(my_test_id, my_boolean_array) VALUES(?, ?)")
        .bind(id)
        .bind(&[true, false, true])
        .execute(&pool)
        .await?;

    let (my_test_id, my_boolean_array): (Uuid, Vec<bool>) = sqlx::query_as(
        "SELECT my_test_id, my_boolean_array FROM test_collection WHERE my_test_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_test_id);
    assert_eq!([false, true], *my_boolean_array);

    Ok(())
}

#[tokio::test]
async fn it_can_select_tinyint() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_tinyint) VALUES(?, ?)")
        .bind(id)
        .bind(117i8)
        .execute(&pool)
        .await?;

    let (my_test_id, my_tinyint): (Uuid, i8) =
        sqlx::query_as("SELECT my_test_id, my_tinyint FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(117, my_tinyint);

    Ok(())
}

#[tokio::test]
async fn it_can_select_smallint() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_smallint) VALUES(?, ?)")
        .bind(id)
        .bind(117i16)
        .execute(&pool)
        .await?;

    let (my_test_id, my_smallint): (Uuid, i16) =
        sqlx::query_as("SELECT my_test_id, my_smallint FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(117, my_smallint);

    Ok(())
}

#[tokio::test]
async fn it_can_select_int() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_int) VALUES(?, ?)")
        .bind(id)
        .bind(117i32)
        .execute(&pool)
        .await?;

    let (my_test_id, my_int): (Uuid, i32) =
        sqlx::query_as("SELECT my_test_id, my_int FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(117, my_int);

    Ok(())
}

#[tokio::test]
async fn it_can_select_bigint() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_bigint) VALUES(?, ?)")
        .bind(id)
        .bind(117i64)
        .execute(&pool)
        .await?;

    let (my_test_id, my_bigint): (Uuid, i64) =
        sqlx::query_as("SELECT my_test_id, my_bigint FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(117, my_bigint);

    Ok(())
}

#[tokio::test]
async fn it_can_select_float() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_float) VALUES(?, ?)")
        .bind(id)
        .bind(11.2f32)
        .execute(&pool)
        .await?;

    let (my_test_id, my_float): (Uuid, f32) =
        sqlx::query_as("SELECT my_test_id, my_float FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(11.2, my_float);

    Ok(())
}

#[tokio::test]
async fn it_can_select_double() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_double) VALUES(?, ?)")
        .bind(id)
        .bind(11.2f64)
        .execute(&pool)
        .await?;

    let (my_test_id, my_double): (Uuid, f64) =
        sqlx::query_as("SELECT my_test_id, my_double FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(11.2, my_double);

    Ok(())
}

#[tokio::test]
async fn it_can_select_ascii() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_ascii) VALUES(?, ?)")
        .bind(id)
        .bind("Hello World!")
        .execute(&pool)
        .await?;

    let (my_test_id, my_ascii): (Uuid, String) =
        sqlx::query_as("SELECT my_test_id, my_ascii FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!("Hello World!", my_ascii);

    Ok(())
}

#[tokio::test]
async fn it_can_select_text() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_text) VALUES(?, ?)")
        .bind(id)
        .bind(String::from("Hello World!").repeat(200))
        .execute(&pool)
        .await?;

    let (my_test_id, my_text): (Uuid, String) =
        sqlx::query_as("SELECT my_test_id, my_text FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(String::from("Hello World!").repeat(200), my_text);

    Ok(())
}

#[tokio::test]
async fn it_can_select_blob() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_blob) VALUES(?, ?)")
        .bind(id)
        .bind(vec![0xEFu8, 0xBB, 0xBF, 0x48, 0x65, 0x6C, 0x6C, 0x6F]) // <BOM>Hello
        .execute(&pool)
        .await?;

    let (my_test_id, my_blob): (Uuid, Vec<u8>) =
        sqlx::query_as("SELECT my_test_id, my_blob FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!([0xEFu8, 0xBB, 0xBF, 0x48, 0x65, 0x6C, 0x6C, 0x6F], *my_blob);

    Ok(())
}

#[tokio::test]
async fn it_can_select_inet() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_inet) VALUES(?, ?)")
        .bind(id)
        .bind(Ipv4Addr::new(192, 0, 2, 1))
        .execute(&pool)
        .await?;

    let (my_test_id, my_inet): (Uuid, IpAddr) =
        sqlx::query_as("SELECT my_test_id, my_inet FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(Ipv4Addr::new(192, 0, 2, 1), my_inet);

    Ok(())
}

#[tokio::test]
async fn it_can_select_uuid() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();
    let my_uuid = Uuid::from_str("a4db9540-37ed-4e0c-a41d-6f8ddd8dc121")?;

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_uuid) VALUES(?, ?)")
        .bind(id)
        .bind(my_uuid)
        .execute(&pool)
        .await?;

    let (my_test_id, my_uuid): (Uuid, Uuid) =
        sqlx::query_as("SELECT my_test_id, my_uuid FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(
        Uuid::from_str("a4db9540-37ed-4e0c-a41d-6f8ddd8dc121")?,
        my_uuid
    );

    Ok(())
}

#[tokio::test]
async fn it_can_select_timeuuid() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();
    let my_timeuuid = CqlTimeuuid::from_str("8e14e760-7fa8-11eb-bc66-000000000001")?;

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_timeuuid) VALUES(?, ?)")
        .bind(id)
        .bind(my_timeuuid)
        .execute(&pool)
        .await?;

    let (my_test_id, my_timeuuid): (Uuid, CqlTimeuuid) =
        sqlx::query_as("SELECT my_test_id, my_timeuuid FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(
        CqlTimeuuid::from_str("8e14e760-7fa8-11eb-bc66-000000000001")?,
        my_timeuuid
    );

    let (my_test_id, my_timeuuid): (Uuid, Uuid) =
        sqlx::query_as("SELECT my_test_id, my_timeuuid FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(
        Uuid::from_str("8e14e760-7fa8-11eb-bc66-000000000001")?,
        my_timeuuid
    );

    Ok(())
}

#[tokio::test]
async fn it_can_select_date() -> anyhow::Result<()> {
    let pool = setup_pool().await?;

    let id = Uuid::new_v4();
    let my_date = NaiveDate::from_str("2006-01-02")?;

    let _ = sqlx::query("INSERT INTO test_native(my_test_id, my_date) VALUES(?, ?)")
        .bind(id)
        .bind(my_date)
        .execute(&pool)
        .await?;

    let (my_test_id, my_date): (Uuid, NaiveDate) =
        sqlx::query_as("SELECT my_test_id, my_date FROM test_native WHERE my_test_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_test_id);
    assert_eq!(NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(), my_date);

    Ok(())
}
