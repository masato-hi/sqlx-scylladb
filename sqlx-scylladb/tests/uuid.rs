use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use std::str::FromStr;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_uuid(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO uuid_tests(my_id, my_uuid, my_uuid_list, my_uuid_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Uuid::from_str("c53954ff-13aa-412c-844a-b97faca10ef6")?)
    .bind(&[
        Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
        Uuid::from_str("f8e9f4c2-3f5d-4437-920a-8644efb72676")?,
        Uuid::from_str("faab8e0b-9093-4819-86a1-145b66d317c7")?,
        Uuid::from_str("dea32754-e72d-4981-bbf7-3285da65970b")?,
    ])
    .bind(&[
        Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
        Uuid::from_str("faab8e0b-9093-4819-86a1-145b66d317c7")?,
        Uuid::from_str("f8e9f4c2-3f5d-4437-920a-8644efb72676")?,
        Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_uuid, my_uuid_list, my_uuid_set): (Uuid, Uuid, Vec<Uuid>, Vec<Uuid>) =
        sqlx::query_as(
            "SELECT my_id, my_uuid, my_uuid_list, my_uuid_set FROM uuid_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(
        Uuid::from_str("c53954ff-13aa-412c-844a-b97faca10ef6")?,
        my_uuid
    );
    assert_eq!(
        vec![
            Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
            Uuid::from_str("f8e9f4c2-3f5d-4437-920a-8644efb72676")?,
            Uuid::from_str("faab8e0b-9093-4819-86a1-145b66d317c7")?,
            Uuid::from_str("dea32754-e72d-4981-bbf7-3285da65970b")?,
        ],
        my_uuid_list
    );
    assert_eq!(
        vec![
            Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
            Uuid::from_str("f8e9f4c2-3f5d-4437-920a-8644efb72676")?,
            Uuid::from_str("faab8e0b-9093-4819-86a1-145b66d317c7")?,
        ],
        my_uuid_set
    );

    #[derive(FromRow)]
    struct UuidTest {
        my_id: Uuid,
        my_uuid: Uuid,
        my_uuid_list: Vec<Uuid>,
        my_uuid_set: Vec<Uuid>,
    }

    let row: UuidTest = sqlx::query_as(
        "SELECT my_id, my_uuid, my_uuid_list, my_uuid_set FROM uuid_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(
        Uuid::from_str("c53954ff-13aa-412c-844a-b97faca10ef6")?,
        row.my_uuid
    );
    assert_eq!(
        vec![
            Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
            Uuid::from_str("f8e9f4c2-3f5d-4437-920a-8644efb72676")?,
            Uuid::from_str("faab8e0b-9093-4819-86a1-145b66d317c7")?,
            Uuid::from_str("dea32754-e72d-4981-bbf7-3285da65970b")?,
        ],
        row.my_uuid_list
    );
    assert_eq!(
        vec![
            Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
            Uuid::from_str("f8e9f4c2-3f5d-4437-920a-8644efb72676")?,
            Uuid::from_str("faab8e0b-9093-4819-86a1-145b66d317c7")?,
        ],
        row.my_uuid_set
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_uuid(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_uuid, my_uuid_list, my_uuid_set FROM uuid_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_uuid", describe.columns()[1].name());
    assert_eq!("my_uuid_list", describe.columns()[2].name());
    assert_eq!("my_uuid_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("UUID", describe.columns()[1].type_info().name());
    assert_eq!("UUID[]", describe.columns()[2].type_info().name());
    assert_eq!("UUID[]", describe.columns()[3].type_info().name());

    Ok(())
}
