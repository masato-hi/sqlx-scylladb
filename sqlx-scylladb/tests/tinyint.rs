use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_tinyint(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO tinyint_tests(my_id, my_tinyint, my_tinyint_list, my_tinyint_set) VALUES(?, ?, ?, ?)")
        .bind(id)
        .bind(117i8)
        .bind([11i8, 4, 7,11])
        .bind([11i8, 4, 7, 11])
        .execute(&pool)
        .await?;

    let (my_id, my_tinyint, my_tinyint_list, my_tinyint_set): (Uuid, i8, Vec<i8>, Vec<i8>) =
        sqlx::query_as("SELECT my_id, my_tinyint, my_tinyint_list, my_tinyint_set FROM tinyint_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_id);
    assert_eq!(117, my_tinyint);
    assert_eq!(vec![11, 4, 7, 11], my_tinyint_list);
    assert_eq!(vec![4, 7, 11], my_tinyint_set);

    #[derive(FromRow)]
    struct TinyIntTest {
        my_id: Uuid,
        my_tinyint: i8,
        my_tinyint_list: Vec<i8>,
        my_tinyint_set: Vec<i8>,
    }

    let row: TinyIntTest =
        sqlx::query_as("SELECT my_id, my_tinyint, my_tinyint_list, my_tinyint_set FROM tinyint_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(117, row.my_tinyint);
    assert_eq!(vec![11, 4, 7, 11], row.my_tinyint_list);
    assert_eq!(vec![4, 7, 11], row.my_tinyint_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_tinyint_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO tinyint_tests(my_id, my_tinyint, my_tinyint_list, my_tinyint_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<i8>)
    .bind(None::<Vec<i8>>)
    .bind(None::<Vec<i8>>)
    .execute(&pool)
    .await?;

    let (my_id, my_tinyint, my_tinyint_list, my_tinyint_set): (
        Uuid,
        Option<i8>,
        Option<Vec<i8>>,
        Option<Vec<i8>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_tinyint, my_tinyint_list, my_tinyint_set FROM tinyint_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_tinyint.is_none());
    assert!(my_tinyint_list.is_none());
    assert!(my_tinyint_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO tinyint_tests(my_id, my_tinyint, my_tinyint_list, my_tinyint_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(117i8))
    .bind(Some([11i8, 4, 7, 11]))
    .bind(Some([11i8, 4, 7, 11]))
    .execute(&pool)
    .await?;

    let (my_id, my_tinyint, my_tinyint_list, my_tinyint_set): (
        Uuid,
        Option<i8>,
        Option<Vec<i8>>,
        Option<Vec<i8>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_tinyint, my_tinyint_list, my_tinyint_set FROM tinyint_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(117, my_tinyint.unwrap());
    assert_eq!(vec![11, 4, 7, 11], my_tinyint_list.unwrap());
    assert_eq!(vec![4, 7, 11], my_tinyint_set.unwrap());

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_tinyint(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_tinyint, my_tinyint_list, my_tinyint_set FROM tinyint_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_tinyint", describe.columns()[1].name());
    assert_eq!("my_tinyint_list", describe.columns()[2].name());
    assert_eq!("my_tinyint_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("TINYINT", describe.columns()[1].type_info().name());
    assert_eq!("TINYINT[]", describe.columns()[2].type_info().name());
    assert_eq!("TINYINT[]", describe.columns()[3].type_info().name());

    Ok(())
}
