use sqlx::{Acquire, Column, Executor, FromRow, TypeInfo};
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_bigint(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO bigint_tests(my_id, my_bigint, my_bigint_list, my_bigint_set) VALUES(?, ?, ?, ?)")
        .bind(id)
        .bind(117i64)
        .bind([11i64, 4, 7, 11])
        .bind([11i64, 4, 7, 11])
        .execute(&pool)
        .await?;

    let (my_id, my_bigint, my_bigint_list, my_bigint_set): (Uuid, i64, Vec<i64>, Vec<i64>) =
        sqlx::query_as("SELECT my_id, my_bigint, my_bigint_list, my_bigint_set FROM bigint_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_id);
    assert_eq!(117, my_bigint);
    assert_eq!(vec![11, 4, 7, 11], my_bigint_list);
    assert_eq!(vec![4, 7, 11], my_bigint_set);

    #[derive(FromRow)]
    struct BigIntTest {
        my_id: Uuid,
        my_bigint: i64,
        my_bigint_list: Vec<i64>,
        my_bigint_set: Vec<i64>,
    }

    let row: BigIntTest = sqlx::query_as(
        "SELECT my_id, my_bigint, my_bigint_list, my_bigint_set FROM bigint_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(117, row.my_bigint);
    assert_eq!(vec![11, 4, 7, 11], row.my_bigint_list);
    assert_eq!(vec![4, 7, 11], row.my_bigint_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_bigint_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO bigint_tests(my_id, my_bigint, my_bigint_list, my_bigint_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<i64>)
    .bind(None::<Vec<i64>>)
    .bind(None::<Vec<i64>>)
    .execute(&pool)
    .await?;

    let (my_id, my_bigint, my_bigint_list, my_bigint_set): (
        Uuid,
        Option<i64>,
        Option<Vec<i64>>,
        Option<Vec<i64>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_bigint, my_bigint_list, my_bigint_set FROM bigint_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_bigint.is_none());
    assert!(my_bigint_list.is_none());
    assert!(my_bigint_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO bigint_tests(my_id, my_bigint, my_bigint_list, my_bigint_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(117i64))
    .bind(Some([11i64, 4, 7, 11]))
    .bind(Some([11i64, 4, 7, 11]))
    .execute(&pool)
    .await?;

    let (my_id, my_bigint, my_bigint_list, my_bigint_set): (
        Uuid,
        Option<i64>,
        Option<Vec<i64>>,
        Option<Vec<i64>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_bigint, my_bigint_list, my_bigint_set FROM bigint_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(117, my_bigint.unwrap());
    assert_eq!(vec![11, 4, 7, 11], my_bigint_list.unwrap());
    assert_eq!(vec![4, 7, 11], my_bigint_set.unwrap());

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_bigint(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_bigint, my_bigint_list, my_bigint_set FROM bigint_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_bigint", describe.columns()[1].name());
    assert_eq!("my_bigint_list", describe.columns()[2].name());
    assert_eq!("my_bigint_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("BIGINT", describe.columns()[1].type_info().name());
    assert_eq!("BIGINT[]", describe.columns()[2].type_info().name());
    assert_eq!("BIGINT[]", describe.columns()[3].type_info().name());

    Ok(())
}
