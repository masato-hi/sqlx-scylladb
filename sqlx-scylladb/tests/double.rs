use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_double(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO double_tests(my_id, my_double, my_double_list, my_double_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(117.5f64)
    .bind([11.5f64, 4.25, 7.125, 11.5])
    .bind([11.5f64, 4.25, 7.125, 11.5])
    .execute(&pool)
    .await?;

    let (my_id, my_double, my_double_list, my_double_set): (Uuid, f64, Vec<f64>, Vec<f64>) =
        sqlx::query_as(
            "SELECT my_id, my_double, my_double_list, my_double_set FROM double_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(117.5, my_double);
    assert_eq!(vec![11.5f64, 4.25, 7.125, 11.5], my_double_list);
    assert_eq!(vec![4.25f64, 7.125, 11.5], my_double_set);

    #[derive(FromRow)]
    struct DoubleTest {
        my_id: Uuid,
        my_double: f64,
        my_double_list: Vec<f64>,
        my_double_set: Vec<f64>,
    }

    let row: DoubleTest = sqlx::query_as(
        "SELECT my_id, my_double, my_double_list, my_double_set FROM double_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(117.5, row.my_double);
    assert_eq!(vec![11.5f64, 4.25, 7.125, 11.5], row.my_double_list);
    assert_eq!(vec![4.25f64, 7.125, 11.5], row.my_double_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_double(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_double, my_double_list, my_double_set FROM double_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_double", describe.columns()[1].name());
    assert_eq!("my_double_list", describe.columns()[2].name());
    assert_eq!("my_double_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("DOUBLE", describe.columns()[1].type_info().name());
    assert_eq!("DOUBLE[]", describe.columns()[2].type_info().name());
    assert_eq!("DOUBLE[]", describe.columns()[3].type_info().name());

    Ok(())
}
