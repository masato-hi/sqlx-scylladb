use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_float(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO float_tests(my_id, my_float, my_float_list, my_float_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(117.5f32)
    .bind([11.5f32, 4.25, 7.125, 11.5])
    .bind([11.5f32, 4.25, 7.125, 11.5])
    .execute(&pool)
    .await?;

    let (my_id, my_float, my_float_list, my_float_set): (Uuid, f32, Vec<f32>, Vec<f32>) =
        sqlx::query_as(
            "SELECT my_id, my_float, my_float_list, my_float_set FROM float_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(117.5, my_float);
    assert_eq!(vec![11.5f32, 4.25, 7.125, 11.5], my_float_list);
    assert_eq!(vec![4.25f32, 7.125, 11.5], my_float_set);

    #[derive(FromRow)]
    struct FloatTest {
        my_id: Uuid,
        my_float: f32,
        my_float_list: Vec<f32>,
        my_float_set: Vec<f32>,
    }

    let row: FloatTest = sqlx::query_as(
        "SELECT my_id, my_float, my_float_list, my_float_set FROM float_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(117.5, row.my_float);
    assert_eq!(vec![11.5f32, 4.25, 7.125, 11.5], row.my_float_list);
    assert_eq!(vec![4.25f32, 7.125, 11.5], row.my_float_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_float_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO float_tests(my_id, my_float, my_float_list, my_float_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<f32>)
    .bind(None::<Vec<f32>>)
    .bind(None::<Vec<f32>>)
    .execute(&pool)
    .await?;

    let (my_id, my_float, my_float_list, my_float_set): (
        Uuid,
        Option<f32>,
        Option<Vec<f32>>,
        Option<Vec<f32>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_float, my_float_list, my_float_set FROM float_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_float.is_none());
    assert!(my_float_list.is_none());
    assert!(my_float_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO float_tests(my_id, my_float, my_float_list, my_float_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(117.5f32))
    .bind(Some([11.5f32, 4.25, 7.125, 11.5]))
    .bind(Some([11.5f32, 4.25, 7.125, 11.5]))
    .execute(&pool)
    .await?;

    let (my_id, my_float, my_float_list, my_float_set): (
        Uuid,
        Option<f32>,
        Option<Vec<f32>>,
        Option<Vec<f32>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_float, my_float_list, my_float_set FROM float_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(117.5f32, my_float.unwrap());
    assert_eq!(vec![11.5f32, 4.25, 7.125, 11.5], my_float_list.unwrap());
    assert_eq!(vec![4.25f32, 7.125, 11.5], my_float_set.unwrap());

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_float(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_float, my_float_list, my_float_set FROM float_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_float", describe.columns()[1].name());
    assert_eq!("my_float_list", describe.columns()[2].name());
    assert_eq!("my_float_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("FLOAT", describe.columns()[1].type_info().name());
    assert_eq!("FLOAT[]", describe.columns()[2].type_info().name());
    assert_eq!("FLOAT[]", describe.columns()[3].type_info().name());

    Ok(())
}
