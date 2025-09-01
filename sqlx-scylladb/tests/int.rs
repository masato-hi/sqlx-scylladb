use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_int(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO int_tests(my_id, my_int, my_int_list, my_int_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(117i32)
    .bind([11i32, 4, 7, 11])
    .bind([11i32, 4, 7, 11])
    .execute(&pool)
    .await?;

    let (my_id, my_int, my_int_list, my_int_set): (Uuid, i32, Vec<i32>, Vec<i32>) = sqlx::query_as(
        "SELECT my_id, my_int, my_int_list, my_int_set FROM int_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(117, my_int);
    assert_eq!(vec![11, 4, 7, 11], my_int_list);
    assert_eq!(vec![4, 7, 11], my_int_set);

    #[derive(FromRow)]
    struct IntTest {
        my_id: Uuid,
        my_int: i32,
        my_int_list: Vec<i32>,
        my_int_set: Vec<i32>,
    }

    let row: IntTest = sqlx::query_as(
        "SELECT my_id, my_int, my_int_list, my_int_set FROM int_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(117, row.my_int);
    assert_eq!(vec![11, 4, 7, 11], row.my_int_list);
    assert_eq!(vec![4, 7, 11], row.my_int_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_int(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_int, my_int_list, my_int_set FROM int_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_int", describe.columns()[1].name());
    assert_eq!("my_int_list", describe.columns()[2].name());
    assert_eq!("my_int_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("INT", describe.columns()[1].type_info().name());
    assert_eq!("INT[]", describe.columns()[2].type_info().name());
    assert_eq!("INT[]", describe.columns()[3].type_info().name());

    Ok(())
}
