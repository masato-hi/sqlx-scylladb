use scylla::value::Counter;
use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_counter(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query("UPDATE counter_tests SET my_counter = my_counter + 2 WHERE my_id = ?")
        .bind(id)
        .bind(2)
        .execute(&pool)
        .await?;

    let (my_id, my_counter): (Uuid, Counter) =
        sqlx::query_as("SELECT my_id, my_counter FROM counter_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_id);
    assert_eq!(2, my_counter.0);

    #[derive(FromRow)]
    struct CounterTest {
        my_id: Uuid,
        my_counter: Counter,
    }

    let row: CounterTest =
        sqlx::query_as("SELECT my_id, my_counter FROM counter_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(2, row.my_counter.0);

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_counter(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_counter FROM counter_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_counter", describe.columns()[1].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("COUNTER", describe.columns()[1].type_info().name());

    Ok(())
}
