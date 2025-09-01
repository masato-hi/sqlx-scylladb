use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_boolean(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO boolean_tests(my_id, my_boolean, my_boolean_list, my_boolean_set) VALUES(?, ?, ?, ?)")
        .bind(id)
        .bind(true)
        .bind([true, false, true])
        .bind([true, false, true])
        .execute(&pool)
        .await?;

    let (my_id, my_boolean, my_boolean_list, my_boolean_set): (Uuid, bool, Vec<bool>, Vec<bool>) =
        sqlx::query_as("SELECT my_id, my_boolean, my_boolean_list, my_boolean_set FROM boolean_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_id);
    assert_eq!(true, my_boolean);
    assert_eq!(vec![true, false, true], my_boolean_list);
    assert_eq!(vec![false, true], my_boolean_set);

    #[derive(FromRow)]
    struct BooleanTest {
        my_id: Uuid,
        my_boolean: bool,
        my_boolean_list: Vec<bool>,
        my_boolean_set: Vec<bool>,
    }

    let row: BooleanTest =
        sqlx::query_as("SELECT my_id, my_boolean, my_boolean_list, my_boolean_set FROM boolean_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(true, row.my_boolean);
    assert_eq!(vec![true, false, true], row.my_boolean_list);
    assert_eq!(vec![false, true], row.my_boolean_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_boolean_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO boolean_tests(my_id, my_boolean, my_boolean_list, my_boolean_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<bool>)
    .bind(None::<Vec<bool>>)
    .bind(None::<Vec<bool>>)
    .execute(&pool)
    .await?;

    let (my_id, my_boolean, my_boolean_list, my_boolean_set): (
        Uuid,
        Option<bool>,
        Option<Vec<bool>>,
        Option<Vec<bool>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_boolean, my_boolean_list, my_boolean_set FROM boolean_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_boolean.is_none());
    assert!(my_boolean_list.is_none());
    assert!(my_boolean_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO boolean_tests(my_id, my_boolean, my_boolean_list, my_boolean_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(true))
    .bind(Some([true, false, true]))
    .bind(Some([true, false, true]))
    .execute(&pool)
    .await?;

    let (my_id, my_boolean, my_boolean_list, my_boolean_set): (
        Uuid,
        Option<bool>,
        Option<Vec<bool>>,
        Option<Vec<bool>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_boolean, my_boolean_list, my_boolean_set FROM boolean_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(true, my_boolean.unwrap());
    assert_eq!(vec![true, false, true], my_boolean_list.unwrap());
    assert_eq!(vec![false, true], my_boolean_set.unwrap());

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_boolean(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_boolean, my_boolean_list, my_boolean_set FROM boolean_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_boolean", describe.columns()[1].name());
    assert_eq!("my_boolean_list", describe.columns()[2].name());
    assert_eq!("my_boolean_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("BOOLEAN", describe.columns()[1].type_info().name());
    assert_eq!("BOOLEAN[]", describe.columns()[2].type_info().name());
    assert_eq!("BOOLEAN[]", describe.columns()[3].type_info().name());

    Ok(())
}
