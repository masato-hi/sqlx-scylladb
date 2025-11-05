use sqlx::{Acquire, Column, Executor, FromRow, TypeInfo};
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_smallint(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO smallint_tests(my_id, my_smallint, my_smallint_list, my_smallint_set) VALUES(?, ?, ?, ?)")
        .bind(id)
        .bind(117i16)
        .bind([11i16, 4, 7,11])
        .bind([11i16, 4, 7, 11])
        .execute(&pool)
        .await?;

    let (my_id, my_smallint, my_smallint_list, my_smallint_set): (Uuid, i16, Vec<i16>, Vec<i16>) =
        sqlx::query_as("SELECT my_id, my_smallint, my_smallint_list, my_smallint_set FROM smallint_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_id);
    assert_eq!(117, my_smallint);
    assert_eq!(vec![11, 4, 7, 11], my_smallint_list);
    assert_eq!(vec![4, 7, 11], my_smallint_set);

    #[derive(FromRow)]
    struct SmallIntTest {
        my_id: Uuid,
        my_smallint: i16,
        my_smallint_list: Vec<i16>,
        my_smallint_set: Vec<i16>,
    }

    let row: SmallIntTest =
        sqlx::query_as("SELECT my_id, my_smallint, my_smallint_list, my_smallint_set FROM smallint_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(117, row.my_smallint);
    assert_eq!(vec![11, 4, 7, 11], row.my_smallint_list);
    assert_eq!(vec![4, 7, 11], row.my_smallint_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_smallint_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO smallint_tests(my_id, my_smallint, my_smallint_list, my_smallint_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<i16>)
    .bind(None::<Vec<i16>>)
    .bind(None::<Vec<i16>>)
    .execute(&pool)
    .await?;

    let (my_id, my_smallint, my_smallint_list, my_smallint_set): (
        Uuid,
        Option<i16>,
        Option<Vec<i16>>,
        Vec<i16>,
    ) = sqlx::query_as(
        "SELECT my_id, my_smallint, my_smallint_list, my_smallint_set FROM smallint_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_smallint.is_none());
    assert!(my_smallint_list.is_none());
    assert!(my_smallint_set.is_empty());

    let _ = sqlx::query(
        "INSERT INTO smallint_tests(my_id, my_smallint, my_smallint_list, my_smallint_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(117i16))
    .bind(Some([11i16, 4, 7, 11]))
    .bind(Some([11i16, 4, 7, 11]))
    .execute(&pool)
    .await?;

    let (my_id, my_smallint, my_smallint_list, my_smallint_set): (
        Uuid,
        Option<i16>,
        Option<Vec<i16>>,
        Vec<i16>,
    ) = sqlx::query_as(
        "SELECT my_id, my_smallint, my_smallint_list, my_smallint_set FROM smallint_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(117, my_smallint.unwrap());
    assert_eq!(vec![11, 4, 7, 11], my_smallint_list.unwrap());
    assert_eq!(vec![4, 7, 11], my_smallint_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_smallint(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe(
            "SELECT my_id, my_smallint, my_smallint_list, my_smallint_set FROM smallint_tests",
        )
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_smallint", describe.columns()[1].name());
    assert_eq!("my_smallint_list", describe.columns()[2].name());
    assert_eq!("my_smallint_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("SMALLINT", describe.columns()[1].type_info().name());
    assert_eq!("SMALLINT[]", describe.columns()[2].type_info().name());
    assert_eq!("SMALLINT[]", describe.columns()[3].type_info().name());

    Ok(())
}
