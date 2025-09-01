use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_ascii(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO ascii_tests(my_id, my_ascii, my_ascii_list, my_ascii_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind("Hello!")
    .bind(["Hello!", "Good morning!", "Bye.", "Good night."])
    .bind(["Hello!", "Good morning!", "Bye.", "Hello!"])
    .execute(&pool)
    .await?;

    let (my_id, my_ascii, my_ascii_list, my_ascii_set): (Uuid, String, Vec<String>, Vec<String>) =
        sqlx::query_as(
            "SELECT my_id, my_ascii, my_ascii_list, my_ascii_set FROM ascii_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!("Hello!", my_ascii);
    assert_eq!(
        vec!["Hello!", "Good morning!", "Bye.", "Good night."],
        my_ascii_list
    );
    assert_eq!(vec!["Bye.", "Good morning!", "Hello!",], my_ascii_set);

    #[derive(FromRow)]
    struct AsciiTest {
        my_id: Uuid,
        my_ascii: String,
        my_ascii_list: Vec<String>,
        my_ascii_set: Vec<String>,
    }

    let row: AsciiTest = sqlx::query_as(
        "SELECT my_id, my_ascii, my_ascii_list, my_ascii_set FROM ascii_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!("Hello!", row.my_ascii);
    assert_eq!(
        vec!["Hello!", "Good morning!", "Bye.", "Good night."],
        row.my_ascii_list
    );
    assert_eq!(vec!["Bye.", "Good morning!", "Hello!",], row.my_ascii_set);

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_ascii(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_ascii, my_ascii_list, my_ascii_set FROM ascii_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_ascii", describe.columns()[1].name());
    assert_eq!("my_ascii_list", describe.columns()[2].name());
    assert_eq!("my_ascii_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("ASCII", describe.columns()[1].type_info().name());
    assert_eq!("ASCII[]", describe.columns()[2].type_info().name());
    assert_eq!("ASCII[]", describe.columns()[3].type_info().name());

    Ok(())
}
