use sqlx::{Acquire, Column, Executor, FromRow, TypeInfo};
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_text(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO text_tests(my_id, my_text, my_text_list, my_text_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind("こんにちは")
    .bind(["こんにちは", "おはよう", "さようなら", "おやすみ"])
    .bind(["こんにちは", "おはよう", "さようなら", "こんにちは"])
    .execute(&pool)
    .await?;

    let (my_id, my_text, my_text_list, my_text_set): (Uuid, String, Vec<String>, Vec<String>) =
        sqlx::query_as(
            "SELECT my_id, my_text, my_text_list, my_text_set FROM text_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!("こんにちは", my_text);
    assert_eq!(
        vec!["こんにちは", "おはよう", "さようなら", "おやすみ"],
        my_text_list
    );
    assert_eq!(vec!["おはよう", "こんにちは", "さようなら",], my_text_set);

    #[derive(FromRow)]
    struct TextTest {
        my_id: Uuid,
        my_text: String,
        my_text_list: Vec<String>,
        my_text_set: Vec<String>,
    }

    let row: TextTest = sqlx::query_as(
        "SELECT my_id, my_text, my_text_list, my_text_set FROM text_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!("こんにちは", row.my_text);
    assert_eq!(
        vec!["こんにちは", "おはよう", "さようなら", "おやすみ"],
        row.my_text_list
    );
    assert_eq!(
        vec!["おはよう", "こんにちは", "さようなら",],
        row.my_text_set
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_text_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO text_tests(my_id, my_text, my_text_list, my_text_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<String>)
    .bind(None::<Vec<String>>)
    .bind(None::<Vec<String>>)
    .execute(&pool)
    .await?;

    let (my_id, my_text, my_text_list, my_text_set): (
        Uuid,
        Option<String>,
        Option<Vec<String>>,
        Option<Vec<String>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_text, my_text_list, my_text_set FROM text_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_text.is_none());
    assert!(my_text_list.is_none());
    assert!(my_text_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO text_tests(my_id, my_text, my_text_list, my_text_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some("こんにちは"))
    .bind(Some(["こんにちは", "おはよう", "さようなら", "おやすみ"]))
    .bind(Some(["こんにちは", "おはよう", "さようなら", "こんにちは"]))
    .execute(&pool)
    .await?;

    let (my_id, my_text, my_text_list, my_text_set): (
        Uuid,
        Option<String>,
        Option<Vec<String>>,
        Option<Vec<String>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_text, my_text_list, my_text_set FROM text_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!("こんにちは", my_text.unwrap());
    assert_eq!(
        vec!["こんにちは", "おはよう", "さようなら", "おやすみ"],
        my_text_list.unwrap()
    );
    assert_eq!(
        vec!["おはよう", "こんにちは", "さようなら",],
        my_text_set.unwrap()
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_text(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_text, my_text_list, my_text_set FROM text_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_text", describe.columns()[1].name());
    assert_eq!("my_text_list", describe.columns()[2].name());
    assert_eq!("my_text_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("TEXT", describe.columns()[1].type_info().name());
    assert_eq!("TEXT[]", describe.columns()[2].type_info().name());
    assert_eq!("TEXT[]", describe.columns()[3].type_info().name());

    Ok(())
}
