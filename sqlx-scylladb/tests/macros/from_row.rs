use sqlx_scylladb::{ScyllaDBPool, macros::FromRow};
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_text(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO text_tests(my_id) VALUES(?)")
        .bind(id)
        .execute(&pool)
        .await?;

    #[derive(FromRow)]
    struct TextTest {
        my_id: Uuid,
        #[sqlx(default_when_null)]
        my_text: String,
        #[sqlx(default_when_null)]
        my_text_list: Vec<String>,
        #[sqlx(default_when_null)]
        my_text_set: Vec<String>,
    }

    let row: TextTest = sqlx::query_as(
        "SELECT my_id, my_text, my_text_list, my_text_set FROM text_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!("", row.my_text);
    assert_eq!(0, row.my_text_list.len());
    assert_eq!(0, row.my_text_set.len());

    let _ = sqlx::query(
        "INSERT INTO text_tests(my_id, my_text, my_text_list, my_text_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind("こんにちは")
    .bind(["こんにちは", "おはよう", "さようなら", "おやすみ"])
    .bind(["こんにちは", "おはよう", "さようなら", "こんにちは"])
    .execute(&pool)
    .await?;

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
