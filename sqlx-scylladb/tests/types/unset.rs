use scylla::value::{MaybeUnset, Unset};
use sqlx::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_maybe_unset_text(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO text_tests(my_id, my_text, my_text_list, my_text_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(MaybeUnset::Set("こんにちは"))
    .bind(MaybeUnset::Set([
        "こんにちは",
        "おはよう",
        "さようなら",
        "おやすみ",
    ]))
    .bind(Unset)
    .execute(&pool)
    .await?;

    let (my_id, my_text, my_text_list, my_text_set): (
        Uuid,
        MaybeUnset<String>,
        Option<Vec<String>>,
        MaybeUnset<Vec<String>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_text, my_text_list, my_text_set FROM text_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);

    // assert my_text
    let MaybeUnset::Set(my_text) = my_text else {
        panic!("expect set");
    };
    assert_eq!("こんにちは", my_text);

    // assert my_text_list
    let Some(my_text_list) = my_text_list else {
        panic!("expect set");
    };
    assert_eq!(
        vec!["こんにちは", "おはよう", "さようなら", "おやすみ"],
        my_text_list
    );

    // assert my_text_set
    if let MaybeUnset::Set(_) = my_text_set {
        panic!("expect unset");
    }

    #[derive(FromRow)]
    struct TextTest {
        my_id: Uuid,
        my_text: MaybeUnset<String>,
        my_text_list: MaybeUnset<Vec<String>>,
        my_text_set: Option<Vec<String>>,
    }

    let row: TextTest = sqlx::query_as(
        "SELECT my_id, my_text, my_text_list, my_text_set FROM text_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);

    // assert my_text
    let MaybeUnset::Set(my_text) = &row.my_text else {
        panic!("expect set");
    };
    assert_eq!("こんにちは", my_text);

    // assert my_text_list
    let MaybeUnset::Set(my_text_list) = &row.my_text_list else {
        panic!("expect set");
    };
    assert_eq!(
        vec!["こんにちは", "おはよう", "さようなら", "おやすみ"],
        *my_text_list
    );

    // assert my_text_set
    if row.my_text_set.is_some() {
        panic!("expect unset");
    }

    Ok(())
}
