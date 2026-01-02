use scylla::value::{MaybeUnset, Unset};
use sqlx::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_maybe_unset_text(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO unset_tests(my_id, my_text, my_bigint, my_tinyint) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(MaybeUnset::<String>::Unset)
    .bind(MaybeUnset::Set(7i64))
    .bind(Unset)
    .execute(&pool)
    .await?;

    let (my_id, my_text, my_bigint, my_tinyint): (
        Uuid,
        MaybeUnset<String>,
        Option<i64>,
        MaybeUnset<i8>,
    ) = sqlx::query_as(
        "SELECT my_id, my_text, my_bigint, my_tinyint FROM unset_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);

    // assert my_text
    if let MaybeUnset::Set(_) = my_text {
        panic!("expect unset");
    };

    // assert my_bigint
    let Some(my_bigint) = my_bigint else {
        panic!("expect set");
    };
    assert_eq!(7, my_bigint);

    // assert my_tinyint
    if let MaybeUnset::Set(_) = my_tinyint {
        panic!("expect unset");
    }

    #[derive(FromRow)]
    struct TextTest {
        my_id: Uuid,
        my_text: MaybeUnset<String>,
        my_bigint: MaybeUnset<i64>,
        my_tinyint: Option<i8>,
    }

    let row: TextTest = sqlx::query_as(
        "SELECT my_id, my_text, my_bigint, my_tinyint FROM unset_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);

    // assert my_text
    if let MaybeUnset::Set(_) = row.my_text {
        panic!("expect unset");
    };

    // assert my_bigint
    let MaybeUnset::Set(my_bigint) = row.my_bigint else {
        panic!("expect set");
    };
    assert_eq!(7, my_bigint);

    // assert my_tinyint
    if row.my_tinyint.is_some() {
        panic!("expect unset");
    }

    Ok(())
}
