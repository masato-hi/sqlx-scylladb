use sqlx::{Acquire, Column, Executor, FromRow, TypeInfo};
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_blob(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO blob_tests(my_id, my_blob, my_blob_list, my_blob_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind([0x00u8, 0x61, 0x73, 0x6d])
    .bind([
        [0x00u8, 0x61, 0x73, 0x6d],
        [0x00u8, 0x61, 0x73, 0x6e],
        [0x00u8, 0x61, 0x73, 0x6f],
        [0x00u8, 0x61, 0x73, 0x70],
    ])
    .bind([
        [0x00u8, 0x61, 0x73, 0x6d],
        [0x00u8, 0x61, 0x73, 0x6e],
        [0x00u8, 0x61, 0x73, 0x6f],
        [0x00u8, 0x61, 0x73, 0x6d],
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_blob, my_blob_list, my_blob_set): (Uuid, Vec<u8>, Vec<Vec<u8>>, Vec<Vec<u8>>) =
        sqlx::query_as(
            "SELECT my_id, my_blob, my_blob_list, my_blob_set FROM blob_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(vec![0x00u8, 0x61, 0x73, 0x6d], my_blob);
    assert_eq!(
        vec![
            vec![0x00u8, 0x61, 0x73, 0x6d],
            vec![0x00u8, 0x61, 0x73, 0x6e],
            vec![0x00u8, 0x61, 0x73, 0x6f],
            vec![0x00u8, 0x61, 0x73, 0x70],
        ],
        my_blob_list
    );
    assert_eq!(
        vec![
            vec![0x00u8, 0x61, 0x73, 0x6d],
            vec![0x00u8, 0x61, 0x73, 0x6e],
            vec![0x00u8, 0x61, 0x73, 0x6f],
        ],
        my_blob_set
    );

    #[derive(FromRow)]
    struct AsciiTest {
        my_id: Uuid,
        my_blob: Vec<u8>,
        my_blob_list: Vec<Vec<u8>>,
        my_blob_set: Vec<Vec<u8>>,
    }

    let row: AsciiTest = sqlx::query_as(
        "SELECT my_id, my_blob, my_blob_list, my_blob_set FROM blob_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(vec![0x00u8, 0x61, 0x73, 0x6d], row.my_blob);
    assert_eq!(
        vec![
            vec![0x00u8, 0x61, 0x73, 0x6d],
            vec![0x00u8, 0x61, 0x73, 0x6e],
            vec![0x00u8, 0x61, 0x73, 0x6f],
            vec![0x00u8, 0x61, 0x73, 0x70],
        ],
        row.my_blob_list
    );
    assert_eq!(
        vec![
            vec![0x00u8, 0x61, 0x73, 0x6d],
            vec![0x00u8, 0x61, 0x73, 0x6e],
            vec![0x00u8, 0x61, 0x73, 0x6f],
        ],
        row.my_blob_set
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_blob_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO blob_tests(my_id, my_blob, my_blob_list, my_blob_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<Vec<u8>>)
    .bind(None::<Vec<Vec<u8>>>)
    .bind(None::<Vec<Vec<u8>>>)
    .execute(&pool)
    .await?;

    let (my_id, my_blob, my_blob_list, my_blob_set): (
        Uuid,
        Option<Vec<u8>>,
        Option<Vec<Vec<u8>>>,
        Option<Vec<Vec<u8>>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_blob, my_blob_list, my_blob_set FROM blob_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_blob.is_none());
    assert!(my_blob_list.is_none());
    assert!(my_blob_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO blob_tests(my_id, my_blob, my_blob_list, my_blob_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some([0x00u8, 0x61, 0x73, 0x6d]))
    .bind(Some([
        [0x00u8, 0x61, 0x73, 0x6d],
        [0x00u8, 0x61, 0x73, 0x6e],
        [0x00u8, 0x61, 0x73, 0x6f],
        [0x00u8, 0x61, 0x73, 0x70],
    ]))
    .bind(Some([
        [0x00u8, 0x61, 0x73, 0x6d],
        [0x00u8, 0x61, 0x73, 0x6e],
        [0x00u8, 0x61, 0x73, 0x6f],
    ]))
    .execute(&pool)
    .await?;

    let (my_id, my_blob, my_blob_list, my_blob_set): (
        Uuid,
        Option<Vec<u8>>,
        Option<Vec<Vec<u8>>>,
        Option<Vec<Vec<u8>>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_blob, my_blob_list, my_blob_set FROM blob_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(vec![0x00u8, 0x61, 0x73, 0x6d], my_blob.unwrap());
    assert_eq!(
        vec![
            vec![0x00u8, 0x61, 0x73, 0x6d],
            vec![0x00u8, 0x61, 0x73, 0x6e],
            vec![0x00u8, 0x61, 0x73, 0x6f],
            vec![0x00u8, 0x61, 0x73, 0x70],
        ],
        my_blob_list.unwrap()
    );
    assert_eq!(
        vec![
            vec![0x00u8, 0x61, 0x73, 0x6d],
            vec![0x00u8, 0x61, 0x73, 0x6e],
            vec![0x00u8, 0x61, 0x73, 0x6f],
        ],
        my_blob_set.unwrap()
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_blob(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_blob, my_blob_list, my_blob_set FROM blob_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_blob", describe.columns()[1].name());
    assert_eq!("my_blob_list", describe.columns()[2].name());
    assert_eq!("my_blob_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("BLOB", describe.columns()[1].type_info().name());
    assert_eq!("BLOB[]", describe.columns()[2].type_info().name());
    assert_eq!("BLOB[]", describe.columns()[3].type_info().name());

    Ok(())
}
