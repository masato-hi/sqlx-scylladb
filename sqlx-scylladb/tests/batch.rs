use sqlx_scylladb::ScyllaDBPool;

#[sqlx::test(migrations = "tests/migrations")]
async fn it_can_execute_transaction_with_batch_statement(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    let _ = sqlx::query("INSERT INTO my_tests(my_id, my_name) VALUES(?, ?)")
        .bind(1i64)
        .bind("Alice")
        .execute(&mut *tx)
        .await?;

    let _ = sqlx::query("INSERT INTO my_tests(my_id, my_name) VALUES(?, ?)")
        .bind(2i64)
        .bind("Bob")
        .execute(&mut *tx)
        .await?;

    let _ = sqlx::query("UPDATE my_tests SET my_name = ? WHERE my_id = ?")
        .bind("Charlie")
        .bind(1i64)
        .execute(&mut *tx)
        .await?;

    let row =
        sqlx::query_as::<_, (i64, String)>("SELECT my_id, my_name FROM my_tests WHERE my_id = ?")
            .bind(1i64)
            .fetch_optional(&mut *tx)
            .await?;

    assert!(row.is_none());

    tx.commit().await?;

    let row =
        sqlx::query_as::<_, (i64, String)>("SELECT my_id, my_name FROM my_tests WHERE my_id = ?")
            .bind(1i64)
            .fetch_optional(&pool)
            .await?;

    assert!(row.is_some());
    let (my_id, my_name) = row.unwrap();
    assert_eq!(1, my_id);
    assert_eq!("Charlie", my_name);

    Ok(())
}
