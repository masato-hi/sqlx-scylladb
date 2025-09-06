use sqlx_scylladb::{ScyllaDBConnectOptions, ScyllaDBPool, ScyllaDBPoolOptions};

#[sqlx::test(migrations = "tests/migrations")]
async fn it_affected_rows_is_zero(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = 1i64;

    let query_result = sqlx::query("INSERT INTO my_tests(my_id, my_name) VALUES(?, ?)")
        .bind(id)
        .bind("Alice")
        .execute(&pool)
        .await?;

    assert_eq!(0, query_result.rows_num);
    assert_eq!(0, query_result.rows_affected);

    let query_result = sqlx::query("UPDATE my_tests SET my_name = ? WHERE my_id = ?")
        .bind("Bob")
        .bind(id)
        .execute(&pool)
        .await?;

    assert_eq!(0, query_result.rows_num);
    assert_eq!(0, query_result.rows_affected);

    let query_result = sqlx::query("DELETE FROM my_tests WHERE my_id = ?")
        .bind(id)
        .execute(&pool)
        .await?;

    assert_eq!(0, query_result.rows_num);
    assert_eq!(0, query_result.rows_affected);

    Ok(())
}

#[sqlx::test(migrations = "tests/migrations")]
async fn it_lwt_was_affected(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = 1i64;

    let query_result =
        sqlx::query("INSERT INTO my_tests(my_id, my_name) VALUES(?, ?) IF NOT EXISTS")
            .bind(id)
            .bind("Alice")
            .execute(&pool)
            .await?;

    assert_eq!(1, query_result.rows_num);
    assert_eq!(1, query_result.rows_affected);

    let query_result = sqlx::query("UPDATE my_tests SET my_name = ? WHERE my_id = ? IF EXISTS")
        .bind("Bob")
        .bind(id)
        .execute(&pool)
        .await?;

    assert_eq!(1, query_result.rows_num);
    assert_eq!(1, query_result.rows_affected);

    let query_result = sqlx::query("UPDATE my_tests SET my_name = ? WHERE my_id = ? IF EXISTS")
        .bind("Bob")
        .bind(1 + id)
        .execute(&pool)
        .await?;

    assert_eq!(1, query_result.rows_num);
    assert_eq!(0, query_result.rows_affected);

    let query_result = sqlx::query("DELETE FROM my_tests WHERE my_id = ? IF EXISTS")
        .bind(id)
        .execute(&pool)
        .await?;

    assert_eq!(1, query_result.rows_num);
    assert_eq!(1, query_result.rows_affected);

    Ok(())
}

#[sqlx::test(migrations = "tests/migrations")]
async fn it_can_get_total_rows(
    pool_options: ScyllaDBPoolOptions,
    connect_options: ScyllaDBConnectOptions,
) -> anyhow::Result<()> {
    let connect_options = connect_options.page_size(2);
    let pool = pool_options.connect_with(connect_options).await?;

    for i in 1..11i64 {
        let _ = sqlx::query("INSERT INTO my_tests(my_id, my_name) VALUES(?, ?)")
            .bind(i)
            .bind(format!("Alice{i}"))
            .execute(&pool)
            .await?;
    }

    let query_result = sqlx::query("SELECT my_id FROM my_tests WHERE my_id IN ?")
        .bind([1i64, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        .execute(&pool)
        .await?;

    assert_eq!(10, query_result.rows_num);
    assert_eq!(0, query_result.rows_affected);

    Ok(())
}
