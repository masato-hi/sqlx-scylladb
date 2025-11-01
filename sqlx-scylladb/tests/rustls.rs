use sqlx_scylladb::{ScyllaDBConnectOptions, ScyllaDBPoolOptions};

#[sqlx::test(migrations = "tests/migrations")]
async fn it_can_connect_by_rustls(
    pool_options: ScyllaDBPoolOptions,
    connect_options: ScyllaDBConnectOptions,
) -> anyhow::Result<()> {
    let connect_options = connect_options
        .port(9142) // set tls port.
        .tls_rootcert("tests/certs/ca-cert.pem")
        .tls_cert("tests/certs/client-cert.pem")
        .tls_key("tests/certs/client-key.pem");

    let pool = pool_options.connect_with(connect_options).await?;

    let id = 1i64;

    let _ = sqlx::query("INSERT INTO my_tests(my_id, my_name) VALUES(?, ?)")
        .bind(id)
        .bind("Alice")
        .execute(&pool)
        .await?;

    let (name,): (String,) = sqlx::query_as("SELECT my_name FROM my_tests WHERE my_id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!("Alice", name);

    Ok(())
}
