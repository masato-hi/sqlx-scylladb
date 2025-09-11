use sqlx_scylladb::{ScyllaDBConnectOptions, ScyllaDBPoolOptions};
use url::Url;

#[sqlx::test(migrations = "tests/migrations")]
async fn it_can_connect_by_openssl(
    pool_options: ScyllaDBPoolOptions,
    connect_options: ScyllaDBConnectOptions,
) -> anyhow::Result<()> {
    let node = connect_options.get_nodes().first().unwrap();
    let url = Url::parse(&format!("scylladb://{}", node))?;
    let tls_node = format!("{}:9142", url.host().unwrap());
    let connect_options = connect_options
        .nodes(vec![tls_node])
        .tls_rootcert("certs/ca-cert.pem")
        .tls_cert("certs/client-cert.pem")
        .tls_key("certs/client-key.pem");

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
