use std::env;

use sqlx_scylladb::{ScyllaDBConnectOptions, ScyllaDBPoolOptions};
use url::Url;

#[sqlx::test(migrations = "tests/migrations")]
async fn it_can_connect_by_openssl(
    pool_options: ScyllaDBPoolOptions,
    connect_options: ScyllaDBConnectOptions,
) -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let database_url = env::var("SCYLLADB_URL")?;
    let url = Url::parse(&database_url)?;
    let tls_node = format!("{}:9142", url.host().unwrap());
    let connect_options = connect_options
        .nodes(vec![tls_node])
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
