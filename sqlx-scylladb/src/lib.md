# sqlx-scylladb

A ScyllaDB database driver for the Rust [sqlx](https://github.com/launchbadge/sqlx) framework.

This crate adapts the [scylla-rust-driver](https://github.com/scylladb/scylla-rust-driver) to the sqlx interface.

## Basic Usage

```rust,ignore
use sqlx_scylladb::ScyllaDBPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = ScyllaDBPoolOptions::new()
        .max_connections(5)
        .connect("scylladb://localhost/test")
        .await?;

    sqlx::query("INSERT INTO users(id, name) VALUES(?, ?)")
      .bind(1)
      .bind("Alice")
      .execute(&pool)
      .await?;

    let (name,): (String,) = sqlx::query_as("SELECT name FROM users WHERE id = ?")
      .bind(1)
      .fetch_one(&pool)
      .await?;

    assert_eq!("Alice", name);

    Ok(())
}
```

### Features

- Binding and fetching for ScyllaDB native types and supported Rust types.
- User-defined types through the `UserDefinedType` derive macro.
- Support for the [`#[sqlx::test]`](https://docs.rs/sqlx/latest/sqlx/attr.test.html) macro.
- Migrations through the `sqlx-scylladb` command-line tool.
- TLS support through the `openssl-010` and `rustls-023` features.
