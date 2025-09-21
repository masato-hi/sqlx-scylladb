# sqlx-scylladb

A database driver for ScyllaDB to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework.

Wrap the [scylla-rust-driver](https://github.com/scylladb/scylla-rust-driver) using the sqlx interface.

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

- Standard type binding and fetching.
- Support for user-defined type macros.
- [`#[sqlx::test]`](https://docs.rs/sqlx/latest/sqlx/attr.test.html) macro support.
- Migration support using the `sqlx-scylladb` command-line tool.
- TLS support.
