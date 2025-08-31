# sqlx-scylladb

A database driver for ScyllaDB to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework.

Wrap the [scylla-rust-driver](https://github.com/scylladb/scylla-rust-driver) using the sqlx interface.

## Why not use the scylla-rust-driver directly?

sqlx has excellent testing and migration features.

It is better to use these features rather than creating your own testing or migration functionality.

## Usage

### Quickstart

```rust
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

## Features

### Basic type binding

- [x] ASCII (&str, String)
  - [ ] Box\<str>
  - [ ] Arc\<str>
- [x] TEXT (&str, String)
  - [ ] Box\<str>
  - [ ] Arc\<str>
- [x] BOOLEAN (bool)
- [x] TINYINT (i8)
- [x] SMALLINT (i16)
- [x] INT (i32)
- [x] BIGINT (i64)
- [x] FLOAT (f32)
- [x] DOUBLE (f64)
- [x] BLOB (&[u8], Vec\<u8>)
- [x] UUID (uuid::Uuid)
- [x] TIMEUUID (scylla::value::CqlTimeuuid)
- [x] TIMESTAMP (scylla::value::CqlTimestamp, chrono::DateTime\<Utc>, time::OffsetDateTime)
- [x] DATE (scylla::value::CqlDate, chrono::NaiveDate, time::Date)
- [x] TIME (scylla::value::CqlTime, chrono::NaiveTime, time::Time)
- [x] INET (std::net::IpAddr)
- [x] DECIMAL (bigdecimal::Decimal)
  - [ ] scylla::value::CqlDecimal
- [ ] Counter
- [ ] Duration
- [ ] Varint

### List or Set type binding

To avoid additional memory allocation, only borrowing is supported.

- [x] LIST\<ASCII>, SET\<ASCII> (&[&str], &[String])
- [x] LIST\<TEXT>, SET\<TEXT> (&[&str], &[String])
- [x] LIST\<BOOLEAN>, SET\<BOOLEAN> (&[bool])
- [x] LIST\<TINYINT>, SET\<TINYINT> (&[i8])
- [x] LIST\<SMALLINT>, SET\<SMALLINT> (&[i16])
- [x] LIST\<INT>, SET\<INT> (&[i32])
- [x] LIST\<BIGINT>, SET\<BIGINT> (&[i64])
- [x] LIST\<FLOAT>, SET\<FLOAT> (&[f32])
- [x] LIST\<DOUBLE>, SET\<DOUBLE> (&[f64])
- [x] LIST\<UUID>, SET\<UUID> (&[uuid::Uuid])
- [x] LIST\<TIMEUUID>, SET\<TIMEUUID> (&[scylla::value::CqlTimeuuid])
- [x] LIST\<TIMESTAMP>, SET\<TIMESTAMP> (&[scylla::value::CqlTimestamp], &[chrono::DateTime\<Utc>], &[time::OffsetDateTime])
- [x] LIST\<DATE>, SET\<DATE> (&[scylla::value::CqlDate], &[chrono::NaiveDate], &[time::Date])
- [x] LIST\<TIME>, SET\<TIME> (&[scylla::value::CqlTime], &[chrono::NaiveTime], &[time::Time])
- [ ] LIST\<INET>, SET\<INET> (&[std::net::IpAddr])
- [x] LIST\<DECIMAL>, SET\<DECIMAL> (&[bigdecimal::Decimal])
  - [ ] scylla::value::CqlDecimal
- [ ] Duration
- [ ] Varint

### User defined type

Currently, only manual implementation is supported.

- [ ] Manual implementation.
- [ ] Derive macro.

### Testing

- [x] #[sqlx::test] macro.

### Migration

- [x] Support migrations in #[sqlx::test] macro.
- [x] sqlx::migrate::Migrator
- [ ] CLI
