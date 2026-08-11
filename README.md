# sqlx-scylladb

A ScyllaDB database driver for the Rust [sqlx](https://github.com/transact-rs/sqlx) framework.

This crate adapts the [scylla-rust-driver](https://github.com/scylladb/scylla-rust-driver) to the sqlx interface, allowing sqlx queries, connection pools, migrations, tests, and type conversions to be used with ScyllaDB.

## Why not use the scylla-rust-driver directly?

sqlx provides testing and migration features that are useful when working with a database-backed application.

Using those features through this driver avoids having to maintain separate testing and migration infrastructure.

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

## Connection URL

The driver reads the connection URL from `DATABASE_URL` and also supports `SCYLLADB_URL`.

### Example

```url
scylladb://myname:mypassword@localhost:9042/my_keyspace?nodes=example.test,example2.test:9043&tcp_nodelay&tcp_keepalive=40&compression=lz4&replication_strategy=simple&replication_factor=2&page_size=10
```

### Basic

| Part     | Required | Example     | Explanation                                   |
|----------|----------|-------------|-----------------------------------------------|
| scheme   | Required | scylladb    | Must be `scylladb`.                           |
| username | Optional | myname      | Specify the username for user authentication. |
| password | Optional | mypassword  | Specify the password for user authentication. |
| host     | Required | localhost   | The hostname of the initial node to contact.  |
| port     | Optional | 9042        | Specify the port number. The default is 9042. |
| path     | Required | my_keyspace | Specify the keyspace.                         |

### Options

| Name                 | Example                         | Explanation                                                                                                                                                                  |
|----------------------|---------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| nodes                | example.test,example2.test:9043 | Additional nodes to contact, separated by commas.                                                                                                                            |
| tcp_nodelay          |                                 | Enables TCP_NODELAY. This is a key-only option and does not require a value.                                                                                                 |
| tcp_keepalive        | 40                              | TCP keepalive interval, in seconds.                                                                                                                                          |
| compression          | lz4                             | Compression for protocol traffic. Supported values are `lz4` and `snappy`.                                                                                                   |
| replication_strategy | SimpleStrategy                  | Replication strategy used when the migration support creates a keyspace. Supported values are `simple`, `network_topology`, `SimpleStrategy`, and `NetworkTopologyStrategy`. |
| replication_factor   | 2                               | Replication factor used when the migration support creates a keyspace.                                                                                                       |
| page_size            | 10                              | Maximum number of rows requested in each page of a paged query.                                                                                                              |
| tls_rootcert         | /etc/certs/ca.crt               | Path to the root CA certificate used for TLS server verification.                                                                                                            |
| tls_cert             | /etc/certs/client.crt           | Path to the client certificate used for TLS client authentication.                                                                                                           |
| tls_key              | /etc/certs/client.key           | Path to the private key corresponding to `tls_cert`.                                                                                                                         |

## Features

### Type bindings

<!-- markdownlint-disable MD033 -->

<details>
<summary>Basic type bindings.</summary>

- ASCII (&str, String, Box\<str>, Cow\<'_, str>, Rc\<str>, Arc\<str>)
- TEXT (&str, String, Box\<str>, Cow\<'_, str>, Rc\<str>, Arc\<str>)
- BOOLEAN (bool)
- TINYINT (i8)
- SMALLINT (i16)
- INT (i32)
- BIGINT (i64)
- FLOAT (f32)
- DOUBLE (f64)
- BLOB (Vec\<u8>)
- UUID (uuid::Uuid)
- TIMEUUID (scylla::value::CqlTimeuuid)
- TIMESTAMP (scylla::value::CqlTimestamp, chrono::DateTime\<Utc>, time::OffsetDateTime)
- DATE (scylla::value::CqlDate, chrono::NaiveDate, time::Date)
- TIME (scylla::value::CqlTime, chrono::NaiveTime, time::Time)
- INET (std::net::IpAddr)
- DECIMAL (bigdecimal::Decimal)
- Counter (deserialize only) (scylla::value::Counter)
- Duration
- [ ] Varint

</details>

<details>
<summary>List or Set type bindings.</summary>

- LIST\<ASCII>, SET\<ASCII> (Vec\<String>)
- LIST\<TEXT>, SET\<TEXT> (Vec\<String>)
- LIST\<BOOLEAN>, SET\<BOOLEAN> (Vec\<bool>)
- LIST\<TINYINT>, SET\<TINYINT> (Vec\<i8>)
- LIST\<SMALLINT>, SET\<SMALLINT> (Vec\<i16>)
- LIST\<INT>, SET\<INT> (Vec\<i32>)
- LIST\<BIGINT>, SET\<BIGINT> (Vec\<i64>)
- LIST\<FLOAT>, SET\<FLOAT> (Vec\<f32>)
- LIST\<DOUBLE>, SET\<DOUBLE> (Vec\<f64>)
- LIST\<BLOB>, SET\<BLOB> (Vec\<Vec\<u8>>)
- LIST\<UUID>, SET\<UUID> (Vec\<uuid::Uuid>)
- LIST\<TIMEUUID>, SET\<TIMEUUID> (Vec\<scylla::value::CqlTimeuuid>)
- LIST\<TIMESTAMP>, SET\<TIMESTAMP> (Vec\<scylla::value::CqlTimestamp>, Vec\<chrono::DateTime\<Utc>>, Vec\<time::OffsetDateTime>)
- LIST\<DATE>, SET\<DATE> (Vec\<scylla::value::CqlDate>, Vec\<chrono::NaiveDate>, Vec\<time::Date>)
- LIST\<TIME>, SET\<TIME> (Vec\<scylla::value::CqlTime>, Vec\<chrono::NaiveTime>, Vec\<time::Time>)
- LIST\<INET>, SET\<INET> (Vec\<std::net::IpAddr>)
- LIST\<DECIMAL>, SET\<DECIMAL> (Vec\<bigdecimal::Decimal>)
- LIST\<DURATION> (Vec\<scylla::value::CqlDuration>)
- [ ] Varint

</details>

<details>
<summary>Map type bindings.</summary>

- MAP\<ASCII, ASCII>, MAP\<ASCII, TEXT>, MAP\<TEXT, ASCII>, MAP\<TEXT, TEXT> (HashMap\<String, String>)
- MAP\<ASCII, BOOLEAN>, MAP\<TEXT, BOOLEAN> (HashMap\<String, bool>)
- MAP\<ASCII, TINYINT>, MAP\<TEXT, TINYINT> (HashMap\<String, i8>)
- MAP\<ASCII, SMALLINT>, MAP\<TEXT, SMALLINT> (HashMap\<String, i16>)
- MAP\<ASCII, INT>, MAP\<TEXT, INT> (HashMap\<String, i32>)
- MAP\<ASCII, BIGINT>, MAP\<TEXT, BIGINT> (HashMap\<String, i64>)
- MAP\<ASCII, FLOAT>, MAP\<TEXT, FLOAT> (HashMap\<String, f32>)
- MAP\<ASCII, DOUBLE>, MAP\<TEXT, DOUBLE> (HashMap\<String, f64>)
- MAP\<ASCII, UUID>, MAP\<TEXT, UUID> (HashMap\<String, uuid::Uuid>)
- MAP\<ASCII, INET>, MAP\<TEXT, INET> (HashMap\<String, IpAddr>)

</details>

<!-- markdownlint-enable MD033 -->

### User defined type

- Define a Rust type with the `UserDefinedType` derive macro. See the [example](https://github.com/masato-hi/sqlx-scylladb/blob/main/sqlx-scylladb/examples/user_defined_type.rs).

### `FromRow` derive macro

The `FromRow` derive macro supports the same field and container attributes as
SQLx's standard `FromRow` derive macro, including `rename`, `rename_all`,
`default`, `flatten`, `try_from`, `json`, and `skip`. It also supports the
`#[sqlx(default_when_null)]` field attribute, which uses the field type's
`Default` value when the corresponding database column is `NULL`.

```rust
use sqlx_scylladb::macros::FromRow;

#[derive(FromRow)]
struct User {
    id: i64,
    #[sqlx(default_when_null)]
    display_name: String,
}
```

### Testing

- Use the [`#[sqlx::test]`](https://docs.rs/sqlx/latest/sqlx/attr.test.html) macro for database-backed tests.

### Migration

- Implements the `sqlx::migrate::Migrator` integration.
- Supports migrations used by `#[sqlx::test]`.
- Provides a command-line tool. Install it with `cargo install --git https://github.com/masato-hi/sqlx-scylladb --path sqlx-scylladb-cli`.

### TLS

- TLS is available when the `openssl-010` or `rustls-023` feature is enabled.

### Transaction

Transactions are implemented by collecting data-changing statements and executing them as a ScyllaDB batch when the transaction is committed.

Because of this implementation, read the ScyllaDB documentation on batch operations before relying on transactions. Batch statements have different performance and atomicity characteristics from transactions in traditional relational databases.

[BATCH | ScyllaDB Docs](https://enterprise.docs.scylladb.com/stable/cql/dml/batch.html)

## Performance

In the benchmark included in this repository, performance is approximately 5–10% lower than when using the scylla-rust-driver directly, depending on the data type and operation.

Each benchmark performs 10,000 operations. In the results below, the difference between the two implementations is approximately 17–33 milliseconds. Actual results depend on the workload and environment.

Each benchmark executes 10,000 sequential operations per Criterion iteration. Table setup, connection or pool creation, and data preparation for SELECT benchmarks are excluded from the measured time. Value generation for INSERT benchmarks is included. The comparison uses ScyllaDB's `CachingSession` and SQLx's `ScyllaDBPool` with up to 8 connections.

<!-- markdownlint-disable MD033 -->

<details>
<summary>Benchmark results.</summary>

| Type | Operation | Crate              | Lower bound | Estimate  | Upper bound | Benchmark Name                 |
|------|-----------|--------------------|-------------|-----------|-------------|--------------------------------|
| Text | INSERT    | scylla-rust-driver | 302.86 ms   | 304.94 ms | 307.55 ms   | insert_text_with_scylla        |
| Text | INSERT    | sqlx-scylladb      | 321.82 ms   | 323.95 ms | 326.36 ms   | insert_text_with_sqlx_scylladb |
| Text | SELECT    | scylla-rust-driver | 301.21 ms   | 302.00 ms | 302.83 ms   | select_text_with_scylla        |
| Text | SELECT    | sqlx-scylladb      | 320.15 ms   | 321.17 ms | 322.22 ms   | select_text_with_sqlx_scylladb |
| UUID | INSERT    | scylla-rust-driver | 302.72 ms   | 304.33 ms | 306.27 ms   | insert_uuid_with_scylla        |
| UUID | INSERT    | sqlx-scylladb      | 319.46 ms   | 320.84 ms | 322.52 ms   | insert_uuid_with_sqlx_scylladb |
| UUID | SELECT    | scylla-rust-driver | 301.19 ms   | 301.92 ms | 302.65 ms   | select_uuid_with_scylla        |
| UUID | SELECT    | sqlx-scylladb      | 320.79 ms   | 321.65 ms | 322.53 ms   | select_uuid_with_sqlx_scylladb |
| Blob | INSERT    | scylla-rust-driver | 332.32 ms   | 334.30 ms | 336.77 ms   | insert_blob_with_scylla        |
| Blob | INSERT    | sqlx-scylladb      | 365.69 ms   | 367.39 ms | 369.51 ms   | insert_blob_with_sqlx_scylladb |
| Blob | SELECT    | scylla-rust-driver | 310.20 ms   | 311.22 ms | 312.37 ms   | select_blob_with_scylla        |
| Blob | SELECT    | sqlx-scylladb      | 329.80 ms   | 330.87 ms | 332.13 ms   | select_blob_with_sqlx_scylladb |

</details>

<!-- markdownlint-enable MD033 -->

## License

This project is licensed under either of

Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/masato-hi/sqlx-scylladb/blob/main/LICENSE-APACHE) or [https://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))

MIT license ([LICENSE-MIT](https://github.com/masato-hi/sqlx-scylladb/blob/main/LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.

## Contribution

Unless you explicitly state otherwise, any Contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
