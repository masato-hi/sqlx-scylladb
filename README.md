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

## Connection URL

In addition to DATABASE_URL, it also supports SCYLLADB_URL as an environment variable.

### Example

```url
scylladb://myname:mypassword@localhost:9042/my_keyspace?nodes=example.test,example2.test:9043&tcp_nodelay&tcp_keepalive=40&compression=lz4&replication_strategy=simple&replication_factor=2&page_size=10
```

### Basic

| Part     | Required | Example     | Explanation                                   |
|----------|----------|-------------|-----------------------------------------------|
| schema   | Required | scylladb    | Only `scylladb` can be specified.             |
| username | Optional | myname      | Specify the username for user authentication. |
| password | Optional | mypassword  | Specify the password for user authentication. |
| host     | Required | localhost   | Specify the hostname of the primary node.     |
| port     | Optional | 9042        | Specify the port number. The default is 9042. |
| path     | Required | my_keyspace | Specify the keyspace.                         |

### Options

| Name                 | Example                         | Explanation                                                                                                                                                  |
|----------------------|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| nodes                | example.test,example2.test:9043 | Specify additional nodes separated by commas.                                                                                                                |
| tcp_nodelay          |                                 | When using tcp_nodelay, specify the key. No value is required.                                                                                               |
| tcp_keepalive        | 40                              | When using tcp_keepalive, specify the keepalive interval in seconds.                                                                                         |
| compression          | lz4                             | Specify when compressing communication data. Supported values are `lz4` or `snappy`.                                                                         |
| replication_strategy | SimpleStrategy                  | Specifies the replication strategy when creating a keyspace. Supported values are `simple`, `network_topology`, `SimpleStrategy`, `NetworkTopologyStrategy`. |
| replication_factor   | 2                               | Specify the replication factor when creating a keyspace.                                                                                                     |
| page_size            | 10                              | Specify the number of results to retrieve per page when receiving query results.                                                                             |
| tls_rootcert         | /etc/certs/ca.crt               | Specify the path to the root CA certificate when establishing a TLS connection.                                                                              |
| tls_cert             | /etc/certs/client.crt           | Specify the path to the client certificate when establishing a TLS connection                                                                                |
| tls_key              | /etc/certs/client.key           | Specify the path to the client private key when establishing a TLS connection                                                                                |

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

- Definition using the derive macro. (See the [example](https://github.com/masato-hi/sqlx-scylladb/blob/main/sqlx-scylladb/examples/user_defined_type.rs) for usage.)

### Any type not supported by default

- You can add any type supported by scylla-rust-driver. (See the [example](https://github.com/masato-hi/sqlx-scylladb/blob/main/sqlx-scylladb/examples/any.rs) for usage.)

### Testing

- You can use #[sqlx::test] macro.

### Migration

- Implemented sqlx::migrate::Migrator trait.
- Support migrations in #[sqlx::test] macro.
- You can use the command-line tool.
  - To install it, run `cargo install --git https://github.com/masato-hi/sqlx-scylladb/tree/main/sqlx-scylladb-cli`

### TLS

- TLS (Enable with the `openssl-010` or `rustls-023` feature)

### Transaction

Transaction are implemented using batch statement.

Please carefully read the documentation on batch operations in ScyllaDB before using them.

[BATCH | ScyllaDB Docs](https://enterprise.docs.scylladb.com/stable/cql/dml/batch.html)

## Performance

Compared to using the scylla-rust-driver, performance decreases by approximately 10%.

However, this equates to a reduction of about 50 milliseconds for 10,000 operations.

<!-- markdownlint-disable MD033 -->

<details>
<summary>Benchmark results.</summary>

| Name                           | Crate              | Lower bound | Estimate  | Upper bound |
|--------------------------------|--------------------|-------------|-----------|-------------|
| insert_text_with_scylla        | scylla-rust-driver | 460.84 ms   | 461.76 ms | 462.75 ms   |
| insert_text_with_sqlx_scylladb | sqlx-scylladb      | 502.23 ms   | 503.31 ms | 504.54 ms   |
| select_text_with_scylla        | scylla-rust-driver | 456.53 ms   | 457.33 ms | 458.17 ms   |
| select_text_with_sqlx_scylladb | sqlx-scylladb      | 501.69 ms   | 502.67 ms | 503.65 ms   |
| insert_uuid_with_scylla        | scylla-rust-driver | 462.09 ms   | 462.68 ms | 463.29 ms   |
| insert_uuid_with_sqlx_scylladb | sqlx-scylladb      | 506.77 ms   | 507.97 ms | 509.39 ms   |
| select_uuid_with_scylla        | scylla-rust-driver | 457.12 ms   | 458.14 ms | 459.40 ms   |
| select_uuid_with_sqlx_scylladb | sqlx-scylladb      | 502.01 ms   | 502.88 ms | 503.76 ms   |

</details>

<!-- markdownlint-enable MD033 -->

## License

This project is licensed under either of

Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/masato-hi/sqlx-scylladb/blob/main/LICENSE-APACHE) or [https://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))

MIT license ([LICENSE-MIT](https://github.com/masato-hi/sqlx-scylladb/blob/main/LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.

## Contribution

Unless you explicitly state otherwise, any Contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
