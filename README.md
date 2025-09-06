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

## URL

In addition to DATABASE_URL, SCYLLADB_URL is supported as a source for retrieving environment variables during testing.

### Full example

```url
scylladb://myname:mypassword@localhost:9042/my_keyspace?nodes=example.test,example2.test:9043&tcp_nodelay&tcp_keepalive=40&compression=lz4&replication_strategy=SimpleStrategy&replication_factor=2&page_size=10
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

### Query parameters

| Name                 | Example                         | Explanation                                                                                                                      |
|----------------------|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| nodes                | example.test,example2.test:9043 | Specify additional nodes separated by commas.                                                                                    |
| tcp_nodelay          |                                 | When using tcp_nodelay, specify the key. No value is required.                                                                   |
| tcp_keepalive        | 40                              | When using tcp_keepalive, specify the keepalive interval in seconds.                                                             |
| compression          | lz4                             | Specify when compressing communication data. Supported values are `lz4` or `snappy`.                                             |
| replication_strategy | SimpleStrategy                  | Specifies the replication strategy when creating a keyspace. Supported values are `SimpleStrategy` or `NetworkTopologyStrategy`. |
| replication_factor   | 2                               | Specify the replication factor when creating a keyspace.                                                                         |
| page_size            | 10                              | Specify the number of results to retrieve per page when receiving query results.                                                 |

## Features

### Basic type binding

- [x] ASCII (&str, String, Box\<str>, Cow\<'_, str>, Rc\<str>, Arc\<str>)
- [x] TEXT (&str, String, Box\<str>, Cow\<'_, str>, Rc\<str>, Arc\<str>)
- [x] BOOLEAN (bool)
- [x] TINYINT (i8)
- [x] SMALLINT (i16)
- [x] INT (i32)
- [x] BIGINT (i64)
- [x] FLOAT (f32)
- [x] DOUBLE (f64)
- [x] BLOB (Vec\<u8>, Vec\<u8>)
- [x] UUID (uuid::Uuid)
- [x] TIMEUUID (scylla::value::CqlTimeuuid)
- [x] TIMESTAMP (scylla::value::CqlTimestamp, chrono::DateTime\<Utc>, time::OffsetDateTime)
- [x] DATE (scylla::value::CqlDate, chrono::NaiveDate, time::Date)
- [x] TIME (scylla::value::CqlTime, chrono::NaiveTime, time::Time)
- [x] INET (std::net::IpAddr)
- [x] DECIMAL (bigdecimal::Decimal)
  - [ ] scylla::value::CqlDecimal
- [x] Counter (deserialize only) (scylla::value::Counter)
- [ ] Duration
- [ ] Varint

### List or Set type binding

- [x] LIST\<ASCII>, SET\<ASCII> ([&str], Vec\<String>)
- [x] LIST\<TEXT>, SET\<TEXT> ([&str], Vec\<String>)
- [x] LIST\<BOOLEAN>, SET\<BOOLEAN> (Vec\<bool>)
- [x] LIST\<TINYINT>, SET\<TINYINT> (Vec\<i8>)
- [x] LIST\<SMALLINT>, SET\<SMALLINT> (Vec\<i16>)
- [x] LIST\<INT>, SET\<INT> (Vec\<i32>)
- [x] LIST\<BIGINT>, SET\<BIGINT> (Vec\<i64>)
- [x] LIST\<FLOAT>, SET\<FLOAT> (Vec\<f32>)
- [x] LIST\<DOUBLE>, SET\<DOUBLE> (Vec\<f64>)
- [x] LIST\<UUID>, SET\<UUID> (Vec\<uuid::Uuid>)
- [x] LIST\<TIMEUUID>, SET\<TIMEUUID> (Vec\<scylla::value::CqlTimeuuid>)
- [x] LIST\<TIMESTAMP>, SET\<TIMESTAMP> (Vec\<scylla::value::CqlTimestamp>, Vec\<chrono::DateTime\<Utc>>, Vec\<time::OffsetDateTime>)
- [x] LIST\<DATE>, SET\<DATE> (Vec\<scylla::value::CqlDate>, Vec\<chrono::NaiveDate>, Vec\<time::Date>)
- [x] LIST\<TIME>, SET\<TIME> (Vec\<scylla::value::CqlTime>, Vec\<chrono::NaiveTime>, Vec\<time::Time>)
- [x] LIST\<INET>, SET\<INET> (Vec\<std::net::IpAddr>)
- [x] LIST\<DECIMAL>, SET\<DECIMAL> (Vec\<bigdecimal::Decimal>)
  - [ ] scylla::value::CqlDecimal
- [ ] Duration
- [ ] Varint

### User defined type

Currently, only manual implementation is supported.

- [x] Manual implementation.
- [x] Derive macro. Use the new type idiom to implement user defined types for array types. (See the [example](https://github.com/masato-hi/sqlx-scylladb/blob/main/examples/user_defined_type.rs) for usage.)

### Testing

- [x] #[sqlx::test] macro.

### Migration

- [x] Support migrations in #[sqlx::test] macro.
- [x] sqlx::migrate::Migrator
- [ ] CLI

### TLS

Currently not supported.

- [ ] TLS

### Transaction

Transaction are implemented using batch statement.

Please carefully read the documentation on batch operations in ScyllaDB before using them.

[BATCH | ScyllaDB Docs](https://enterprise.docs.scylladb.com/stable/cql/dml/batch.html)
