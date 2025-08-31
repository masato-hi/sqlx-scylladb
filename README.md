# sqlx-scylladb

A database driver for ScyllaDB to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework.

Wrap the [scylla-rust-driver](https://github.com/scylladb/scylla-rust-driver) using the sqlx interface.

## Why not use the scylla-rust-driver directly?

sqlx has excellent testing and migration features.

It is better to use these features rather than creating your own testing or migration functionality.
