use std::{fmt::Display, str::FromStr, time::Duration};

use futures_core::future::BoxFuture;
use log::LevelFilter;
use scylla::frame::Compression;
use sqlx::{ConnectOptions, Error};
use sqlx_core::connection::LogSettings;
use url::Url;

use crate::{ScyllaDBError, connection::ScyllaDBConnection};

const DEFAULT_PORT: u16 = 9042;

#[derive(Debug, Clone)]
pub struct ScyllaDBConnectOptions {
    pub(crate) nodes: Vec<String>,
    pub(crate) keyspace: Option<String>,
    pub(crate) statement_cache_capacity: usize,
    pub(crate) log_settings: LogSettings,
    pub(crate) tcp_nodelay: bool,
    pub(crate) authentication_options: Option<ScyllaDBAuthenticationOptions>,
    pub(crate) replication_options: Option<ScyllaDBReplicationOptions>,
    pub(crate) compression_options: Option<ScyllaDBCompressionOptions>,
    pub(crate) tcp_keepalive: Option<Duration>,
}

impl ScyllaDBConnectOptions {
    pub(crate) fn parse_from_url(url: &Url) -> Result<Self, Error> {
        let mut options = Self::new();

        let host = url.host_str();
        if let Some(host) = host {
            let port = url.port().unwrap_or(DEFAULT_PORT);
            let node = format!("{}:{}", host, port);
            options = options.add_node(node);
        }

        let path = url.path().trim_start_matches('/');
        if !path.is_empty() {
            options = options.keyspace(path);
        }

        let username = url.username();
        if !username.is_empty() {
            let password = url.password().unwrap_or_default();
            options = options.user(username, password);
        }

        let query_pairs = url.query_pairs();
        for (key, value) in query_pairs {
            match key.as_ref() {
                "nodes" => {
                    let nodes = value.split(",");
                    for node in nodes {
                        options = options.add_node(node);
                    }
                }
                "replication_strategy" => {
                    let strategy = ScyllaDBReplicationStrategy::from_str(&value)?;
                    options = options.replication_strategy(strategy);
                }
                "replication_factor" => {
                    let replication_factor = value.parse().unwrap();
                    options = options.replication_factor(replication_factor);
                }
                "compression" => {
                    let compressor = ScyllaDBCompressor::from_str(&value)?;
                    options = options.compressor(compressor);
                }
                "tcp_nodelay" => {
                    options = options.tcp_nodelay();
                }
                "tcp_keepalive" => {
                    let secs = value.parse().unwrap();
                    options = options.tcp_keepalive(secs);
                }
                _ => todo!(),
            }
        }

        Ok(options)
    }
}

impl ScyllaDBConnectOptions {
    pub fn new() -> Self {
        const DEFAULT_STATEMENT_CACHE_CAPACITY: usize = 128;

        Self {
            nodes: vec![],
            keyspace: None,
            statement_cache_capacity: DEFAULT_STATEMENT_CACHE_CAPACITY,
            log_settings: Default::default(),
            tcp_nodelay: false,
            authentication_options: None,
            replication_options: None,
            compression_options: None,
            tcp_keepalive: None,
        }
    }

    fn add_node(mut self, node: impl Into<String>) -> Self {
        self.nodes.push(node.into());
        self
    }

    fn keyspace(mut self, keyspace: &str) -> Self {
        self.keyspace = Some(keyspace.to_owned());
        self
    }

    fn user(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        let authentication_options = ScyllaDBAuthenticationOptions {
            username: username.into(),
            password: password.into(),
        };
        self.authentication_options = Some(authentication_options);
        self
    }

    fn replication_strategy(mut self, strategy: ScyllaDBReplicationStrategy) -> Self {
        let mut replication_options = self.replication_options_or_default();
        replication_options.strategy = strategy;
        self.replication_options = Some(replication_options);
        self
    }

    fn replication_factor(mut self, factor: usize) -> Self {
        let mut replication_options = self.replication_options_or_default();
        replication_options.replication_factor = factor;
        self.replication_options = Some(replication_options);
        self
    }

    fn compressor(mut self, compressor: ScyllaDBCompressor) -> Self {
        self.compression_options = Some(ScyllaDBCompressionOptions { compressor });
        self
    }

    fn tcp_nodelay(mut self) -> Self {
        self.tcp_nodelay = true;
        self
    }

    fn tcp_keepalive(mut self, secs: u64) -> Self {
        self.tcp_keepalive = Some(Duration::from_secs(secs));
        self
    }

    fn replication_options_or_default(&self) -> ScyllaDBReplicationOptions {
        if let Some(replication_options) = self.replication_options {
            replication_options
        } else {
            ScyllaDBReplicationOptions::default()
        }
    }
}

impl ConnectOptions for ScyllaDBConnectOptions {
    type Connection = ScyllaDBConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        Self::parse_from_url(url)
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, Error>>
    where
        Self::Connection: Sized,
    {
        Box::pin(async { ScyllaDBConnection::establish(self).await })
    }

    fn log_statements(mut self, level: LevelFilter) -> Self {
        self.log_settings.log_statements(level);
        self
    }

    fn log_slow_statements(mut self, level: LevelFilter, duration: Duration) -> Self {
        self.log_settings.log_slow_statements(level, duration);
        self
    }
}

impl FromStr for ScyllaDBConnectOptions {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url: Url = s.parse().map_err(Error::config)?;
        Self::from_url(&url)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ScyllaDBAuthenticationOptions {
    pub(crate) username: String,
    pub(crate) password: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ScyllaDBReplicationStrategy {
    #[default]
    SimpleStrategy,
    NetworkTopologyStrategy,
}

impl FromStr for ScyllaDBReplicationStrategy {
    type Err = ScyllaDBError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let class = match s {
            "SimpleStrategy" => Self::SimpleStrategy,
            "NetworkTopologyStrategy" => Self::NetworkTopologyStrategy,
            _ => todo!(),
        };

        Ok(class)
    }
}

impl Display for ScyllaDBReplicationStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScyllaDBReplicationStrategy::SimpleStrategy => write!(f, "SimpleStrategy"),
            ScyllaDBReplicationStrategy::NetworkTopologyStrategy => {
                write!(f, "NetworkTopologyStrategy")
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ScyllaDBReplicationOptions {
    pub(crate) strategy: ScyllaDBReplicationStrategy,
    pub(crate) replication_factor: usize,
}

impl Default for ScyllaDBReplicationOptions {
    fn default() -> Self {
        Self {
            strategy: Default::default(),
            replication_factor: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScyllaDBCompressor {
    LZ4Compressor,
    SnappyCompressor,
}

impl Into<Compression> for ScyllaDBCompressor {
    fn into(self) -> Compression {
        match self {
            ScyllaDBCompressor::LZ4Compressor => Compression::Lz4,
            ScyllaDBCompressor::SnappyCompressor => Compression::Snappy,
        }
    }
}

impl FromStr for ScyllaDBCompressor {
    type Err = ScyllaDBError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let compressor = match s.to_ascii_lowercase().as_str() {
            "lz4" => Self::LZ4Compressor,
            "snappy" => Self::SnappyCompressor,
            _ => todo!(),
        };

        Ok(compressor)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ScyllaDBCompressionOptions {
    pub(crate) compressor: ScyllaDBCompressor,
}

#[cfg(test)]
mod tests {
    use crate::{
        ScyllaDBConnectOptions,
        options::{ScyllaDBCompressor, ScyllaDBReplicationStrategy},
    };

    #[test]
    fn test_parse_url() -> anyhow::Result<()> {
        const URL: &'static str = "scylladb://my_name:my_passwd@localhost/my_keyspace?nodes=example.test,example2.test:9043&tcp_nodelay&tcp_keepalive=40&compression=lz4&replication_strategy=SimpleStrategy&replication_factor=2";
        let options: ScyllaDBConnectOptions = URL.parse()?;

        assert_eq!("my_keyspace", options.keyspace.unwrap());
        assert!(options.tcp_nodelay);
        assert_eq!(40, options.tcp_keepalive.unwrap().as_secs());

        let authentication_options = options.authentication_options.clone().unwrap();
        assert_eq!("my_name", &authentication_options.username);
        assert_eq!("my_passwd", &authentication_options.password);

        let compression_options = options.compression_options;
        assert_eq!(
            vec!["localhost:9042", "example.test", "example2.test:9043"],
            options.nodes
        );
        assert_eq!(
            ScyllaDBCompressor::LZ4Compressor,
            compression_options.unwrap().compressor
        );

        let replication_options = options.replication_options;
        assert_eq!(
            ScyllaDBReplicationStrategy::SimpleStrategy,
            replication_options.unwrap().strategy
        );
        assert_eq!(2, replication_options.unwrap().replication_factor);

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{str::FromStr, sync::Once, thread::sleep, time::Duration};

//     use scylla::client::{session::Session, session_builder::SessionBuilder};
//     use sqlx::{
//         Acquire, ConnectOptions, Execute,
//         migrate::{MigrateDatabase, Migrator},
//         prelude::FromRow,
//     };
//     use tokio::time::Sleep;

//     use crate::{ScyllaDB, ScyllaDBConnectOptions, types::udt::UDT};

//     #[tokio::test]
//     async fn test_connect() {
//         const DATABASE_URL: &'static str =
//             "scylladb://localhost:9042/test?tcp_nodelay&replication_strategy=SimpleStrategy";
//         let exists = ScyllaDB::database_exists(DATABASE_URL).await.unwrap();
//         dbg!(exists);
//         // ScyllaDB::drop_database(DATABASE_URL).await.unwrap();
//         let exists = ScyllaDB::database_exists(DATABASE_URL).await.unwrap();
//         dbg!(exists);
//         ScyllaDB::create_database(DATABASE_URL).await.unwrap();
//         let exists = ScyllaDB::database_exists(DATABASE_URL).await.unwrap();
//         dbg!(exists);

//         let options = ScyllaDBConnectOptions::from_str(DATABASE_URL).unwrap();
//         dbg!(&options);
//         let mut conn = options.connect().await.unwrap();

//         static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

//         // sqlx::query("DROP KEYSPACE test")
//         //     .execute(&mut conn)
//         //     .await
//         //     .unwrap();

//         MIGRATOR.run(&mut conn).await.unwrap();

//         // sqlx::query("CREATE TYPE IF NOT EXISTS my_udt(id BIGINT, name TEXT);")
//         //     .execute(&mut conn)
//         //     .await
//         //     .unwrap();

//         // sqlx::query("CREATE TABLE IF NOT EXISTS tests(id BIGINT PRIMARY KEY, set_items SET<BIGINT>, list_items SET<BIGINT>, udt my_udt, my_tuple TUPLE<BIGINT, TEXT, INT>, udt_set SET<FROZEN<my_udt>>);")
//         //     .execute(&mut conn)
//         //     .await
//         //     .unwrap();

//         sqlx::query(
//             "INSERT INTO tests(id, set_items, list_items, udt, my_tuple, udt_set) VALUES(?, ?, ?, ?, ?, ?);",
//         )
//         .bind(1i64)
//         .bind(vec![1i64, 2i64, 3i64])
//         .bind::<Option<Vec<i64>>>(Some(vec![11i64, 22i64, 33i64]))
//         .bind(crate::types::udt::UDT {
//             id: 1,
//             name: String::from("my_udt"),
//         })
//         .bind((1i64, "test", 1))
//         .bind(vec![UDT {
//             id: 1,
//             name: String::from("my_udt"),
//         }])
//         .execute(&mut conn)
//         .await
//         .unwrap();

//         // let session = SessionBuilder::new()
//         //     .known_node("localhost:9042")
//         //     .build()
//         //     .await
//         //     .unwrap();
//         // session.use_keyspace("test", true).await.unwrap();

//         // let prepared = session
//         //     .prepare("SELECT id, items, tup FROM tests WHERE id = ?")
//         //     .await
//         //     .unwrap();
//         // let query_result = session.execute_unpaged(&prepared, (1i64,)).await.unwrap();
//         // let rows_result = query_result.into_rows_result().unwrap();
//         // let row: (i64, Vec<i64>, (i64, i64, Option<i64>)) = rows_result.first_row().unwrap();
//         // dbg!(row);

//         let row: (i64,) = sqlx::query_as("SELECT id FROM tests")
//             .fetch_one(&mut conn)
//             .await
//             .unwrap();
//         dbg!(row);

//         let row: (i64,) = sqlx::query_as("SELECT id FROM tests WHERE id = ?")
//             .bind(1i64)
//             .fetch_one(&mut conn)
//             .await
//             .unwrap();
//         dbg!(row);

//         let row: Option<(i64,)> = sqlx::query_as("SELECT id FROM tests WHERE id = ?")
//             .bind(2i64)
//             .fetch_optional(&mut conn)
//             .await
//             .unwrap();
//         dbg!(row);

//         let row: Option<(i64,)> = sqlx::query_as("SELECT id FROM tests WHERE id = ?")
//             .bind(1i64)
//             .fetch_optional(&mut conn)
//             .await
//             .unwrap();
//         dbg!(row);

//         let row: Option<(i64,)> = sqlx::query_as("SELECT id FROM tests WHERE id IN ?")
//             .bind(vec![1i64])
//             .fetch_optional(&mut conn)
//             .await
//             .unwrap();
//         dbg!(row);

//         let row: Option<(i64, Vec<i64>)> =
//             sqlx::query_as("SELECT id, set_items FROM tests WHERE id IN ?")
//                 .bind(vec![1i64])
//                 .fetch_optional(&mut conn)
//                 .await
//                 .unwrap();
//         dbg!(row);

//         let row: Option<(i64, Vec<i64>)> =
//             sqlx::query_as("SELECT id, list_items FROM tests WHERE id IN ?")
//                 .bind(vec![1i64])
//                 .fetch_optional(&mut conn)
//                 .await
//                 .unwrap();
//         dbg!(row);

//         #[derive(Debug, FromRow)]
//         struct Data {
//             id: i64,
//             set_items: Vec<i64>,
//             list_items: Vec<i64>,
//             my_tuple: (i64, String, i32),
//             udt_set: Vec<UDT>,
//         }

//         let row = sqlx::query_as::<_, Data>(
//             "SELECT id, set_items, list_items, my_tuple, udt_set FROM tests WHERE id IN ?",
//         )
//         .bind(vec![1i64])
//         .fetch_optional(&mut conn)
//         .await
//         .unwrap();
//         dbg!(row);

//         let mut tx = conn.begin().await.unwrap();
//         // let tx_conn = tx.acquire().await.unwrap();
//         {
//             sqlx::query(
//                 "INSERT INTO tests(id, set_items, list_items, udt, my_tuple, udt_set) VALUES(?, ?, ?, ?, ?, ?);",
//             )
//             .bind(2i64)
//             .bind(vec![1i64, 2i64, 3i64])
//             .bind::<Option<Vec<i64>>>(Some(vec![11i64, 22i64, 33i64]))
//             .bind(crate::types::udt::UDT {
//                 id: 1,
//                 name: String::from("my_udt"),
//             })
//             .bind((1i64, "test", 1))
//             .bind(vec![UDT {
//                 id: 1,
//                 name: String::from("my_udt"),
//             }])
//             .execute(&mut *tx)
//             .await
//             .unwrap();
//         }

//         {
//             sqlx::query(
//                 "INSERT INTO tests(id, set_items, list_items, udt, my_tuple, udt_set) VALUES(?, ?, ?, ?, ?, ?);",
//             )
//             .bind(3i64)
//             .bind(vec![1i64, 2i64, 3i64])
//             .bind::<Option<Vec<i64>>>(Some(vec![11i64, 22i64, 33i64]))
//             .bind(crate::types::udt::UDT {
//                 id: 1,
//                 name: String::from("my_udt"),
//             })
//             .bind((1i64, "test", 1))
//             .bind(vec![UDT {
//                 id: 1,
//                 name: String::from("my_udt"),
//             }])
//             .execute(&mut *tx)
//             .await
//             .unwrap();
//         }

//         // dbg!(&tx_conn.transaction);
//         // dbg!(&tx);

//         tx.commit().await.unwrap();

//         let row = sqlx::query_as::<_, Data>(
//             "SELECT id, set_items, list_items, my_tuple, udt_set FROM tests WHERE id IN ?",
//         )
//         .bind(vec![2i64, 3i64])
//         .fetch_all(&mut conn)
//         .await
//         .unwrap();
//         dbg!(row);

//         // let sql = sqlx::query_as::<_, Data>(
//         //     "SELECT id, set_items, list_items, my_tuple, udt_set FROM tests WHERE id IN ?",
//         // )
//         // .bind(vec![1i64])
//         // .sql();
//         // dbg!(sql);

//         // let row: (i64,) = sqlx::query_as!(Data, "SELECT id FROM tests")
//         //     .fetch_one(&mut conn)
//         //     .await
//         //     .unwrap();
//         // dbg!(row);
//         // let row: (i64, (i64, i64, i64)) = sqlx::query_as("SELECT id, tup FROM tests WHERE id IN ?")
//         //     .bind(vec![1])
//         //     .fetch_one(&mut conn)
//         //     .await
//         //     .unwrap();
//         // dbg!(row);
//     }
// }
