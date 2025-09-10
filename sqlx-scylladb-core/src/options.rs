use std::{fmt::Display, num::ParseIntError, str::FromStr, time::Duration};

use futures_core::future::BoxFuture;
use log::LevelFilter;
use scylla::frame::Compression;
use sqlx::{ConnectOptions, Error};
use sqlx_core::connection::LogSettings;
use url::Url;

use crate::{ScyllaDBError, connection::ScyllaDBConnection};

const DEFAULT_PORT: u16 = 9042;
const DEFAULT_PAGE_SIZE: i32 = 5000;
const DEFAULT_STATEMENT_CACHE_CAPACITY: usize = 128;

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
    pub(crate) page_size: i32,
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
            options = options.user_authentication(username, password);
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
                    let replication_factor = value.parse().map_err(|err: ParseIntError| {
                        let message = format!("Invalid replication_factor. {err}");
                        Error::Configuration(message.into())
                    })?;
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
                    let secs = value.parse().map_err(|err: ParseIntError| {
                        let message = format!("Invalid tcp_keepalive. {err}");
                        Error::Configuration(message.into())
                    })?;
                    options = options.tcp_keepalive(secs);
                }
                "page_size" => {
                    let page_size = value.parse().map_err(|err: ParseIntError| {
                        let message = format!("Invalid page_size. {err}");
                        Error::Configuration(message.into())
                    })?;
                    options = options.page_size(page_size);
                }
                _ => eprintln!("Not supported options. {key}"),
            }
        }

        Ok(options)
    }
}

impl ScyllaDBConnectOptions {
    pub fn new() -> Self {
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
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    pub fn add_node(mut self, node: impl Into<String>) -> Self {
        self.nodes.push(node.into());
        self
    }

    pub fn keyspace(mut self, keyspace: impl Into<String>) -> Self {
        self.keyspace = Some(keyspace.into());
        self
    }

    pub fn user_authentication(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let authentication_options = ScyllaDBAuthenticationOptions {
            username: username.into(),
            password: password.into(),
        };
        self.authentication_options = Some(authentication_options);
        self
    }

    pub fn replication_strategy(mut self, strategy: ScyllaDBReplicationStrategy) -> Self {
        let mut replication_options = self.replication_options_or_default();
        replication_options.strategy = strategy;
        self.replication_options = Some(replication_options);
        self
    }

    pub fn replication_factor(mut self, factor: usize) -> Self {
        let mut replication_options = self.replication_options_or_default();
        replication_options.replication_factor = factor;
        self.replication_options = Some(replication_options);
        self
    }

    pub fn compressor(mut self, compressor: ScyllaDBCompressor) -> Self {
        self.compression_options = Some(ScyllaDBCompressionOptions { compressor });
        self
    }

    pub fn tcp_nodelay(mut self) -> Self {
        self.tcp_nodelay = true;
        self
    }

    pub fn tcp_keepalive(mut self, secs: u64) -> Self {
        self.tcp_keepalive = Some(Duration::from_secs(secs));
        self
    }

    pub fn page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
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
pub enum ScyllaDBReplicationStrategy {
    #[default]
    SimpleStrategy,
    NetworkTopologyStrategy,
}

impl FromStr for ScyllaDBReplicationStrategy {
    type Err = ScyllaDBError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let class = match s {
            "simple" => Self::SimpleStrategy,
            "network_topology" => Self::NetworkTopologyStrategy,
            "SimpleStrategy" => Self::SimpleStrategy,
            "NetworkTopologyStrategy" => Self::NetworkTopologyStrategy,
            _ => {
                return Err(ScyllaDBError::ConfigurationError(format!(
                    "replication_strategy '{s}' is invalid."
                )));
            }
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
pub enum ScyllaDBCompressor {
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
            _ => {
                return Err(ScyllaDBError::ConfigurationError(format!(
                    "compressor '{s}' is invalid."
                )));
            }
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
    use std::{str::FromStr, time::Duration};

    use crate::{
        ScyllaDBConnectOptions,
        options::{ScyllaDBCompressor, ScyllaDBReplicationStrategy},
    };

    #[test]
    fn test_parse_url() -> anyhow::Result<()> {
        const URL: &'static str = "scylladb://my_name:my_passwd@localhost/my_keyspace?nodes=example.test,example2.test:9043&tcp_nodelay&tcp_keepalive=40&compression=lz4&replication_strategy=SimpleStrategy&replication_factor=2&page_size=10";
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

        let page_size = options.page_size;
        assert_eq!(10, page_size);

        Ok(())
    }

    #[test]
    fn test_add_nodes() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_eq!(0, options.nodes.len());

        let options = options.add_node("example1.test:9043");

        assert_eq!(vec!["example1.test:9043"], options.nodes);

        Ok(())
    }

    #[test]
    fn test_keyspace() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(options.keyspace.is_none());

        let options = options.keyspace("test");

        assert_eq!("test", options.keyspace.unwrap());

        Ok(())
    }

    #[test]
    fn test_user_authentication() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(options.authentication_options.is_none());

        let options = options.user_authentication("my_name", "my_password");

        let authentication_options = options.authentication_options.unwrap();
        assert_eq!("my_name", &authentication_options.username);
        assert_eq!("my_password", &authentication_options.password);

        Ok(())
    }

    #[test]
    fn test_replication_strategy() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(options.replication_options.is_none());

        let options =
            options.replication_strategy(ScyllaDBReplicationStrategy::NetworkTopologyStrategy);

        let replication_options = options.replication_options.unwrap();
        assert_eq!(
            ScyllaDBReplicationStrategy::NetworkTopologyStrategy,
            replication_options.strategy
        );
        assert_eq!(1, replication_options.replication_factor);

        Ok(())
    }

    #[test]
    fn test_replication_strategy_from_str() -> anyhow::Result<()> {
        assert_eq!(
            ScyllaDBReplicationStrategy::SimpleStrategy,
            ScyllaDBReplicationStrategy::from_str("simple")?
        );

        assert_eq!(
            ScyllaDBReplicationStrategy::SimpleStrategy,
            ScyllaDBReplicationStrategy::from_str("SimpleStrategy")?
        );

        assert_eq!(
            ScyllaDBReplicationStrategy::NetworkTopologyStrategy,
            ScyllaDBReplicationStrategy::from_str("network_topology")?
        );

        assert_eq!(
            ScyllaDBReplicationStrategy::NetworkTopologyStrategy,
            ScyllaDBReplicationStrategy::from_str("NetworkTopologyStrategy")?
        );

        Ok(())
    }

    #[test]
    fn test_replication_factor() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(options.replication_options.is_none());

        let options = options.replication_factor(2);

        let replication_options = options.replication_options.unwrap();
        assert_eq!(
            ScyllaDBReplicationStrategy::SimpleStrategy,
            replication_options.strategy
        );
        assert_eq!(2, replication_options.replication_factor);

        Ok(())
    }

    #[test]
    fn test_compressor() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(options.compression_options.is_none());

        let options = options.compressor(ScyllaDBCompressor::SnappyCompressor);

        let compression_options = options.compression_options.unwrap();
        assert_eq!(
            ScyllaDBCompressor::SnappyCompressor,
            compression_options.compressor
        );

        Ok(())
    }

    #[test]
    fn test_tcp_nodelay() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(!options.tcp_nodelay);

        let options = options.tcp_nodelay();

        assert!(options.tcp_nodelay);

        Ok(())
    }

    #[test]
    fn test_tcp_keepalive() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(options.tcp_keepalive.is_none());

        let options = options.tcp_keepalive(20);

        assert_eq!(Duration::from_secs(20), options.tcp_keepalive.unwrap());

        Ok(())
    }

    #[test]
    fn test_page_size() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_eq!(5000, options.page_size);

        let options = options.page_size(200);

        assert_eq!(200, options.page_size);

        Ok(())
    }
}
