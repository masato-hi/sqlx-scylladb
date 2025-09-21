use std::{fmt::Display, num::ParseIntError, str::FromStr, time::Duration};

use futures_core::future::BoxFuture;
use log::LevelFilter;
use scylla::{client::session::TlsContext, frame::Compression};
use sqlx::{ConnectOptions, Error};
use sqlx_core::connection::LogSettings;
use url::Url;

use crate::{ScyllaDBError, connection::ScyllaDBConnection};

const DEFAULT_PORT: u16 = 9042;
const DEFAULT_PAGE_SIZE: i32 = 5000;
const DEFAULT_STATEMENT_CACHE_CAPACITY: usize = 128;

/// Options and flags which can be used to configure a ScyllaDB connection.
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
    pub(crate) tls_options: Option<ScyllaDBTLSOptions>,
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
                "tls_rootcert" => {
                    options = options.tls_rootcert(value.to_string());
                }
                "tls_cert" => {
                    options = options.tls_cert(value.to_string());
                }
                "tls_key" => {
                    options = options.tls_key(value.to_string());
                }
                _ => eprintln!("Not supported options. {key}"),
            }
        }

        Ok(options)
    }
}

impl ScyllaDBConnectOptions {
    /// Create a default set of connection options.
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
            tls_options: None,
            tcp_keepalive: None,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    /// Set the nodes to connect to.
    pub fn nodes(mut self, nodes: Vec<String>) -> Self {
        self.nodes = nodes;
        self
    }

    /// Add the node to connect to.
    pub fn add_node(mut self, node: impl Into<String>) -> Self {
        self.nodes.push(node.into());
        self
    }

    /// Set the keyspace to use.
    pub fn keyspace(mut self, keyspace: impl Into<String>) -> Self {
        self.keyspace = Some(keyspace.into());
        self
    }

    /// Set the authentication information when performing authentication.
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

    /// Set the replication strategy. This value is only used during keyspace creation and is not normally required to be set.
    pub fn replication_strategy(mut self, strategy: ScyllaDBReplicationStrategy) -> Self {
        let mut replication_options = self.replication_options_or_default();
        replication_options.strategy = strategy;
        self.replication_options = Some(replication_options);
        self
    }

    /// Set the replication factor. This value is only used during keytable creation and is not normally required to be set.
    pub fn replication_factor(mut self, factor: usize) -> Self {
        let mut replication_options = self.replication_options_or_default();
        replication_options.replication_factor = factor;
        self.replication_options = Some(replication_options);
        self
    }

    /// Set the compression method used during communication.
    pub fn compressor(mut self, compressor: ScyllaDBCompressor) -> Self {
        self.compression_options = Some(ScyllaDBCompressionOptions { compressor });
        self
    }

    /// Set the path to the RootCA certificate when using TLS.
    pub fn tls_rootcert(mut self, root_cert: impl Into<String>) -> Self {
        let root_cert = root_cert.into();
        if let Some(mut tls_options) = self.tls_options {
            tls_options.root_cert = root_cert;
            self.tls_options = Some(tls_options);
        } else {
            self.tls_options = Some(ScyllaDBTLSOptions {
                root_cert,
                ..Default::default()
            });
        }
        self
    }

    /// Set the path to the client certificate when using TLS.
    pub fn tls_cert(mut self, cert: impl Into<String>) -> Self {
        let cert = cert.into();
        if let Some(mut tls_options) = self.tls_options {
            tls_options.cert = Some(cert);
            self.tls_options = Some(tls_options);
        } else {
            self.tls_options = Some(ScyllaDBTLSOptions {
                cert: Some(cert),
                ..Default::default()
            });
        }
        self
    }

    /// Set the path to the client private key when using TLS.
    pub fn tls_key(mut self, key: impl Into<String>) -> Self {
        let key = key.into();
        if let Some(mut tls_options) = self.tls_options {
            tls_options.key = Some(key);
            self.tls_options = Some(tls_options);
        } else {
            self.tls_options = Some(ScyllaDBTLSOptions {
                key: Some(key),
                ..Default::default()
            });
        }
        self
    }

    /// Enable tcp_nodelay.
    pub fn tcp_nodelay(mut self) -> Self {
        self.tcp_nodelay = true;
        self
    }

    /// Set the interval for TCP keepalive.
    pub fn tcp_keepalive(mut self, secs: u64) -> Self {
        self.tcp_keepalive = Some(Duration::from_secs(secs));
        self
    }

    /// Sets the size per page for data retrieval pagination.
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

/// Replication strategy classes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ScyllaDBReplicationStrategy {
    /// Simple
    #[default]
    SimpleStrategy,
    /// Network topology
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

/// Compression methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScyllaDBCompressor {
    /// Compress with lz4.
    LZ4Compressor,
    /// Compress with snappy.
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

#[derive(Debug, Clone, Default)]
pub(crate) struct ScyllaDBTLSOptions {
    root_cert: String,
    cert: Option<String>,
    key: Option<String>,
}

impl TryInto<TlsContext> for ScyllaDBTLSOptions {
    type Error = sqlx::Error;

    fn try_into(self) -> Result<TlsContext, Self::Error> {
        #[cfg(feature = "openssl-010")]
        {
            let ssl_context: openssl_010::ssl::SslContext = self.try_into()?;
            return Ok(TlsContext::OpenSsl010(ssl_context));
        }

        #[allow(unreachable_code)]
        #[cfg(feature = "rustls-023")]
        {
            let client_config: rustls_023::ClientConfig = self.try_into()?;
            return Ok(TlsContext::Rustls023(std::sync::Arc::new(client_config)));
        }

        #[allow(unreachable_code)]
        Err(Error::Configuration(
            "To enable TLS, specify the ‘openssl-010’ or ‘rustls-023’ feature.".into(),
        ))
    }
}

#[cfg(feature = "openssl-010")]
impl TryInto<openssl_010::ssl::SslContext> for ScyllaDBTLSOptions {
    type Error = sqlx::Error;

    fn try_into(self) -> Result<openssl_010::ssl::SslContext, Self::Error> {
        use std::{fs, fs::File, io::Read, path::PathBuf};

        use openssl_010::{
            pkey::PKey,
            ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
            x509::{X509, store::X509StoreBuilder},
        };

        let mut context_builder = SslContextBuilder::new(SslMethod::tls())
            .map_err(|e| Error::Configuration(Box::new(e)))?;

        let ca_path = fs::canonicalize(PathBuf::from(&self.root_cert))
            .map_err(|e| Error::Configuration(Box::new(e)))?;
        let mut ca_file = File::open(ca_path).map_err(|e| Error::Configuration(Box::new(e)))?;
        let mut ca_buf = Vec::new();
        ca_file
            .read_to_end(&mut ca_buf)
            .map_err(|e| Error::Configuration(Box::new(e)))?;
        let ca_x509 = X509::from_pem(&ca_buf).map_err(|e| Error::Configuration(Box::new(e)))?;

        let mut builder = X509StoreBuilder::new().map_err(|e| Error::Configuration(Box::new(e)))?;
        builder
            .add_cert(ca_x509)
            .map_err(|e| Error::Configuration(Box::new(e)))?;
        let cert_store = builder.build();

        context_builder.set_cert_store(cert_store);

        if let Some(cert) = &self.cert {
            if let Some(key) = &self.key {
                let cert_path = fs::canonicalize(PathBuf::from(cert))
                    .map_err(|e| Error::Configuration(Box::new(e)))?;
                let mut cert_file =
                    File::open(cert_path).map_err(|e| Error::Configuration(Box::new(e)))?;
                let mut cert_buf = Vec::new();
                cert_file
                    .read_to_end(&mut cert_buf)
                    .map_err(|e| Error::Configuration(Box::new(e)))?;
                let cert_x509 =
                    X509::from_pem(&cert_buf).map_err(|e| Error::Configuration(Box::new(e)))?;
                context_builder
                    .set_certificate(&cert_x509)
                    .map_err(|e| Error::Configuration(Box::new(e)))?;

                let key_path = fs::canonicalize(PathBuf::from(key))
                    .map_err(|e| Error::Configuration(Box::new(e)))?;
                let mut key_file =
                    File::open(key_path).map_err(|e| Error::Configuration(Box::new(e)))?;
                let mut key_buf = Vec::new();
                key_file
                    .read_to_end(&mut key_buf)
                    .map_err(|e| Error::Configuration(Box::new(e)))?;
                let pkey = PKey::private_key_from_pem(&key_buf)
                    .map_err(|e| Error::Configuration(Box::new(e)))?;
                context_builder
                    .set_private_key(&pkey)
                    .map_err(|e| Error::Configuration(Box::new(e)))?;

                context_builder.set_verify(SslVerifyMode::PEER);
            } else {
                return Err(Error::Configuration(
                    "Client private key is required.".into(),
                ));
            }
        } else {
            context_builder.set_verify(SslVerifyMode::NONE);
        }

        let context = context_builder.build();

        Ok(context)
    }
}

#[cfg(feature = "rustls-023")]
impl TryInto<rustls_023::ClientConfig> for ScyllaDBTLSOptions {
    type Error = sqlx::Error;

    fn try_into(self) -> Result<rustls_023::ClientConfig, Self::Error> {
        use rustls_023::{
            ClientConfig, RootCertStore,
            pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        };

        let rustls_ca = CertificateDer::from_pem_file(&self.root_cert)
            .map_err(|e| Error::Configuration(Box::new(e)))?;
        let mut root_store = RootCertStore::empty();
        root_store
            .add(rustls_ca)
            .map_err(|e| Error::Configuration(Box::new(e)))?;

        let builder = ClientConfig::builder().with_root_certificates(root_store);

        let client_config = if let Some(cert) = &self.cert {
            if let Some(key) = &self.key {
                let client_cert = CertificateDer::from_pem_file(cert)
                    .map_err(|e| Error::Configuration(Box::new(e)))?;
                let priv_key = PrivateKeyDer::from_pem_file(key)
                    .map_err(|e| Error::Configuration(Box::new(e)))?;

                builder
                    .with_client_auth_cert(vec![client_cert], priv_key)
                    .map_err(|e| Error::Configuration(Box::new(e)))?
            } else {
                return Err(Error::Configuration(
                    "Client private key is required.".into(),
                ));
            }
        } else {
            builder.with_no_client_auth()
        };

        Ok(client_config)
    }
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
        const URL: &'static str = "scylladb://my_name:my_passwd@localhost/my_keyspace?nodes=example.test,example2.test:9043&tcp_nodelay&tcp_keepalive=40&compression=lz4&replication_strategy=simple&replication_factor=2&page_size=10&tls_rootcert=/etc/tls/root.pem&tls_cert=/etc/tls/client.pem&tls_key=/etc/tls/client.key";
        let options: ScyllaDBConnectOptions = URL.parse()?;

        assert_eq!("my_keyspace", options.keyspace.unwrap());
        assert!(options.tcp_nodelay);
        assert_eq!(40, options.tcp_keepalive.unwrap().as_secs());

        let authentication_options = options.authentication_options.clone().unwrap();
        assert_eq!("my_name", &authentication_options.username);
        assert_eq!("my_passwd", &authentication_options.password);

        assert_eq!(
            vec!["localhost:9042", "example.test", "example2.test:9043"],
            options.nodes
        );

        let compression_options = options.compression_options.unwrap();
        assert_eq!(
            ScyllaDBCompressor::LZ4Compressor,
            compression_options.compressor
        );

        let replication_options = options.replication_options.unwrap();
        assert_eq!(
            ScyllaDBReplicationStrategy::SimpleStrategy,
            replication_options.strategy
        );
        assert_eq!(2, replication_options.replication_factor);

        let page_size = options.page_size;
        assert_eq!(10, page_size);

        let tls_options = options.tls_options.unwrap();

        assert_eq!("/etc/tls/root.pem", tls_options.root_cert);
        assert_eq!("/etc/tls/client.pem", tls_options.cert.unwrap());
        assert_eq!("/etc/tls/client.key", tls_options.key.unwrap());

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
