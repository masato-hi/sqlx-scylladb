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
    pub(crate) host: String,
    pub(crate) port: u16,
    nodes: Vec<String>,
    pub(crate) keyspace: Option<String>,
    pub(crate) statement_cache_capacity: usize,
    pub(crate) log_settings: LogSettings,
    pub(crate) tcp_nodelay: bool,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) replication_strategy: Option<ScyllaDBReplicationStrategy>,
    pub(crate) replication_factor: usize,
    pub(crate) compression: Option<ScyllaDBCompression>,
    pub(crate) tls_rootcert: Option<String>,
    pub(crate) tls_cert: Option<String>,
    pub(crate) tls_key: Option<String>,
    pub(crate) tcp_keepalive: Option<Duration>,
    pub(crate) page_size: i32,
}

impl ScyllaDBConnectOptions {
    pub(crate) fn parse_from_url(url: &Url) -> Result<Self, Error> {
        let mut options = Self::new();

        if let Some(host) = url.host_str() {
            options = options.host(host);
        }

        if let Some(port) = url.port() {
            options = options.port(port)
        }

        let path = url.path().trim_start_matches('/');
        if !path.is_empty() {
            options = options.keyspace(path);
        }

        let username = url.username();
        if !username.is_empty() {
            options = options.username(username);
            if let Some(password) = url.password() {
                options = options.password(password);
            }
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
                    let compressor = ScyllaDBCompression::from_str(&value)?;
                    options = options.compresson(compressor);
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
                    options = options.tls_rootcert(&value);
                }
                "tls_cert" => {
                    options = options.tls_cert(&value);
                }
                "tls_key" => {
                    options = options.tls_key(&value);
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
            host: String::from("localhost"),
            port: DEFAULT_PORT,
            nodes: vec![],
            keyspace: None,
            username: None,
            password: None,
            replication_strategy: None,
            replication_factor: 1,
            compression: None,
            tls_rootcert: None,
            tls_cert: None,
            tls_key: None,
            tcp_nodelay: false,
            tcp_keepalive: None,
            page_size: DEFAULT_PAGE_SIZE,
            statement_cache_capacity: DEFAULT_STATEMENT_CACHE_CAPACITY,
            log_settings: Default::default(),
        }
    }

    pub(crate) fn get_connect_nodes(&self) -> Vec<String> {
        let mut nodes = Vec::with_capacity(self.nodes.len() + 1);
        // Push primary node.
        nodes.push(format!("{}:{}", self.host, self.port));
        nodes.extend_from_slice(&self.nodes);
        nodes
    }

    /// Set the host of primary node to connect to.
    pub fn host(mut self, host: &str) -> Self {
        host.clone_into(&mut self.host);
        self
    }

    /// Set the port of primary node to connect to.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
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

    /// Set the username for authentication.
    pub fn username(mut self, username: &str) -> Self {
        self.username = Some(username.to_string());
        self
    }

    /// Set the password for authentication.
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    /// Set the replication strategy. This value is only used during keyspace creation and is not normally required to be set.
    pub fn replication_strategy(mut self, strategy: ScyllaDBReplicationStrategy) -> Self {
        self.replication_strategy = Some(strategy);
        self
    }

    /// Set the replication factor. This value is only used during keytable creation and is not normally required to be set.
    pub fn replication_factor(mut self, factor: usize) -> Self {
        self.replication_factor = factor;
        self
    }

    /// Set the compression method used during communication.
    pub fn compresson(mut self, compression: ScyllaDBCompression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Set the path to the RootCA certificate when using TLS.
    pub fn tls_rootcert(mut self, root_cert: &str) -> Self {
        self.tls_rootcert = Some(root_cert.to_string());
        self
    }

    /// Set the path to the client certificate when using TLS.
    pub fn tls_cert(mut self, cert: &str) -> Self {
        self.tls_cert = Some(cert.to_string());
        self
    }

    /// Set the path to the client private key when using TLS.
    pub fn tls_key(mut self, key: &str) -> Self {
        self.tls_key = Some(key.to_string());
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
}

impl ConnectOptions for ScyllaDBConnectOptions {
    type Connection = ScyllaDBConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        Self::parse_from_url(url)
    }

    fn to_url_lossy(&self) -> Url {
        let mut url = Url::from_str(&format!("scylladb://{}:{}/", self.host, self.port))
            .expect("BUG: generated un-parseable URL");

        if let Some(keyspace) = &self.keyspace {
            let _ = url.set_path(keyspace);
        }

        if let Some(username) = &self.username {
            let _ = url.set_username(&username);
        }

        let _ = url.set_password(self.password.as_deref());

        if self.nodes.len() > 0 {
            let nodes = self.nodes.join(",");
            url.query_pairs_mut().append_pair("nodes", &nodes);
        }

        if let Some(replication_strategy) = self.replication_strategy {
            url.query_pairs_mut()
                .append_pair("replication_strategy", &replication_strategy.to_string())
                .append_pair("replication_factor", &self.replication_factor.to_string());
        }

        if let Some(compression) = self.compression {
            url.query_pairs_mut()
                .append_pair("compression", &compression.to_string());
        }

        if self.tcp_nodelay {
            url.query_pairs_mut().append_key_only("tcp_nodelay");
        }

        if let Some(tcp_keepalive) = self.tcp_keepalive {
            url.query_pairs_mut()
                .append_pair("tcp_keepalive", &tcp_keepalive.as_secs().to_string());
        }

        url.query_pairs_mut()
            .append_pair("page_size", &self.page_size.to_string());

        if let Some(tls_rootcert) = &self.tls_rootcert {
            url.query_pairs_mut()
                .append_pair("tls_rootcert", &tls_rootcert);
        }

        if let Some(tls_cert) = &self.tls_cert {
            url.query_pairs_mut().append_pair("tls_cert", &tls_cert);
        }

        if let Some(tls_key) = &self.tls_key {
            url.query_pairs_mut().append_pair("tls_key", &tls_key);
        }

        url
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

/// Compression methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScyllaDBCompression {
    /// Compress with lz4.
    LZ4Compressor,
    /// Compress with snappy.
    SnappyCompressor,
}

impl Into<Compression> for ScyllaDBCompression {
    fn into(self) -> Compression {
        match self {
            ScyllaDBCompression::LZ4Compressor => Compression::Lz4,
            ScyllaDBCompression::SnappyCompressor => Compression::Snappy,
        }
    }
}

impl FromStr for ScyllaDBCompression {
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

impl Display for ScyllaDBCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScyllaDBCompression::LZ4Compressor => write!(f, "lz4"),
            ScyllaDBCompression::SnappyCompressor => write!(f, "snappy"),
        }
    }
}

impl TryInto<TlsContext> for &ScyllaDBConnectOptions {
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
impl TryInto<openssl_010::ssl::SslContext> for &ScyllaDBConnectOptions {
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

        if let Some(root_cert) = &self.tls_rootcert {
            let ca_path = fs::canonicalize(PathBuf::from(root_cert))
                .map_err(|e| Error::Configuration(Box::new(e)))?;
            let mut ca_file = File::open(ca_path).map_err(|e| Error::Configuration(Box::new(e)))?;
            let mut ca_buf = Vec::new();
            ca_file
                .read_to_end(&mut ca_buf)
                .map_err(|e| Error::Configuration(Box::new(e)))?;
            let ca_x509 = X509::from_pem(&ca_buf).map_err(|e| Error::Configuration(Box::new(e)))?;

            let mut builder =
                X509StoreBuilder::new().map_err(|e| Error::Configuration(Box::new(e)))?;
            builder
                .add_cert(ca_x509)
                .map_err(|e| Error::Configuration(Box::new(e)))?;
            let cert_store = builder.build();

            context_builder.set_cert_store(cert_store);
        }

        if let Some(cert) = &self.tls_cert {
            if let Some(key) = &self.tls_key {
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
impl TryInto<rustls_023::ClientConfig> for &ScyllaDBConnectOptions {
    type Error = sqlx::Error;

    fn try_into(self) -> Result<rustls_023::ClientConfig, Self::Error> {
        use rustls_023::{
            ClientConfig, RootCertStore,
            pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        };

        let builder = ClientConfig::builder();

        let builder = if let Some(root_cert) = &self.tls_rootcert {
            let rustls_ca = CertificateDer::from_pem_file(root_cert)
                .map_err(|e| Error::Configuration(Box::new(e)))?;
            let mut root_store = RootCertStore::empty();
            root_store
                .add(rustls_ca)
                .map_err(|e| Error::Configuration(Box::new(e)))?;
            builder.with_root_certificates(root_store)
        } else {
            return Err(Error::Configuration(
                "Root certification file is required.".into(),
            ));
        };

        let client_config = if let Some(cert) = &self.tls_cert {
            if let Some(key) = &self.tls_key {
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

    use claims::{assert_none, assert_some_eq};
    use sqlx::ConnectOptions;

    use crate::{
        ScyllaDBConnectOptions,
        options::{ScyllaDBCompression, ScyllaDBReplicationStrategy},
    };

    #[test]
    fn it_can_parse_url() -> anyhow::Result<()> {
        const URL: &'static str = "scylladb://my_name:my_passwd@localhost/my_keyspace?nodes=example.test,example2.test:9043&tcp_nodelay&tcp_keepalive=40&compression=lz4&replication_strategy=simple&replication_factor=2&page_size=10&tls_rootcert=/etc/tls/root.pem&tls_cert=/etc/tls/client.pem&tls_key=/etc/tls/client.key";
        let options: ScyllaDBConnectOptions = URL.parse()?;

        assert_some_eq!(options.keyspace, "my_keyspace");
        assert!(options.tcp_nodelay);
        assert_some_eq!(options.tcp_keepalive, Duration::from_secs(40));

        assert_eq!(options.host, "localhost");
        assert_eq!(options.port, 9042);

        assert_some_eq!(options.username, "my_name");
        assert_some_eq!(options.password, "my_passwd");

        assert_eq!(vec!["example.test", "example2.test:9043"], options.nodes);

        assert_some_eq!(options.compression, ScyllaDBCompression::LZ4Compressor);

        assert_some_eq!(
            options.replication_strategy,
            ScyllaDBReplicationStrategy::SimpleStrategy
        );
        assert_eq!(options.replication_factor, 2);

        let page_size = options.page_size;
        assert_eq!(10, page_size);

        assert_some_eq!(options.tls_rootcert, "/etc/tls/root.pem");
        assert_some_eq!(options.tls_cert, "/etc/tls/client.pem");
        assert_some_eq!(options.tls_key, "/etc/tls/client.key");

        Ok(())
    }

    #[test]
    fn it_can_convert_options_to_url() -> anyhow::Result<()> {
        const URL: &'static str = "scylladb://my_name:my_passwd@localhost/my_keyspace?nodes=example.test,example2.test:9043&tcp_nodelay&tcp_keepalive=40&compression=lz4&replication_strategy=simple&replication_factor=2&page_size=10&tls_rootcert=/etc/tls/root.pem&tls_cert=/etc/tls/client.pem&tls_key=/etc/tls/client.key";
        let options: ScyllaDBConnectOptions = URL.parse()?;

        let url = options.to_url_lossy();

        assert_eq!(
            url.to_string(),
            "scylladb://my_name:my_passwd@localhost:9042/my_keyspace?nodes=example.test%2Cexample2.test%3A9043&replication_strategy=SimpleStrategy&replication_factor=2&compression=lz4&tcp_nodelay&tcp_keepalive=40&page_size=10&tls_rootcert=%2Fetc%2Ftls%2Froot.pem&tls_cert=%2Fetc%2Ftls%2Fclient.pem&tls_key=%2Fetc%2Ftls%2Fclient.key"
        );

        Ok(())
    }

    #[test]
    fn it_can_add_node() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_eq!(options.nodes.len(), 0);

        let options = options.add_node("example1.test:9043");

        assert_eq!(options.nodes, vec!["example1.test:9043"]);

        Ok(())
    }

    #[test]
    fn it_can_set_keyspace() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_none!(&options.keyspace);

        let options = options.keyspace("test");

        assert_some_eq!(options.keyspace, "test");

        Ok(())
    }

    #[test]
    fn it_can_set_username() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_none!(&options.username);

        let options = options.username("my_name");

        assert_some_eq!(options.username, "my_name");

        Ok(())
    }

    #[test]
    fn it_can_set_password() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_none!(&options.password);

        let options = options.password("my_password");

        assert_some_eq!(options.password, "my_password");

        Ok(())
    }

    #[test]
    fn it_can_set_replication_strategy() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_none!(options.replication_strategy);

        let options =
            options.replication_strategy(ScyllaDBReplicationStrategy::NetworkTopologyStrategy);

        assert_some_eq!(
            options.replication_strategy,
            ScyllaDBReplicationStrategy::NetworkTopologyStrategy,
        );
        assert_eq!(options.replication_factor, 1);

        Ok(())
    }

    #[test]
    fn it_can_parse_replication_strategy_from_str() -> anyhow::Result<()> {
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
    fn it_can_set_replication_factor() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_eq!(options.replication_factor, 1);

        let options = options.replication_factor(2);

        assert_eq!(options.replication_factor, 2);

        Ok(())
    }

    #[test]
    fn it_can_set_compression() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_none!(options.compression);

        let options = options.compresson(ScyllaDBCompression::SnappyCompressor);

        assert_some_eq!(options.compression, ScyllaDBCompression::SnappyCompressor);

        Ok(())
    }

    #[test]
    fn it_can_set_tcp_nodelay() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(!options.tcp_nodelay);

        let options = options.tcp_nodelay();

        assert!(options.tcp_nodelay);

        Ok(())
    }

    #[test]
    fn it_can_set_tcp_keepalive() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert!(options.tcp_keepalive.is_none());

        let options = options.tcp_keepalive(20);

        assert_some_eq!(options.tcp_keepalive, Duration::from_secs(20));

        Ok(())
    }

    #[test]
    fn it_can_set_page_size() -> anyhow::Result<()> {
        let options = ScyllaDBConnectOptions::new();

        assert_eq!(5000, options.page_size);

        let options = options.page_size(200);

        assert_eq!(200, options.page_size);

        Ok(())
    }
}
