use scylla::client::{
    caching_session::CachingSessionBuilder, session::TlsContext, session_builder::SessionBuilder,
};
use sqlx::Error;

use crate::{ScyllaDBConnectOptions, ScyllaDBConnection, ScyllaDBError};

impl ScyllaDBConnection {
    pub(crate) async fn establish(options: &ScyllaDBConnectOptions) -> Result<Self, Error> {
        let mut builder = SessionBuilder::new().known_nodes(&options.get_connect_nodes());

        if let Some(username) = &options.username {
            let password = options.password.clone().unwrap_or_default();
            builder = builder.user(username, password);
        }
        if options.tls_rootcert.is_some() || options.tls_cert.is_some() {
            let tls_context: TlsContext = options.try_into()?;
            builder = builder.tls_context(Some(tls_context));
        }
        if let Some(compression) = options.compression {
            let compression = compression.into();
            builder = builder.compression(Some(compression));
        }
        if let Some(tcp_keepalive) = options.tcp_keepalive {
            builder = builder.tcp_keepalive_interval(tcp_keepalive);
        }
        if options.tcp_nodelay {
            builder = builder.tcp_nodelay(true);
        }

        let session = builder
            .build()
            .await
            .map_err(ScyllaDBError::NewSessionError)?;

        if let Some(keyspace) = &options.keyspace {
            session
                .use_keyspace(keyspace, true)
                .await
                .map_err(ScyllaDBError::UseKeyspaceError)?;
        }

        let mut builder = CachingSessionBuilder::new(session);
        builder = builder.max_capacity(options.statement_cache_capacity);
        let session = builder.build();

        let conn = ScyllaDBConnection {
            caching_session: session,
            page_size: options.page_size,
            transaction: None,
        };

        Ok(conn)
    }
}
