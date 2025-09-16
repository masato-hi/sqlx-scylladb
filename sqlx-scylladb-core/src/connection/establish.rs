use scylla::client::{
    caching_session::CachingSessionBuilder, session::TlsContext, session_builder::SessionBuilder,
};
use sqlx::Error;

use crate::{ScyllaDBConnectOptions, ScyllaDBConnection, ScyllaDBError};

impl ScyllaDBConnection {
    pub(crate) async fn establish(options: &ScyllaDBConnectOptions) -> Result<Self, Error> {
        let mut builder = SessionBuilder::new().known_nodes(&options.nodes);

        if let Some(authentication_options) = &options.authentication_options {
            builder = builder.user(
                &authentication_options.username,
                &authentication_options.password,
            );
        }
        if let Some(tls_options) = &options.tls_options {
            let tls_context: TlsContext = tls_options.clone().try_into()?;
            builder = builder.tls_context(Some(tls_context));
        }
        if let Some(compression_options) = options.compression_options {
            let compression = compression_options.compressor.into();
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
