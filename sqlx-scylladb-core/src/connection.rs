mod establish;
mod executor;
mod transaction;

use std::fmt::Debug;

use futures_core::future::BoxFuture;
use scylla::client::caching_session::CachingSession;
use sqlx::{Connection, Transaction};

use crate::{ScyllaDB, ScyllaDBConnectOptions, connection::transaction::ScyllaDBTransaction};

pub struct ScyllaDBConnection {
    pub(crate) caching_session: CachingSession,
    pub(crate) page_size: i32,
    pub(crate) transaction: Option<ScyllaDBTransaction>,
}

impl Debug for ScyllaDBConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScyllaDBConnection")
            .field("caching_session", &self.caching_session)
            .field("page_size", &self.page_size)
            .finish()
    }
}

impl Connection for ScyllaDBConnection {
    type Database = ScyllaDB;

    type Options = ScyllaDBConnectOptions;

    fn close(self) -> BoxFuture<'static, Result<(), sqlx::Error>> {
        Box::pin(async move { Ok(()) })
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), sqlx::Error>> {
        Box::pin(async move { Ok(()) })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), sqlx::Error>> {
        Box::pin(async move {
            let state = self.caching_session.get_session().get_cluster_state();
            let nodes = state.get_nodes_info();
            for node in nodes {
                if !node.is_connected() {
                    return Err(sqlx::Error::PoolClosed);
                }
            }
            Ok(())
        })
    }

    fn begin(&mut self) -> BoxFuture<'_, Result<sqlx::Transaction<'_, Self::Database>, sqlx::Error>>
    where
        Self: Sized,
    {
        Transaction::begin(self, None)
    }

    fn shrink_buffers(&mut self) {
        ()
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), sqlx::Error>> {
        Box::pin(async move { Ok(()) })
    }

    fn should_flush(&self) -> bool {
        false
    }
}

impl ScyllaDBConnection {
    #[cfg(feature = "migrate")]
    pub(crate) fn get_keyspace(&self) -> Option<String> {
        self.caching_session
            .get_session()
            .get_keyspace()
            .as_deref()
            .cloned()
    }
}
