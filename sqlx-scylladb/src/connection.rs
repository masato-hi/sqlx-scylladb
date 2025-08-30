mod establish;
mod executor;
mod transaction;

use futures_core::future::BoxFuture;
use scylla::client::caching_session::CachingSession;
use sqlx::{Connection, Transaction};

use crate::{ScyllaDB, ScyllaDBConnectOptions, connection::transaction::ScyllaDBTransaction};

#[derive(Debug)]
pub struct ScyllaDBConnection {
    pub(crate) caching_session: CachingSession,
    pub(crate) transaction: Option<ScyllaDBTransaction>,
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
    pub(crate) fn get_keyspace(&self) -> Option<String> {
        self.caching_session
            .get_session()
            .get_keyspace()
            .as_deref()
            .cloned()
    }
}
