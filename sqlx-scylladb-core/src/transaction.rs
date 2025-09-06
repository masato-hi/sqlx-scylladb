use std::borrow::Cow;

use futures_core::future::BoxFuture;
use sqlx::TransactionManager;

use crate::{ScyllaDB, ScyllaDBConnection};

pub struct ScyllaDBTransactionManager {}

impl TransactionManager for ScyllaDBTransactionManager {
    type Database = ScyllaDB;

    fn begin<'conn>(
        conn: &'conn mut ScyllaDBConnection,
        statement: Option<Cow<'static, str>>,
    ) -> BoxFuture<'conn, Result<(), sqlx::Error>> {
        Box::pin(async {
            conn.begin_transaction(statement).await?;

            Ok(())
        })
    }

    fn commit(conn: &mut ScyllaDBConnection) -> BoxFuture<'_, Result<(), sqlx::Error>> {
        Box::pin(async {
            conn.commit_transaction().await?;

            Ok(())
        })
    }

    fn rollback(conn: &mut ScyllaDBConnection) -> BoxFuture<'_, Result<(), sqlx::Error>> {
        Box::pin(async {
            let _ = conn.rollback_transaction();

            Ok(())
        })
    }

    fn start_rollback(conn: &mut ScyllaDBConnection) {
        let _ = conn.rollback_transaction();
    }

    fn get_transaction_depth(conn: &ScyllaDBConnection) -> usize {
        conn.get_transaction_depth()
    }
}
