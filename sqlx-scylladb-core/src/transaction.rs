use sqlx_core::{error::Error, sql_str::SqlStr, transaction::TransactionManager};

use crate::{ScyllaDB, ScyllaDBConnection};

/// Implementation of [sqlx::TransactionManager] for ScyllaDB.
pub struct ScyllaDBTransactionManager {}

impl TransactionManager for ScyllaDBTransactionManager {
    type Database = ScyllaDB;

    fn begin<'conn>(
        conn: &'conn mut ScyllaDBConnection,
        statement: Option<SqlStr>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'conn {
        Box::pin(async {
            conn.begin_transaction(statement).await?;

            Ok(())
        })
    }

    fn commit(
        conn: &mut ScyllaDBConnection,
    ) -> impl Future<Output = Result<(), sqlx_core::Error>> + Send + '_ {
        Box::pin(async {
            conn.commit_transaction().await?;

            Ok(())
        })
    }

    fn rollback(
        conn: &mut ScyllaDBConnection,
    ) -> impl Future<Output = Result<(), sqlx_core::Error>> + Send + '_ {
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
