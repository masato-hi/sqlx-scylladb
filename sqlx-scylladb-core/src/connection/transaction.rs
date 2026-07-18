use scylla::statement::{Statement, batch::Batch};

use crate::{ScyllaDBArguments, ScyllaDBConnection, ScyllaDBError};
use sqlx_core::sql_str::SqlStr;

pub(crate) struct ScyllaDBTransaction {
    statements: Vec<SqlStr>,
    arguments: Vec<ScyllaDBArguments>,
}

impl Default for ScyllaDBTransaction {
    fn default() -> Self {
        Self {
            statements: Default::default(),
            arguments: Default::default(),
        }
    }
}

impl ScyllaDBConnection {
    pub(crate) async fn begin_transaction(
        &mut self,
        statement: Option<SqlStr>,
    ) -> Result<(), ScyllaDBError> {
        if self.transaction.is_none() {
            self.transaction = Some(ScyllaDBTransaction::default())
        }

        if let Some(statement) = statement {
            self.append_to_transaction(statement, None).await?;
        }

        Ok(())
    }

    pub(crate) async fn commit_transaction(&mut self) -> Result<(), ScyllaDBError> {
        if let Some(transaction) = &self.transaction {
            let mut batch = Batch::default();
            for statement in &transaction.statements {
                let statement = Statement::new(statement.as_str());
                batch.append_statement(statement);
            }

            self.caching_session
                .batch(&batch, &transaction.arguments)
                .await?;
        }

        self.transaction = None;

        Ok(())
    }

    pub(crate) fn rollback_transaction(&mut self) -> Result<(), ScyllaDBError> {
        self.transaction = None;

        Ok(())
    }

    pub(crate) fn get_transaction_depth(&self) -> usize {
        if self.transaction.is_some() { 1 } else { 0 }
    }

    pub(crate) async fn append_to_transaction<'e, 'c: 'e, 'q: 'e>(
        &'c mut self,
        sql: SqlStr,
        arguments: Option<ScyllaDBArguments>,
    ) -> Result<(), ScyllaDBError> {
        if let Some(transaction) = &mut self.transaction {
            transaction.statements.push(sql);
            transaction.arguments.push(arguments.unwrap_or_default());
        } else {
            return Err(ScyllaDBError::TransactionNotStarted);
        }

        Ok(())
    }
}
