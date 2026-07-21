use sqlx_core::database::{Database, HasStatementCache};

use crate::{
    ScyllaDBArguments, ScyllaDBColumn, ScyllaDBConnection, ScyllaDBQueryResult, ScyllaDBRow,
    ScyllaDBStatement, ScyllaDBTransactionManager, ScyllaDBTypeInfo, ScyllaDBValue,
    ScyllaDBValueRef, arguments::ScyllaDBArgumentBuffer,
};

/// ScyllaDB database driver.
#[derive(Debug)]
pub struct ScyllaDB;

impl Database for ScyllaDB {
    type Connection = ScyllaDBConnection;

    type TransactionManager = ScyllaDBTransactionManager;

    type Row = ScyllaDBRow;

    type QueryResult = ScyllaDBQueryResult;

    type Column = ScyllaDBColumn;

    type TypeInfo = ScyllaDBTypeInfo;

    type Value = ScyllaDBValue;

    type ValueRef<'r> = ScyllaDBValueRef<'r>;

    type Arguments = ScyllaDBArguments;

    type ArgumentBuffer = ScyllaDBArgumentBuffer;

    type Statement = ScyllaDBStatement;

    const NAME: &'static str = "ScyllaDB";

    const URL_SCHEMES: &'static [&'static str] = &["scylladb"];
}

impl HasStatementCache for ScyllaDB {}
