use std::error::Error as StdError;

use scylla::{
    cluster::metadata::ColumnType,
    errors::{
        DeserializationError, ExecutionError, IntoRowsResultError, NewSessionError,
        PagerExecutionError, PrepareError, RowsError, SerializationError, TypeCheckError,
        UseKeyspaceError,
    },
};
use sqlx::error::{DatabaseError, ErrorKind};
use sqlx_core::ext::ustr::UStr;
use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
pub enum ScyllaDBError {
    #[error("Configuration error. {0}")]
    ConfigurationError(String),
    NewSessionError(#[from] NewSessionError),
    UseKeyspaceError(#[from] UseKeyspaceError),
    PrepareError(#[from] PrepareError),
    IntoRowsResultError(#[from] IntoRowsResultError),
    RowsError(#[from] RowsError),
    TypeCheckError(#[from] TypeCheckError),
    SerializationError(#[from] SerializationError),
    DeserializationError(#[from] DeserializationError),
    ExecutionError(#[from] ExecutionError),
    PagerExecutionError(#[from] PagerExecutionError),
    #[error("Transaction is not started.")]
    TransactionNotStarted,
    #[error("Column index out of bounds. the len is {len}, but the index is {index}")]
    ColumnIndexOutOfBounds {
        index: usize,
        len: usize,
    },
    #[error("Column type is mismatched. expect: {expect:?}, actual: {actual:?}")]
    ColumnTypeError {
        expect: ColumnType<'static>,
        actual: ColumnType<'static>,
    },
    #[error("Cannot lock on migration.")]
    MigrationLockError,
    #[error("Mismatched column type {0}: {1:?}..")]
    MismatchedColumnTypeError(UStr, ColumnType<'static>),
    #[error("Column type '{0:?}' is not supported.")]
    ColumnTypeNotSupportedError(ColumnType<'static>),
    #[error("{0:?} is null.")]
    NullValueError(UStr),
}

impl DatabaseError for ScyllaDBError {
    fn message(&self) -> &str {
        match self {
            ScyllaDBError::ConfigurationError(message) => &message,
            ScyllaDBError::NewSessionError(_) => "New session error.",
            ScyllaDBError::UseKeyspaceError(_) => "Use keyspace error.",
            ScyllaDBError::PrepareError(_) => "Prepare error.",
            ScyllaDBError::IntoRowsResultError(_) => "Into rows result error.",
            ScyllaDBError::RowsError(_) => "Rows error.",
            ScyllaDBError::TypeCheckError(_) => "Type check error.",
            ScyllaDBError::SerializationError(_) => "Serialization error.",
            ScyllaDBError::DeserializationError(_) => "Deserialization error.",
            ScyllaDBError::ExecutionError(_) => "Execution error.",
            ScyllaDBError::PagerExecutionError(_) => "Pager execution error.",
            ScyllaDBError::TransactionNotStarted => "Transaction is not started.",
            ScyllaDBError::ColumnIndexOutOfBounds { index: _, len: _ } => {
                "Column index out of bounds."
            }
            ScyllaDBError::ColumnTypeError {
                expect: _,
                actual: _,
            } => "Column type error.",
            ScyllaDBError::MigrationLockError => "Migration lock error.",
            ScyllaDBError::MismatchedColumnTypeError(_, _) => "Mismatched column type.",
            ScyllaDBError::ColumnTypeNotSupportedError(_) => "Column type not supported.",
            ScyllaDBError::NullValueError(_) => "Null value error",
        }
    }

    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}
