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

/// Represents all the ways a method can fail within ScyllaDB.
#[derive(Debug, Error)]
#[error(transparent)]
pub enum ScyllaDBError {
    /// There is an error in the options.
    #[error("Configuration error. {0}")]
    ConfigurationError(String),
    /// Error occurred while creating the session.
    NewSessionError(#[from] NewSessionError),
    /// There is an error in the specified keyspace.
    UseKeyspaceError(#[from] UseKeyspaceError),
    /// Error occurred while preparing the statement.
    PrepareError(#[from] PrepareError),
    /// Error occurred while converting to rows result.
    IntoRowsResultError(#[from] IntoRowsResultError),
    /// Error occurred while retrieving the row.
    RowsError(#[from] RowsError),
    /// Error occurred while type checking.
    TypeCheckError(#[from] TypeCheckError),
    /// Error occurred while serialization.
    SerializationError(#[from] SerializationError),
    /// Error occurred while deserialization.
    DeserializationError(#[from] DeserializationError),
    /// Error occurred while execution.
    ExecutionError(#[from] ExecutionError),
    /// Error occurred while pagination.
    PagerExecutionError(#[from] PagerExecutionError),
    /// Transaction is not started.
    #[error("Transaction is not started.")]
    TransactionNotStarted,
    /// Attempted to retrieve data exceeding the number of columns.
    #[error("Column index out of bounds. the len is {len}, but the index is {index}")]
    ColumnIndexOutOfBounds {
        /// index.
        index: usize,
        /// total size.
        len: usize,
    },
    /// Column types do not match.
    #[error("Column type is mismatched. expect: {expect:?}, actual: {actual:?}")]
    ColumnTypeError {
        /// expected column type.
        expect: ColumnType<'static>,
        /// actual column type.
        actual: ColumnType<'static>,
    },
    /// Failed to acquire migration lock.
    #[error("Failed to acquire migration lock.")]
    MigrationLockError,
    /// Column types do not match.
    #[error("Mismatched column type {0}: {1:?}..")]
    MismatchedColumnTypeError(UStr, ColumnType<'static>),
    /// This column type is not supported.
    #[error("Column type '{0:?}' is not supported.")]
    ColumnTypeNotSupportedError(ColumnType<'static>),
    /// The value is null.
    #[error("{0:?} is null.")]
    NullValueError(UStr),
    /// Failed to acquire exclusive lock.
    #[error("Exclusive lock error.")]
    ExclusiveLockError,
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
            ScyllaDBError::ExclusiveLockError => "Exclusive lock error.",
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
