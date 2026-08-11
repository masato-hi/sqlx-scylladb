use std::error::Error as StdError;

use scylla::{
    cluster::metadata::ColumnType,
    errors::{
        DeserializationError, ExecutionError, IntoRowsResultError, NewSessionError,
        PagerExecutionError, PrepareError, RowsError, SerializationError, TypeCheckError,
        UseKeyspaceError,
    },
};
use sqlx_core::error::{DatabaseError, ErrorKind};
use sqlx_core::ext::ustr::UStr;
use thiserror::Error;

/// Errors that can occur while using the ScyllaDB driver.
#[derive(Debug, Error)]
#[error(transparent)]
pub enum ScyllaDBError {
    /// The connection or migration options are invalid.
    #[error("Configuration error. {0}")]
    ConfigurationError(String),
    /// An error occurred while creating the ScyllaDB session.
    NewSessionError(#[from] NewSessionError),
    /// An error occurred while selecting the specified keyspace.
    UseKeyspaceError(#[from] UseKeyspaceError),
    /// An error occurred while preparing a statement.
    PrepareError(#[from] PrepareError),
    /// An error occurred while converting a response into rows.
    IntoRowsResultError(#[from] IntoRowsResultError),
    /// An error occurred while retrieving rows.
    RowsError(#[from] RowsError),
    /// An error occurred during type checking.
    TypeCheckError(#[from] TypeCheckError),
    /// An error occurred while serializing a value.
    SerializationError(#[from] SerializationError),
    /// An error occurred while deserializing a value.
    DeserializationError(#[from] DeserializationError),
    /// An error occurred while executing a statement.
    ExecutionError(#[from] ExecutionError),
    /// An error occurred while fetching a subsequent page.
    PagerExecutionError(#[from] PagerExecutionError),
    /// An operation requiring an active transaction was requested without one.
    #[error("Transaction is not started.")]
    TransactionNotStarted,
    /// A column index was outside the bounds of the row.
    #[error("Column index out of bounds. the len is {len}, but the index is {index}")]
    ColumnIndexOutOfBounds {
        /// The requested column index.
        index: usize,
        /// The number of columns in the row.
        len: usize,
    },
    /// The expected and actual column types do not match.
    #[error("Column type is mismatched. expect: {expect:?}, actual: {actual:?}")]
    ColumnTypeError {
        /// The expected column type.
        expect: ColumnType<'static>,
        /// The actual column type.
        actual: ColumnType<'static>,
    },
    /// Failed to acquire migration lock.
    #[error("Failed to acquire migration lock.")]
    MigrationLockError,
    /// A column type does not match the type required by the operation.
    #[error("Mismatched column type {0}: {1:?}..")]
    MismatchedColumnTypeError(UStr, ColumnType<'static>),
    /// This column type is not supported.
    #[error("Column type '{0:?}' is not supported.")]
    ColumnTypeNotSupportedError(ColumnType<'static>),
    /// A non-null value was required, but the column contained `NULL`.
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
