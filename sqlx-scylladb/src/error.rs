use std::error::Error as StdError;

use scylla::{
    cluster::metadata::ColumnType,
    errors::{DeserializationError, ExecutionError, IntoRowsResultError, PrepareError, RowsError},
};
use sqlx::error::{DatabaseError, ErrorKind};
use sqlx_core::ext::ustr::UStr;
use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
pub enum ScyllaDBError {
    PrepareError(#[from] PrepareError),
    IntoRowsResultError(#[from] IntoRowsResultError),
    RowsError(#[from] RowsError),
    DeserializationError(#[from] DeserializationError),
    ExecutionError(#[from] ExecutionError),
    #[error("Cannot lock on migration.")]
    MigrationLockError,
    #[error("Mismatched type {0}: {1:?}..")]
    MismatchedTypeError(UStr, ColumnType<'static>),
    #[error("{0:?} is null.")]
    NullValueError(UStr),
    #[error("{0} type is not supported.")]
    NotSupportedType(&'static str),
}

impl DatabaseError for ScyllaDBError {
    fn message(&self) -> &str {
        todo!()
    }

    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        todo!()
    }

    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        todo!()
    }

    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        todo!()
    }

    fn kind(&self) -> ErrorKind {
        todo!()
    }
}
