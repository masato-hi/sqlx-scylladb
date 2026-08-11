#![warn(missing_docs)]
#![doc = include_str!("lib.md")]

#[cfg(feature = "any")]
pub mod any;
mod arguments;
mod column;
mod connection;
mod database;
mod error;
#[cfg(feature = "migrate")]
mod migrate;
mod options;
mod query_result;
mod row;
mod statement;
#[cfg(feature = "migrate")]
mod testing;
mod transaction;
mod type_info;
mod types;
mod value;

#[cfg(feature = "bigdecimal-04")]
pub use arguments::ScyllaDBArgumentNativeArrayDecimal;
#[cfg(feature = "bigdecimal-04")]
pub use arguments::ScyllaDBArgumentNativeDecimal;
pub use arguments::{
    ScyllaDBArgument, ScyllaDBArgumentBuffer, ScyllaDBArgumentNative, ScyllaDBArgumentNativeArray,
    ScyllaDBArgumentNativeBlob, ScyllaDBArgumentNativeDate, ScyllaDBArgumentNativeText,
    ScyllaDBArgumentNativeTime, ScyllaDBArgumentNativeTimestamp, ScyllaDBArguments,
};
pub use arguments::{
    ScyllaDBArgumentNativeArrayBlob, ScyllaDBArgumentNativeArrayDate,
    ScyllaDBArgumentNativeArrayText, ScyllaDBArgumentNativeArrayTime,
    ScyllaDBArgumentNativeArrayTimestamp,
};
pub use column::ScyllaDBColumn;
pub use connection::ScyllaDBConnection;
pub use database::ScyllaDB;
pub use error::ScyllaDBError;
pub use options::{ScyllaDBCompression, ScyllaDBConnectOptions, ScyllaDBReplicationStrategy};
pub use query_result::ScyllaDBQueryResult;
pub use row::ScyllaDBRow;
use sqlx_core::{
    executor::Executor,
    impl_acquire, impl_column_index_for_row, impl_column_index_for_statement,
    impl_encode_for_option, impl_into_arguments_for_arguments,
    pool::{Pool, PoolOptions},
    transaction::Transaction,
};
pub use statement::ScyllaDBStatement;
pub use transaction::ScyllaDBTransactionManager;
pub use type_info::{ScyllaDBTypeInfo, ScyllaDBTypeInfoNative, ScyllaDBTypeInfoNativeArray};
pub use types::array::ScyllaDBHasArrayType;
pub use types::user_defined_type::UserDefinedType;
pub use value::{ScyllaDBValue, ScyllaDBValueRef};

/// An alias for [sqlx::Pool], specialized for ScyllaDB.
pub type ScyllaDBPool = Pool<ScyllaDB>;

/// An alias for [sqlx::pool::PoolOptions], specialized for ScyllaDB.
pub type ScyllaDBPoolOptions = PoolOptions<ScyllaDB>;

/// An alias for [`sqlx::Executor<'_, Database = ScyllaDB>`][sqlx::Executor].
pub trait ScyllaDBExecutor<'c>: Executor<'c, Database = ScyllaDB> {}
impl<'c, T: Executor<'c, Database = ScyllaDB>> ScyllaDBExecutor<'c> for T {}

/// An alias for [`sqlx::Transaction<'_, ScyllaDB>`][sqlx::Transaction].
pub type ScyllaDBTransaction<'c> = Transaction<'c, ScyllaDB>;

/// An alias for [`sqlx::Type<ScyllaDB>`][sqlx::Type].
pub trait ScyllaDBType: sqlx_core::types::Type<ScyllaDB> {}

impl_into_arguments_for_arguments!(ScyllaDBArguments);
impl_acquire!(ScyllaDB, ScyllaDBConnection);
impl_column_index_for_row!(ScyllaDBRow);
impl_column_index_for_statement!(ScyllaDBStatement);
impl_encode_for_option!(ScyllaDB);
