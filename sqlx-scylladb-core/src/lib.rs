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
mod testing;
mod transaction;
mod type_info;
pub mod types;
mod value;

pub use arguments::{ScyllaDBArgument, ScyllaDBArgumentBuffer, ScyllaDBArguments};
pub use column::ScyllaDBColumn;
pub use connection::ScyllaDBConnection;
pub use database::ScyllaDB;
pub use error::ScyllaDBError;
pub use options::{ScyllaDBCompressor, ScyllaDBConnectOptions, ScyllaDBReplicationStrategy};
pub use query_result::ScyllaDBQueryResult;
pub use row::ScyllaDBRow;
use sqlx::{Executor, Pool, Transaction, pool::PoolOptions};
use sqlx_core::{
    impl_acquire, impl_column_index_for_row, impl_column_index_for_statement,
    impl_encode_for_option, impl_into_arguments_for_arguments,
};
pub use statement::ScyllaDBStatement;
pub use transaction::ScyllaDBTransactionManager;
pub use type_info::{ScyllaDBTypeInfo, register_any_type};
pub use types::array::ScyllaDBHasArrayType;
pub use value::{ScyllaDBValue, ScyllaDBValueRef};

pub type ScyllaDBPool = Pool<ScyllaDB>;

pub type ScyllaDBPoolOptions = PoolOptions<ScyllaDB>;

pub trait ScyllaDBExecutor<'c>: Executor<'c, Database = ScyllaDB> {}
impl<'c, T: Executor<'c, Database = ScyllaDB>> ScyllaDBExecutor<'c> for T {}

pub type ScyllaDBTransaction<'c> = Transaction<'c, ScyllaDB>;

pub trait ScyllaDBType: sqlx::Type<ScyllaDB> {}

impl_into_arguments_for_arguments!(ScyllaDBArguments);
impl_acquire!(ScyllaDB, ScyllaDBConnection);
impl_column_index_for_row!(ScyllaDBRow);
impl_column_index_for_statement!(ScyllaDBStatement);
impl_encode_for_option!(ScyllaDB);
