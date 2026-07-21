#![allow(missing_docs)]

use std::pin::pin;

use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
pub use sqlx_core::any::driver::install_drivers;
use sqlx_core::{
    Either,
    any::{
        AnyArguments, AnyColumn, AnyConnectOptions, AnyConnectionBackend, AnyQueryResult, AnyRow,
        AnyStatement, AnyTypeInfo, AnyTypeInfoKind, AnyValueKind,
    },
    connection::Connection,
    database::Database,
    executor::Executor,
    sql_str::SqlStr,
    transaction::TransactionManager,
};

use crate::{
    ScyllaDB, ScyllaDBArgument, ScyllaDBArgumentBuffer, ScyllaDBArguments, ScyllaDBColumn,
    ScyllaDBConnectOptions, ScyllaDBConnection, ScyllaDBQueryResult, ScyllaDBRow,
    ScyllaDBTransactionManager, ScyllaDBTypeInfo,
};

sqlx_core::declare_driver_with_optional_migrate!(DRIVER = ScyllaDB);

impl AnyConnectionBackend for ScyllaDBConnection {
    fn name(&self) -> &str {
        ScyllaDB::NAME
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self).boxed()
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self).boxed()
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::ping(self).boxed()
    }

    fn begin(&mut self, statement: Option<SqlStr>) -> BoxFuture<'_, sqlx_core::Result<()>> {
        ScyllaDBTransactionManager::begin(self, statement).boxed()
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        ScyllaDBTransactionManager::commit(self).boxed()
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        ScyllaDBTransactionManager::rollback(self).boxed()
    }

    fn start_rollback(&mut self) {
        ScyllaDBTransactionManager::start_rollback(self)
    }

    fn shrink_buffers(&mut self) {
        Connection::shrink_buffers(self)
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self).boxed()
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    #[cfg(feature = "migrate")]
    fn as_migrate(
        &mut self,
    ) -> sqlx_core::Result<&mut (dyn sqlx_core::migrate::Migrate + Send + 'static)> {
        Ok(self)
    }

    fn fetch_many(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxStream<'_, sqlx_core::Result<sqlx_core::Either<AnyQueryResult, AnyRow>>> {
        let persistent = persistent && arguments.is_some();

        let arguments = arguments.map(map_arguments);

        Box::pin({
            self.run(query, arguments, persistent)
                .try_flatten_stream()
                .map(|res| {
                    Ok(match res? {
                        Either::Left(result) => Either::Left(map_result(result)),
                        Either::Right(row) => Either::Right(AnyRow::try_from(&row)?),
                    })
                })
        })
    }

    fn fetch_optional(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxFuture<'_, sqlx_core::Result<Option<AnyRow>>> {
        let persistent = persistent && arguments.is_some();
        let arguments = arguments
            .map(AnyArguments::convert_into)
            .transpose()
            .map_err(sqlx_core::Error::Encode);

        Box::pin(async move {
            let arguments = arguments?;
            let mut stream = pin!(self.run(query, arguments, persistent).await?);

            if let Some(Either::Right(row)) = stream.try_next().await? {
                return Ok(Some(AnyRow::try_from(&row)?));
            }

            Ok(None)
        })
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: SqlStr,
        _parameters: &[sqlx_core::any::AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<AnyStatement>> {
        Box::pin(async move {
            let statement = Executor::prepare_with(self, sql, &[]).await?;
            let column_names = statement.metadata.column_names.clone();
            AnyStatement::try_from_statement(statement, column_names)
        })
    }

    fn describe<'c>(
        &mut self,
        sql: SqlStr,
    ) -> BoxFuture<'_, sqlx_core::Result<sqlx_core::describe::Describe<sqlx_core::any::Any>>> {
        Box::pin(async move {
            let describe = Executor::describe(self, sql).await?;

            let columns = describe
                .columns
                .iter()
                .map(AnyColumn::try_from)
                .collect::<Result<Vec<_>, _>>()?;

            let parameters = match describe.parameters {
                Some(Either::Left(parameters)) => Some(Either::Left(
                    parameters
                        .iter()
                        .enumerate()
                        .map(|(i, type_info)| {
                            AnyTypeInfo::try_from(type_info).map_err(|_| {
                                sqlx_core::Error::AnyDriverError(
                                    format!(
                                        "Any driver does not support type {type_info} of parameter {i}"
                                    )
                                    .into(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                Some(Either::Right(count)) => Some(Either::Right(count)),
                None => None,
            };

            Ok(sqlx_core::describe::Describe {
                columns,
                parameters,
                nullable: describe.nullable,
            })
        })
    }
}

impl<'a> TryFrom<&'a AnyConnectOptions> for ScyllaDBConnectOptions {
    type Error = sqlx_core::Error;

    fn try_from(any_opts: &'a AnyConnectOptions) -> Result<Self, Self::Error> {
        let opts = Self::parse_from_url(&any_opts.database_url)?;
        Ok(opts)
    }
}

impl<'a> TryFrom<&'a ScyllaDBTypeInfo> for AnyTypeInfo {
    type Error = sqlx_core::Error;

    fn try_from(type_info: &'a ScyllaDBTypeInfo) -> Result<Self, Self::Error> {
        Ok(AnyTypeInfo {
            kind: match &type_info {
                ScyllaDBTypeInfo::Boolean => AnyTypeInfoKind::Bool,
                ScyllaDBTypeInfo::SmallInt => AnyTypeInfoKind::SmallInt,
                ScyllaDBTypeInfo::Int => AnyTypeInfoKind::Integer,
                ScyllaDBTypeInfo::BigInt | ScyllaDBTypeInfo::Counter => AnyTypeInfoKind::BigInt,
                ScyllaDBTypeInfo::Float => AnyTypeInfoKind::Real,
                ScyllaDBTypeInfo::Double => AnyTypeInfoKind::Double,
                ScyllaDBTypeInfo::Blob => AnyTypeInfoKind::Blob,
                ScyllaDBTypeInfo::Text | ScyllaDBTypeInfo::Ascii => AnyTypeInfoKind::Text,
                _ => {
                    return Err(sqlx_core::Error::AnyDriverError(
                        format!("Any driver does not support the ScyllaDB type {type_info:?}")
                            .into(),
                    ));
                }
            },
        })
    }
}

impl<'a> TryFrom<&'a ScyllaDBColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(col: &'a ScyllaDBColumn) -> Result<Self, Self::Error> {
        let type_info =
            AnyTypeInfo::try_from(&col.type_info).map_err(|e| sqlx_core::Error::ColumnDecode {
                index: col.name.to_string(),
                source: e.into(),
            })?;

        Ok(AnyColumn {
            ordinal: col.ordinal,
            name: col.name.clone(),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a ScyllaDBRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a ScyllaDBRow) -> Result<Self, Self::Error> {
        AnyRow::map_from(row, row.column_names())
    }
}

fn map_arguments(args: AnyArguments) -> ScyllaDBArguments {
    let capacity = args.values.0.capacity();
    let mut types = Vec::with_capacity(capacity);
    let mut buffer = Vec::with_capacity(capacity);

    for val in args.values.0.into_iter() {
        let (r#type, argument) = match val {
            AnyValueKind::Null(_) => (ScyllaDBTypeInfo::Null, ScyllaDBArgument::Null),
            AnyValueKind::Bool(b) => (ScyllaDBTypeInfo::Boolean, ScyllaDBArgument::Boolean(b)),
            AnyValueKind::SmallInt(i) => {
                (ScyllaDBTypeInfo::SmallInt, ScyllaDBArgument::SmallInt(i))
            }
            AnyValueKind::Integer(i) => (ScyllaDBTypeInfo::Int, ScyllaDBArgument::Int(i)),
            AnyValueKind::BigInt(i) => (ScyllaDBTypeInfo::BigInt, ScyllaDBArgument::BigInt(i)),
            AnyValueKind::Real(r) => (ScyllaDBTypeInfo::Float, ScyllaDBArgument::Float(r)),
            AnyValueKind::Double(d) => (ScyllaDBTypeInfo::Double, ScyllaDBArgument::Double(d)),
            AnyValueKind::Text(t) => (
                ScyllaDBTypeInfo::Text,
                ScyllaDBArgument::Text(t.to_string()),
            ),
            AnyValueKind::Blob(b) => (ScyllaDBTypeInfo::Blob, ScyllaDBArgument::Blob(b.to_vec())),
            // AnyValueKind is `#[non_exhaustive]` but we should have covered everything
            _ => unreachable!("BUG: missing mapping for {val:?}"),
        };

        types.push(r#type);
        buffer.push(argument);
    }
    let buffer = ScyllaDBArgumentBuffer { buffer };
    ScyllaDBArguments { types, buffer }
}

fn map_result(result: ScyllaDBQueryResult) -> AnyQueryResult {
    AnyQueryResult {
        rows_affected: result.rows_affected,
        last_insert_id: None,
    }
}
