use std::{borrow::Cow, ops::ControlFlow, pin::pin};

use bytes::Bytes;
use futures_core::{Stream, future::BoxFuture, stream::BoxStream};
use futures_util::TryStreamExt;
use scylla::{
    deserialize::row::ColumnIterator,
    response::{
        PagingState, PagingStateResponse,
        query_result::{ColumnSpecs, QueryResult},
    },
    statement::Statement,
};
use sqlx::{Connection, Describe, Either, Error, Executor, Row};
use sqlx_core::{ext::ustr::UStr, try_stream};

use crate::{
    ScyllaDB, ScyllaDBArguments, ScyllaDBColumn, ScyllaDBConnection, ScyllaDBError,
    ScyllaDBQueryResult, ScyllaDBRow, ScyllaDBStatement, ScyllaDBTypeInfo,
    statement::ScyllaDBStatementMetadata,
};

const APPLIED_COLUMN: &'static str = "[applied]";

impl ScyllaDBConnection {
    async fn execute_single_page<'e, 'c: 'e, 'q: 'e>(
        &'c mut self,
        sql: &str,
        arguments: &Option<ScyllaDBArguments>,
        persistent: bool,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ScyllaDBError> {
        if persistent {
            let (query_result, paging_state_response) = if let Some(arguments) = arguments {
                self.caching_session
                    .execute_single_page(sql, arguments, paging_state)
                    .await?
            } else {
                self.caching_session
                    .execute_single_page(sql, (), paging_state)
                    .await?
            };

            Ok((query_result, paging_state_response))
        } else {
            let session = self.caching_session.get_session();

            let (query_result, paging_state_response) = if let Some(arguments) = arguments {
                session
                    .query_single_page(sql, arguments, paging_state)
                    .await?
            } else {
                session.query_single_page(sql, (), paging_state).await?
            };

            Ok((query_result, paging_state_response))
        }
    }

    pub(crate) async fn run<'e, 'c: 'e, 'q: 'e>(
        &'c mut self,
        sql: &'q str,
        arguments: Option<ScyllaDBArguments>,
        persistent: bool,
    ) -> Result<
        impl Stream<Item = Result<Either<ScyllaDBQueryResult, ScyllaDBRow>, Error>> + 'e,
        Error,
    > {
        Ok(try_stream! {
            let statement = self.prepare(sql).await?;

            // INSERT, UPDATE, and DELETE queries during transactions are processed in batches.
            let in_batch = self.is_in_transaction() && statement.is_affect_statement;

            if !in_batch {
                let mut paging_state = PagingState::start();

                loop {
                    let (query_result, paging_state_response) = self.execute_single_page(&statement.sql, &arguments, persistent, paging_state.clone()).await?;

                    if !query_result.is_rows() {
                        break;
                    }

                    let rows_result = query_result.into_rows_result().map_err(ScyllaDBError::IntoRowsResultError)?;
                    let column_specs = rows_result.column_specs();
                    let metadata = ScyllaDBStatementMetadata::from_column_specs(column_specs)?;

                    let is_lwt = column_specs.is_lwt();
                    let rows_num = rows_result.rows_num() as u64;
                    let mut rows_affected = 0;

                    let rows = rows_result.rows::<ColumnIterator<'_,'_>>().map_err(ScyllaDBError::RowsError)?;
                    for row in rows {
                        let row = row.map_err(ScyllaDBError::DeserializationError)?;

                        let mut columns: Vec<Option<Bytes>> = Vec::with_capacity(row.columns_remaining());
                        for column in row {
                            let column = column.map_err(ScyllaDBError::DeserializationError)?;
                            let column = match column.slice {
                                Some(slice) => {
                                    Some(slice.to_bytes())
                                },
                                None => None,
                            };
                            columns.push(column)
                        }

                        let scylladb_row = ScyllaDBRow::new(columns, metadata.clone());

                        if is_lwt {
                            let applied: bool = scylladb_row.try_get(APPLIED_COLUMN).unwrap_or(false);
                            if applied {
                                rows_affected += 1;
                            }
                        }

                        r#yield!(Either::Right(scylladb_row))
                    }

                    r#yield!(Either::Left(ScyllaDBQueryResult { rows_num, rows_affected }));

                    match paging_state_response.into_paging_control_flow() {
                        ControlFlow::Break(()) => {
                            break;
                        }
                        ControlFlow::Continue(new_paging_state) => {
                            paging_state = new_paging_state
                        }
                    }
                }
            } else {
                self.insert_transactional(sql, arguments).await?;
            }

            Ok(())
        })
    }
}

impl<'c> Executor<'c> for &'c mut ScyllaDBConnection {
    type Database = ScyllaDB;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<ScyllaDBQueryResult, ScyllaDBRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, ScyllaDB>,
    {
        let sql = query.sql();
        let mut query = query;
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();

        Box::pin(try_stream! {
            let arguments = arguments?;
            let mut s = pin!(self.run(sql, arguments, persistent).await?);

            while let Some(v) = s.try_next().await? {
                r#yield!(v);
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<ScyllaDBRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        let mut s = self.fetch_many(query);

        Box::pin(async move {
            while let Some(v) = s.try_next().await? {
                if let Either::Right(r) = v {
                    return Ok(Some(r));
                }
            }

            Ok(None)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        _parameters: &'e [ScyllaDBTypeInfo],
    ) -> BoxFuture<'e, Result<ScyllaDBStatement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let statement = Statement::new(sql).with_page_size(self.page_size);
            let prepared_statement = self
                .caching_session
                .add_prepared_statement(&statement)
                .await
                .map_err(ScyllaDBError::PrepareError)?;

            let column_specs = prepared_statement.get_result_set_col_specs();
            let metadata = ScyllaDBStatementMetadata::from_column_specs(column_specs)?;

            let is_affect_statement = column_specs.is_affect_statement();

            Ok(ScyllaDBStatement {
                sql: Cow::Borrowed(sql),
                prepared_statement,
                metadata,
                is_affect_statement,
            })
        })
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let statement = Statement::new(sql);
            let prepared_statement = self
                .caching_session
                .add_prepared_statement(&statement)
                .await
                .map_err(ScyllaDBError::PrepareError)?;
            let column_specs = prepared_statement.get_result_set_col_specs();

            let capacity = column_specs.len();
            let mut columns = Vec::with_capacity(capacity);
            let mut parameters = Vec::with_capacity(capacity);
            let mut nullable = Vec::with_capacity(capacity);
            for (i, column_spec) in column_specs.iter().enumerate() {
                let name = UStr::new(column_spec.name());
                let column_type = column_spec.typ();
                let type_info = ScyllaDBTypeInfo::from_column_type(column_type)?;

                columns.push(ScyllaDBColumn {
                    ordinal: i,
                    name,
                    type_info: type_info.clone(),
                    column_type: column_type.clone().into_owned(),
                });
                parameters.push(type_info);
                nullable.push(Some(true));
            }

            let describe = Describe::<ScyllaDB> {
                columns,
                parameters: Some(Either::Left(parameters)),
                nullable,
            };

            Ok(describe)
        })
    }
}

trait ColumnSpecsExt {
    fn is_affect_statement(&self) -> bool;
    fn is_lwt(&self) -> bool;
}

impl ColumnSpecsExt for ColumnSpecs<'_, '_> {
    #[inline]
    fn is_affect_statement(&self) -> bool {
        // Returns 0 for queries other than SELECT queries.
        if self.len() == 0 {
            return true;
        }

        if self.is_lwt() {
            return true;
        }

        return false;
    }

    #[inline]
    fn is_lwt(&self) -> bool {
        // In the case of a lightweight transaction, [applied] is returned.
        if let Some(column_spec) = self.get_by_index(0) {
            if column_spec.name() == APPLIED_COLUMN {
                return true;
            }
        }

        false
    }
}
