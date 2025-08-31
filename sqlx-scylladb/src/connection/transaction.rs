use std::{borrow::Cow, fmt::Debug};

use scylla::{
    cluster::metadata::ColumnType,
    errors::SerializationError,
    serialize::row::{RowSerializationContext, SerializeRow},
    statement::{Statement, batch::Batch, prepared::PreparedStatement},
};
use scylla_cql::{frame::types::RawValue, serialize::row::SerializedValues};

use crate::{ScyllaDBArguments, ScyllaDBConnection, ScyllaDBError};

#[derive(Debug)]
pub(crate) struct ScyllaDBTransaction {
    prepared_statements: Vec<PreparedStatement>,
    transactional_values: Vec<ScyllaDBTransactionalValue>,
}

#[derive(Debug)]
pub(crate) struct ScyllaDBTransactionalValue {
    column_types: Vec<ColumnType<'static>>,
    serialized_values: SerializedValues,
}

impl Default for ScyllaDBTransaction {
    fn default() -> Self {
        Self {
            prepared_statements: Default::default(),
            transactional_values: Default::default(),
        }
    }
}

impl ScyllaDBConnection {
    pub(crate) async fn begin_transaction(
        &mut self,
        statement: Option<Cow<'_, str>>,
    ) -> Result<(), ScyllaDBError> {
        if self.transaction.is_none() {
            self.transaction = Some(ScyllaDBTransaction::default())
        }

        if let Some(statement) = statement {
            self.insert_transactional(&statement, None).await?;
        }

        Ok(())
    }

    pub(crate) async fn commit_transaction(&mut self) -> Result<(), ScyllaDBError> {
        if let Some(transaction) = &self.transaction {
            let mut batch = Batch::default();
            for prepared_statement in &transaction.prepared_statements {
                batch.append_statement(prepared_statement.clone());
            }

            self.caching_session
                .batch(&batch, &transaction.transactional_values)
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

    pub(crate) fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    pub(crate) async fn insert_transactional<'e, 'c: 'e, 'q: 'e, 'r: 'e>(
        &'c mut self,
        sql: &'q str,
        arguments: Option<ScyllaDBArguments<'r>>,
    ) -> Result<(), ScyllaDBError> {
        if let Some(transaction) = &mut self.transaction {
            let statement = Statement::new(sql);
            let prepared_statement = self
                .caching_session
                .add_prepared_statement(&statement)
                .await?;

            let column_specs = prepared_statement.get_variable_col_specs();
            let mut column_types = Vec::with_capacity(column_specs.len());

            for column_spec in column_specs.iter() {
                let column_type = column_spec.typ().clone().into_owned();
                column_types.push(column_type);
            }

            let ctx = RowSerializationContext::from_specs(column_specs.as_slice());
            let serialized_values = if let Some(arguments) = &arguments {
                SerializedValues::from_serializable(&ctx, arguments)?
            } else {
                SerializedValues::new()
            };

            transaction.prepared_statements.push(prepared_statement);
            transaction
                .transactional_values
                .push(ScyllaDBTransactionalValue {
                    column_types,
                    serialized_values,
                });
        } else {
            return Err(ScyllaDBError::TransactionNotStarted);
        }

        Ok(())
    }
}

impl SerializeRow for ScyllaDBTransactionalValue {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut scylla_cql::serialize::RowWriter,
    ) -> Result<(), SerializationError> {
        for (i, column) in ctx.columns().iter().enumerate() {
            let column_type = self.column_types.get(i).ok_or(SerializationError::new(
                ScyllaDBError::ColumnIndexOutOfBounds {
                    index: i,
                    len: self.column_types.len(),
                },
            ))?;

            if column_type != column.typ() {
                return Err(SerializationError::new(ScyllaDBError::ColumnTypeError {
                    expect: column.typ().clone().into_owned(),
                    actual: column_type.clone(),
                }));
            }
        }

        for raw_value in self.serialized_values.iter() {
            let cell_writer = writer.make_cell_writer();
            match raw_value {
                RawValue::Null => cell_writer.set_null(),
                RawValue::Unset => cell_writer.set_unset(),
                RawValue::Value(items) => cell_writer
                    .set_value(items)
                    .map_err(|err| SerializationError::new(err))?,
            };
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.serialized_values.is_empty()
    }
}
