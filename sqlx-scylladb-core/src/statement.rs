use std::{borrow::Cow, sync::Arc};

use scylla::{response::query_result::ColumnSpecs, statement::prepared::PreparedStatement};
use sqlx::{ColumnIndex, Error, Statement};
use sqlx_core::{HashMap, ext::ustr::UStr, impl_statement_query};

use crate::{ScyllaDB, ScyllaDBArguments, ScyllaDBColumn, ScyllaDBError, ScyllaDBTypeInfo};

/// Implementation of [sqlx::Statement] for ScyllaDB.
#[derive(Clone)]
pub struct ScyllaDBStatement<'q> {
    pub(crate) sql: Cow<'q, str>,
    pub(crate) prepared_statement: PreparedStatement,
    pub(crate) metadata: ScyllaDBStatementMetadata,
    pub(crate) is_affect_statement: bool,
}

impl<'q> Statement<'q> for ScyllaDBStatement<'q> {
    type Database = ScyllaDB;

    fn to_owned(&self) -> ScyllaDBStatement<'static> {
        ScyllaDBStatement::<'static> {
            sql: Cow::Owned(self.sql.clone().into_owned()),
            prepared_statement: self.prepared_statement.clone(),
            metadata: self.metadata.clone(),
            is_affect_statement: self.is_affect_statement,
        }
    }

    fn sql(&self) -> &str {
        &self.sql
    }

    fn parameters(&self) -> Option<sqlx::Either<&[ScyllaDBTypeInfo], usize>> {
        Some(sqlx::Either::Right(self.metadata.parameters))
    }

    fn columns(&self) -> &[ScyllaDBColumn] {
        &self.metadata.columns
    }

    impl_statement_query!(ScyllaDBArguments);
}

impl ColumnIndex<ScyllaDBStatement<'_>> for &'_ str {
    fn index(&self, statement: &ScyllaDBStatement<'_>) -> Result<usize, Error> {
        statement
            .metadata
            .column_names
            .get(*self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
            .copied()
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ScyllaDBStatementMetadata {
    pub(crate) columns: Arc<Vec<ScyllaDBColumn>>,
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
    pub(crate) parameters: usize,
}

impl ScyllaDBStatementMetadata {
    pub(crate) fn from_column_specs(column_specs: ColumnSpecs) -> Result<Self, ScyllaDBError> {
        let parameters = column_specs.len();
        let mut columns = Vec::with_capacity(parameters);
        let mut column_names = HashMap::with_capacity(parameters);
        for (i, column_spec) in column_specs.iter().enumerate() {
            let name = UStr::new(column_spec.name());
            let column_type = column_spec.typ();
            let type_info = ScyllaDBTypeInfo::from_column_type(column_type)?;

            column_names.insert(name.clone(), i);
            columns.push(ScyllaDBColumn {
                ordinal: i,
                name,
                type_info,
                column_type: column_type.clone().into_owned(),
            })
        }

        let columns = Arc::new(columns);
        let column_names = Arc::new(column_names);

        let metadata = ScyllaDBStatementMetadata {
            columns,
            column_names,
            parameters,
        };

        Ok(metadata)
    }
}
