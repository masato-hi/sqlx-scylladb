use std::sync::Arc;

use scylla::response::query_result::ColumnSpecs;
use sqlx_core::{
    Error, HashMap, column::ColumnIndex, ext::ustr::UStr, impl_statement_query, sql_str::SqlStr,
    statement::Statement,
};

use crate::{ScyllaDB, ScyllaDBArguments, ScyllaDBColumn, ScyllaDBError, ScyllaDBTypeInfo};

/// Implementation of [sqlx::Statement] for ScyllaDB.
#[derive(Clone)]
pub struct ScyllaDBStatement {
    pub(crate) sql: SqlStr,
    pub(crate) metadata: ScyllaDBStatementMetadata,
    pub(crate) is_affect_statement: bool,
}

impl Statement for ScyllaDBStatement {
    type Database = ScyllaDB;

    fn into_sql(self) -> SqlStr {
        self.sql
    }

    fn sql(&self) -> &SqlStr {
        &self.sql
    }

    fn parameters(&self) -> Option<sqlx_core::Either<&[ScyllaDBTypeInfo], usize>> {
        Some(sqlx_core::Either::Right(self.metadata.parameters))
    }

    fn columns(&self) -> &[ScyllaDBColumn] {
        &self.metadata.columns
    }

    impl_statement_query!(ScyllaDBArguments);
}

impl ColumnIndex<ScyllaDBStatement> for &'_ str {
    fn index(&self, statement: &ScyllaDBStatement) -> Result<usize, Error> {
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
