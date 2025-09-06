use bytes::Bytes;
use sqlx::{ColumnIndex, Error, Row};

use crate::{ScyllaDB, ScyllaDBColumn, ScyllaDBValueRef, statement::ScyllaDBStatementMetadata};

#[derive(Debug)]
pub struct ScyllaDBRow {
    raw_columns: Vec<Option<Bytes>>,
    metadata: ScyllaDBStatementMetadata,
}

impl ScyllaDBRow {
    #[inline(always)]
    pub(crate) fn new(
        raw_columns: Vec<Option<Bytes>>,
        metadata: ScyllaDBStatementMetadata,
    ) -> Self {
        Self {
            raw_columns,
            metadata,
        }
    }

    #[cfg(feature = "any")]
    #[inline(always)]
    pub(crate) fn column_names(
        &self,
    ) -> std::sync::Arc<sqlx_core::HashMap<sqlx_core::ext::ustr::UStr, usize>> {
        self.metadata.column_names.clone()
    }
}

impl Row for ScyllaDBRow {
    type Database = ScyllaDB;

    fn columns(&self) -> &[ScyllaDBColumn] {
        &self.metadata.columns
    }

    fn try_get_raw<I>(&self, index: I) -> Result<ScyllaDBValueRef<'_>, sqlx::Error>
    where
        I: sqlx::ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        let column_metadata =
            self.metadata
                .columns
                .get(index)
                .ok_or_else(|| Error::ColumnIndexOutOfBounds {
                    index: index,
                    len: self.metadata.columns.len(),
                })?;
        let column_name = column_metadata.name.clone();
        let column_type = &column_metadata.column_type;
        let type_info = column_metadata.type_info.clone();
        if let Some(column) = self.raw_columns.get(index) {
            if let Some(value) = column {
                Ok(ScyllaDBValueRef::new(
                    column_name,
                    type_info,
                    value,
                    column_type,
                ))
            } else {
                Ok(ScyllaDBValueRef::null(column_name, column_type))
            }
        } else {
            Ok(ScyllaDBValueRef::null(column_name, column_type))
        }
    }
}

impl ColumnIndex<ScyllaDBRow> for &'_ str {
    fn index(&self, row: &ScyllaDBRow) -> Result<usize, sqlx::Error> {
        row.metadata
            .column_names
            .get(*self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
            .copied()
    }
}
