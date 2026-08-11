use scylla::cluster::metadata::ColumnType;
use sqlx_core::{column::Column, ext::ustr::UStr};

use crate::{ScyllaDB, ScyllaDBTypeInfo};

/// A ScyllaDB column exposed through the sqlx row interface.
#[derive(Debug)]
pub struct ScyllaDBColumn {
    pub(crate) ordinal: usize,
    pub(crate) name: UStr,
    pub(crate) type_info: ScyllaDBTypeInfo,
    pub(crate) column_type: ColumnType<'static>,
}

impl Column for ScyllaDBColumn {
    type Database = ScyllaDB;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &ScyllaDBTypeInfo {
        &self.type_info
    }
}
