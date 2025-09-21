use scylla::cluster::metadata::ColumnType;
use sqlx::Column;
use sqlx_core::ext::ustr::UStr;

use crate::{ScyllaDB, ScyllaDBTypeInfo};

/// Implementation of [sqlx::Column] for ScyllaDB.
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
