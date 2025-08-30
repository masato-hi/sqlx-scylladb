use std::{borrow::Cow, sync::LazyLock};

use bytes::Bytes;
use scylla::{
    cluster::metadata::ColumnType,
    deserialize::{FrameSlice, value::DeserializeValue},
};
use sqlx::{Value, ValueRef};
use sqlx_core::ext::ustr::UStr;

use crate::{ScyllaDB, ScyllaDBError, ScyllaDBTypeInfo};

#[derive(Debug, Clone)]
pub struct ScyllaDBValue {
    column_name: UStr,
    raw_value: Bytes,
    column_type: ColumnType<'static>,
    type_info: ScyllaDBTypeInfo,
}

impl Value for ScyllaDBValue {
    type Database = ScyllaDB;

    fn as_ref(&self) -> ScyllaDBValueRef<'_> {
        ScyllaDBValueRef {
            column_name: self.column_name.clone(),
            raw_value: &self.raw_value,
            column_type: &self.column_type,
            type_info: self.type_info.clone(),
        }
    }

    fn type_info(&self) -> Cow<'_, ScyllaDBTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    fn is_null(&self) -> bool {
        self.type_info == ScyllaDBTypeInfo::Null
    }
}

#[derive(Debug, Clone)]
pub struct ScyllaDBValueRef<'r> {
    column_name: UStr,
    raw_value: &'r Bytes,
    column_type: &'r ColumnType<'r>,
    type_info: ScyllaDBTypeInfo,
}

impl<'r> ScyllaDBValueRef<'r> {
    #[inline(always)]
    pub(crate) fn new(
        column_name: UStr,
        type_info: ScyllaDBTypeInfo,
        raw_value: &'r Bytes,
        column_type: &'r ColumnType<'r>,
    ) -> ScyllaDBValueRef<'r> {
        Self {
            column_name,
            raw_value,
            column_type,
            type_info,
        }
    }

    #[inline(always)]
    pub(crate) fn null(column_name: UStr, column_type: &'r ColumnType<'r>) -> Self {
        static EMPTY: LazyLock<Bytes> = LazyLock::new(|| Bytes::new());
        Self {
            column_name,
            raw_value: &EMPTY,
            column_type,
            type_info: ScyllaDBTypeInfo::Null,
        }
    }

    #[inline(always)]
    pub fn column_name(&self) -> UStr {
        self.column_name.clone()
    }

    #[inline(always)]
    pub fn column_type(&self) -> ColumnType<'static> {
        self.column_type.clone().into_owned()
    }

    #[inline(always)]
    pub fn deserialize<T>(&self) -> Result<T, ScyllaDBError>
    where
        T: DeserializeValue<'r, 'r>,
    {
        let frame_slice = FrameSlice::new(self.raw_value);
        let val = <_ as DeserializeValue>::deserialize(self.column_type, Some(frame_slice))?;
        Ok(val)
    }
}

impl<'r> ValueRef<'r> for ScyllaDBValueRef<'r> {
    type Database = ScyllaDB;

    fn to_owned(&self) -> ScyllaDBValue {
        ScyllaDBValue {
            column_name: self.column_name.clone(),
            column_type: self.column_type.clone().into_owned(),
            raw_value: self.raw_value.clone(),
            type_info: self.type_info.clone(),
        }
    }

    fn type_info(&self) -> Cow<'_, ScyllaDBTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    fn is_null(&self) -> bool {
        self.type_info == ScyllaDBTypeInfo::Null
    }
}

// #[derive(Debug, Clone)]
// pub struct ScyllaDBValue {
//     column_name: UStr,
//     cql_value: Option<CqlValue>,
//     type_info: ScyllaDBTypeInfo,
// }

// impl Value for ScyllaDBValue {
//     type Database = ScyllaDB;

//     fn as_ref(&self) -> ScyllaDBValueRef<'_> {
//         ScyllaDBValueRef {
//             column_name: self.column_name.clone(),
//             cql_value: self.cql_value.as_ref(),
//             type_info: self.type_info,
//         }
//     }

//     fn type_info(&self) -> Cow<'_, ScyllaDBTypeInfo> {
//         Cow::Borrowed(&self.type_info)
//     }

//     fn is_null(&self) -> bool {
//         self.cql_value.is_none()
//     }
// }

// impl ScyllaDBValue {
//     pub fn cql_value(&self) -> Option<CqlValue> {
//         self.cql_value.clone()
//     }

//     pub fn cql_value_ref<'r>(&'r self) -> Option<&'r CqlValue> {
//         self.cql_value.as_ref()
//     }
// }

// #[derive(Debug, Clone)]
// pub struct ScyllaDBValueRef<'r> {
//     column_name: UStr,
//     cql_value: Option<&'r CqlValue>,
//     type_info: ScyllaDBTypeInfo,
// }

// impl<'r> ScyllaDBValueRef<'r> {
//     #[inline(always)]
//     pub(crate) fn new(
//         column_name: UStr,
//         type_info: ScyllaDBTypeInfo,
//         cql_value: &'r CqlValue,
//     ) -> ScyllaDBValueRef<'r> {
//         Self {
//             column_name,
//             cql_value: Some(cql_value),
//             type_info,
//         }
//     }

//     #[inline(always)]
//     pub(crate) fn null(column_name: UStr) -> Self {
//         Self {
//             column_name,
//             cql_value: None,
//             type_info: ScyllaDBTypeInfo::Null,
//         }
//     }

//     #[inline(always)]
//     pub fn column_name(&self) -> UStr {
//         self.column_name.clone()
//     }

//     #[inline(always)]
//     pub fn cql_value_ref(&self) -> Option<&'r CqlValue> {
//         self.cql_value
//     }
// }

// impl<'r> ValueRef<'r> for ScyllaDBValueRef<'r> {
//     type Database = ScyllaDB;

//     fn to_owned(&self) -> ScyllaDBValue {
//         ScyllaDBValue {
//             column_name: self.column_name.clone(),
//             cql_value: self.cql_value.cloned(),
//             type_info: self.type_info,
//         }
//     }

//     fn type_info(&self) -> Cow<'_, ScyllaDBTypeInfo> {
//         Cow::Borrowed(&self.type_info)
//     }

//     fn is_null(&self) -> bool {
//         self.cql_value.is_none()
//     }
// }
