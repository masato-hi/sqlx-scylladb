use std::fmt::Display;

use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};
use sqlx_core::{ext::ustr::UStr, type_info::TypeInfo};

use crate::ScyllaDBError;

/// A native ScyllaDB type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScyllaDBTypeInfoNative {
    /// `ascii` type.
    Ascii,
    /// `boolean` type.
    Boolean,
    /// `blob` type.
    Blob,
    /// `counter` type.
    Counter,
    /// `decimal` type.
    Decimal,
    /// `date` type.
    Date,
    /// `double` type.
    Double,
    /// `duration` type.
    Duration,
    /// `float` type.
    Float,
    /// `int` type.
    Int,
    /// `bigint` type.
    BigInt,
    /// `text` type.
    Text,
    /// `timestamp` type.
    Timestamp,
    /// `inet` type.
    Inet,
    /// `smallint` type.
    SmallInt,
    /// `tinyint` type.
    TinyInt,
    /// `time` type.
    Time,
    /// `timeuuid` type.
    Timeuuid,
    /// `uuid` type.
    Uuid,
    /// `variant` type.
    Variant,
}

impl ScyllaDBTypeInfoNative {
    /// Returns the ScyllaDB type name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Ascii => "ASCII",
            Self::Boolean => "BOOLEAN",
            Self::Blob => "BLOB",
            Self::Counter => "COUNTER",
            Self::Decimal => "DECIMAL",
            Self::Date => "DATE",
            Self::Double => "DOUBLE",
            Self::Duration => "DURATION",
            Self::Float => "FLOAT",
            Self::Int => "INT",
            Self::BigInt => "BIGINT",
            Self::Text => "TEXT",
            Self::Timestamp => "TIMESTAMP",
            Self::Inet => "INET",
            Self::SmallInt => "SMALLINT",
            Self::TinyInt => "TINYINT",
            Self::Time => "TIME",
            Self::Timeuuid => "TIMEUUID",
            Self::Uuid => "UUID",
            Self::Variant => "VARIANT",
        }
    }
}

/// An array of a native ScyllaDB type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScyllaDBTypeInfoNativeArray {
    /// array of `ascii` type.
    Ascii,
    /// array of `boolean` type.
    Boolean,
    /// array of `blob` type.
    Blob,
    /// array of `decimal` type.
    Decimal,
    /// array of `date` type.
    Date,
    /// array of `double` type.
    Double,
    /// array of `duration` type.
    Duration,
    /// array of `float` type.
    Float,
    /// array of `int` type.
    Int,
    /// array of `bigint` type.
    BigInt,
    /// array of `text` type.
    Text,
    /// array of `timestamp` type.
    Timestamp,
    /// array of `inet` type.
    Inet,
    /// array of `smallint` type.
    SmallInt,
    /// array of `tinyint` type.
    TinyInt,
    /// array of `time` type.
    Time,
    /// array of `timeuuid` type.
    Timeuuid,
    /// array of `uuid` type.
    Uuid,
}

impl ScyllaDBTypeInfoNativeArray {
    /// Returns the ScyllaDB array type name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Ascii => "ASCII[]",
            Self::Boolean => "BOOLEAN[]",
            Self::Blob => "BLOB[]",
            Self::Decimal => "DECIMAL[]",
            Self::Date => "DATE[]",
            Self::Double => "DOUBLE[]",
            Self::Duration => "DURATION[]",
            Self::Float => "FLOAT[]",
            Self::Int => "INT[]",
            Self::BigInt => "BIGINT[]",
            Self::Text => "TEXT[]",
            Self::Timestamp => "TIMESTAMP[]",
            Self::Inet => "INET[]",
            Self::SmallInt => "SMALLINT[]",
            Self::TinyInt => "TINYINT[]",
            Self::Time => "TIME[]",
            Self::Timeuuid => "TIMEUUID[]",
            Self::Uuid => "UUID[]",
        }
    }
}

/// The enum for the supported type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScyllaDBTypeInfo {
    /// A native type.
    Native(ScyllaDBTypeInfoNative),
    /// An array of a native type.
    NativeArray(ScyllaDBTypeInfoNativeArray),
    /// NULL type.
    Null,
    /// Unset type.
    Unset,
    /// Any tuple type.
    Map(UStr),
    /// Any tuple type.
    Tuple(UStr),
    /// user-defined type.
    UserDefinedType(UStr),
    /// array of user-defined type.
    UserDefinedTypeArray(UStr),
}

impl TypeInfo for ScyllaDBTypeInfo {
    fn is_null(&self) -> bool {
        *self == Self::Null
    }

    fn name(&self) -> &str {
        match self {
            Self::Native(native) => native.name(),
            Self::NativeArray(native_array) => native_array.name(),
            Self::Null => "NULL",
            Self::Unset => "UNSET",
            Self::Map(name) => name,
            Self::Tuple(name) => name,
            Self::UserDefinedType(name) => name,
            Self::UserDefinedTypeArray(name) => name,
        }
    }

    fn type_compatible(&self, other: &Self) -> bool
    where
        Self: Sized,
    {
        match self {
            Self::Native(ScyllaDBTypeInfoNative::Ascii)
            | Self::Native(ScyllaDBTypeInfoNative::Text) => {
                *other == Self::Native(ScyllaDBTypeInfoNative::Ascii)
                    || *other == Self::Native(ScyllaDBTypeInfoNative::Text)
            }
            Self::NativeArray(ScyllaDBTypeInfoNativeArray::Ascii)
            | Self::NativeArray(ScyllaDBTypeInfoNativeArray::Text) => {
                *other == Self::NativeArray(ScyllaDBTypeInfoNativeArray::Ascii)
                    || *other == Self::NativeArray(ScyllaDBTypeInfoNativeArray::Text)
            }
            Self::Native(ScyllaDBTypeInfoNative::BigInt) => {
                *other == Self::Native(ScyllaDBTypeInfoNative::Counter)
                    || *other == Self::Native(ScyllaDBTypeInfoNative::BigInt)
            }
            Self::Native(ScyllaDBTypeInfoNative::Uuid) => {
                *other == Self::Native(ScyllaDBTypeInfoNative::Uuid)
                    || *other == Self::Native(ScyllaDBTypeInfoNative::Timeuuid)
            }
            Self::NativeArray(ScyllaDBTypeInfoNativeArray::Uuid) => {
                *other == Self::NativeArray(ScyllaDBTypeInfoNativeArray::Uuid)
                    || *other == Self::NativeArray(ScyllaDBTypeInfoNativeArray::Timeuuid)
            }
            Self::Map(name) => matches!(other, Self::Map(other_name) if name == other_name),
            Self::Tuple(typ) => {
                if let Self::Tuple(other_typ) = other {
                    typ.replace("ASCII", "TEXT") == other_typ.replace("ASCII", "TEXT")
                } else {
                    self == other
                }
            }
            _ => self == other,
        }
    }
}

macro_rules! column_type_not_supported {
    ($column_type:ident) => {{
        return Err($crate::ScyllaDBError::ColumnTypeNotSupportedError(
            $column_type.clone().into_owned(),
        ));
    }};
}

impl TryFrom<&ColumnType<'_>> for ScyllaDBTypeInfo {
    type Error = ScyllaDBError;

    fn try_from(value: &ColumnType) -> Result<Self, Self::Error> {
        Self::from_column_type(value)
    }
}

impl ScyllaDBTypeInfo {
    pub(crate) fn from_column_type(column_type: &ColumnType) -> Result<Self, ScyllaDBError> {
        let type_info = match column_type {
            ColumnType::Native(native_type) => match native_type {
                NativeType::Ascii => Self::Native(ScyllaDBTypeInfoNative::Ascii),
                NativeType::Boolean => Self::Native(ScyllaDBTypeInfoNative::Boolean),
                NativeType::Blob => Self::Native(ScyllaDBTypeInfoNative::Blob),
                NativeType::Counter => Self::Native(ScyllaDBTypeInfoNative::Counter),
                NativeType::Date => Self::Native(ScyllaDBTypeInfoNative::Date),
                NativeType::Decimal => Self::Native(ScyllaDBTypeInfoNative::Decimal),
                NativeType::Double => Self::Native(ScyllaDBTypeInfoNative::Double),
                NativeType::Duration => Self::Native(ScyllaDBTypeInfoNative::Duration),
                NativeType::Float => Self::Native(ScyllaDBTypeInfoNative::Float),
                NativeType::Int => Self::Native(ScyllaDBTypeInfoNative::Int),
                NativeType::BigInt => Self::Native(ScyllaDBTypeInfoNative::BigInt),
                NativeType::Text => Self::Native(ScyllaDBTypeInfoNative::Text),
                NativeType::Timestamp => Self::Native(ScyllaDBTypeInfoNative::Timestamp),
                NativeType::Inet => Self::Native(ScyllaDBTypeInfoNative::Inet),
                NativeType::SmallInt => Self::Native(ScyllaDBTypeInfoNative::SmallInt),
                NativeType::TinyInt => Self::Native(ScyllaDBTypeInfoNative::TinyInt),
                NativeType::Time => Self::Native(ScyllaDBTypeInfoNative::Time),
                NativeType::Timeuuid => Self::Native(ScyllaDBTypeInfoNative::Timeuuid),
                NativeType::Uuid => Self::Native(ScyllaDBTypeInfoNative::Uuid),
                NativeType::Varint => Self::Native(ScyllaDBTypeInfoNative::Variant),
                _ => column_type_not_supported!(column_type),
            },
            ColumnType::Collection { frozen: _, typ } => match typ {
                CollectionType::List(inner) | CollectionType::Set(inner) => match &**inner {
                    ColumnType::Native(native_type) => match native_type {
                        NativeType::Ascii => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Ascii),
                        NativeType::Boolean => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::Boolean)
                        }
                        NativeType::Blob => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Blob),
                        NativeType::Counter => column_type_not_supported!(column_type),
                        NativeType::Date => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Date),
                        NativeType::Decimal => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::Decimal)
                        }
                        NativeType::Double => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::Double)
                        }
                        NativeType::Duration => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::Duration)
                        }
                        NativeType::Float => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Float),
                        NativeType::Int => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Int),
                        NativeType::BigInt => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::BigInt)
                        }
                        NativeType::Text => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Text),
                        NativeType::Timestamp => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::Timestamp)
                        }
                        NativeType::Inet => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Inet),
                        NativeType::SmallInt => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::SmallInt)
                        }
                        NativeType::TinyInt => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::TinyInt)
                        }
                        NativeType::Time => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Time),
                        NativeType::Timeuuid => {
                            Self::NativeArray(ScyllaDBTypeInfoNativeArray::Timeuuid)
                        }
                        NativeType::Uuid => Self::NativeArray(ScyllaDBTypeInfoNativeArray::Uuid),
                        NativeType::Varint => column_type_not_supported!(column_type),
                        _ => column_type_not_supported!(column_type),
                    },
                    ColumnType::UserDefinedType {
                        frozen: _,
                        definition,
                    } => {
                        let type_name = format!("{}[]", definition.name);
                        let type_name = UStr::new(&type_name);
                        Self::UserDefinedTypeArray(type_name)
                    }
                    _ => column_type_not_supported!(column_type),
                },
                CollectionType::Map(key_type, value_type) => {
                    let key_type_info = Self::from_column_type(key_type)?;
                    let value_type_info = Self::from_column_type(value_type)?;
                    let type_info_name =
                        format!("<{},{}>", key_type_info.name(), value_type_info.name());
                    Self::Map(UStr::from(type_info_name))
                }
                _ => column_type_not_supported!(column_type),
            },
            ColumnType::UserDefinedType {
                frozen: _,
                definition,
            } => {
                let type_name = UStr::new(&definition.name);
                Self::UserDefinedType(type_name)
            }
            ColumnType::Tuple(items) => Self::tuple_type_info_name_from_column_types(items)?,
            _ => column_type_not_supported!(column_type),
        };

        Ok(type_info)
    }
}

impl Display for ScyllaDBTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}
