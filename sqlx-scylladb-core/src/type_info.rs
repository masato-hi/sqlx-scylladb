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
#[derive(Debug, Clone, PartialEq, Hash)]
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
    Tuple(UStr),
    /// user-defined type.
    UserDefinedType(UStr),
    /// array of user-defined type.
    UserDefinedTypeArray(UStr),
    /// map type of `ascii` and `ascii`.
    AsciiAsciiMap,
    /// map type of `ascii` and `text`.
    AsciiTextMap,
    /// map type of `ascii` and `boolean`.
    AsciiBooleanMap,
    /// map type of `ascii` and `tinyint`.
    AsciiTinyIntMap,
    /// map type of `ascii` and `smallint`.
    AsciiSmallIntMap,
    /// map type of `ascii` and `int`.
    AsciiIntMap,
    /// map type of `ascii` and `bigint`.
    AsciiBigIntMap,
    /// map type of `ascii` and `float`.
    AsciiFloatMap,
    /// map type of `ascii` and `double`.
    AsciiDoubleMap,
    /// map type of `ascii` and `uuid`.
    AsciiUuidMap,
    /// map type of `ascii` and `timeuuid`.
    AsciiTimeuuidMap,
    /// map type of `ascii` and `inet`.
    AsciiInetMap,
    /// map type of `text` and `ascii`.
    TextAsciiMap,
    /// map type of `text` and `text`.
    TextTextMap,
    /// map type of `text` and `boolean`.
    TextBooleanMap,
    /// map type of `text` and `tinyint`.
    TextTinyIntMap,
    /// map type of `text` and `smallint`.
    TextSmallIntMap,
    /// map type of `text` and `int`.
    TextIntMap,
    /// map type of `text` and `bigint`.
    TextBigIntMap,
    /// map type of `text` and `float`.
    TextFloatMap,
    /// map type of `text` and `double`.
    TextDoubleMap,
    /// map type of `text` and `uuid`.
    TextUuidMap,
    /// map type of `text` and `timeuuid`.
    TextTimeuuidMap,
    /// map type of `text` and `inet`.
    TextInetMap,
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
            Self::Tuple(name) => name,
            Self::UserDefinedType(name) => name,
            Self::UserDefinedTypeArray(name) => name,
            Self::AsciiAsciiMap => "MAP<ASCII, ASCII>",
            Self::AsciiTextMap => "MAP<ASCII, TEXT>",
            Self::AsciiBooleanMap => "MAP<ASCII, BOOLEAN>",
            Self::AsciiTinyIntMap => "MAP<ASCII, TINYINT>",
            Self::AsciiSmallIntMap => "MAP<ASCII, SMALLINT>",
            Self::AsciiIntMap => "MAP<ASCII, INT>",
            Self::AsciiBigIntMap => "MAP<ASCII, BIGINT>",
            Self::AsciiFloatMap => "MAP<ASCII, FLOAT>",
            Self::AsciiDoubleMap => "MAP<ASCII, DOUBLE>",
            Self::AsciiUuidMap => "MAP<ASCII, UUID>",
            Self::AsciiTimeuuidMap => "MAP<ASCII, TIMEUUID>",
            Self::AsciiInetMap => "MAP<ASCII, INET>",
            Self::TextAsciiMap => "MAP<TEXT, ASCII>",
            Self::TextTextMap => "MAP<TEXT, TEXT>",
            Self::TextBooleanMap => "MAP<TEXT, BOOLEAN>",
            Self::TextTinyIntMap => "MAP<TEXT, TINYINT>",
            Self::TextSmallIntMap => "MAP<TEXT, SMALLINT>",
            Self::TextIntMap => "MAP<TEXT, INT>",
            Self::TextBigIntMap => "MAP<TEXT, BIGINT>",
            Self::TextFloatMap => "MAP<TEXT, FLOAT>",
            Self::TextDoubleMap => "MAP<TEXT, DOUBLE>",
            Self::TextUuidMap => "MAP<TEXT, UUID>",
            Self::TextTimeuuidMap => "MAP<TEXT, TIMEUUID>",
            Self::TextInetMap => "MAP<TEXT, INET>",
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
            Self::AsciiAsciiMap | Self::AsciiTextMap | Self::TextTextMap | Self::TextAsciiMap => {
                *other == Self::AsciiAsciiMap
                    || *other == Self::AsciiTextMap
                    || *other == Self::TextTextMap
                    || *other == Self::TextAsciiMap
            }
            Self::AsciiBooleanMap | Self::TextBooleanMap => {
                *other == Self::AsciiBooleanMap || *other == Self::TextBooleanMap
            }
            Self::AsciiTinyIntMap | Self::TextTinyIntMap => {
                *other == Self::AsciiTinyIntMap || *other == Self::TextTinyIntMap
            }
            Self::AsciiSmallIntMap | Self::TextSmallIntMap => {
                *other == Self::AsciiSmallIntMap || *other == Self::TextSmallIntMap
            }
            Self::AsciiIntMap | Self::TextIntMap => {
                *other == Self::AsciiIntMap || *other == Self::TextIntMap
            }
            Self::AsciiBigIntMap | Self::TextBigIntMap => {
                *other == Self::AsciiBigIntMap || *other == Self::TextBigIntMap
            }
            Self::AsciiFloatMap | Self::TextFloatMap => {
                *other == Self::AsciiFloatMap || *other == Self::TextFloatMap
            }
            Self::AsciiDoubleMap | Self::TextDoubleMap => {
                *other == Self::AsciiDoubleMap || *other == Self::TextDoubleMap
            }
            Self::AsciiUuidMap
            | Self::TextUuidMap
            | Self::AsciiTimeuuidMap
            | Self::TextTimeuuidMap => {
                *other == Self::AsciiUuidMap
                    || *other == Self::TextUuidMap
                    || *other == Self::AsciiTimeuuidMap
                    || *other == Self::TextTimeuuidMap
            }
            Self::AsciiInetMap | Self::TextInetMap => {
                *other == Self::AsciiInetMap || *other == Self::TextInetMap
            }
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
                CollectionType::Map(key_type, value_type) => match &**key_type {
                    ColumnType::Native(key_native_type) => match key_native_type {
                        NativeType::Ascii => match &**value_type {
                            ColumnType::Native(value_native_type) => match value_native_type {
                                NativeType::Ascii => Self::AsciiAsciiMap,
                                NativeType::Text => Self::AsciiTextMap,
                                NativeType::Boolean => Self::AsciiBooleanMap,
                                NativeType::Double => Self::AsciiDoubleMap,
                                NativeType::Float => Self::AsciiFloatMap,
                                NativeType::Int => Self::AsciiIntMap,
                                NativeType::BigInt => Self::AsciiBigIntMap,
                                NativeType::SmallInt => Self::AsciiSmallIntMap,
                                NativeType::TinyInt => Self::AsciiTinyIntMap,
                                NativeType::Timeuuid => Self::AsciiTimeuuidMap,
                                NativeType::Uuid => Self::AsciiUuidMap,
                                NativeType::Inet => Self::AsciiInetMap,
                                _ => column_type_not_supported!(column_type),
                            },
                            _ => column_type_not_supported!(column_type),
                        },
                        NativeType::Text => match &**value_type {
                            ColumnType::Native(value_native_type) => match value_native_type {
                                NativeType::Ascii => Self::TextAsciiMap,
                                NativeType::Text => Self::TextTextMap,
                                NativeType::Boolean => Self::TextBooleanMap,
                                NativeType::Double => Self::TextDoubleMap,
                                NativeType::Float => Self::TextFloatMap,
                                NativeType::Int => Self::TextIntMap,
                                NativeType::BigInt => Self::TextBigIntMap,
                                NativeType::SmallInt => Self::TextSmallIntMap,
                                NativeType::TinyInt => Self::TextTinyIntMap,
                                NativeType::Timeuuid => Self::TextTimeuuidMap,
                                NativeType::Uuid => Self::TextUuidMap,
                                NativeType::Inet => Self::TextInetMap,
                                _ => column_type_not_supported!(column_type),
                            },
                            _ => column_type_not_supported!(column_type),
                        },
                        _ => column_type_not_supported!(column_type),
                    },
                    _ => column_type_not_supported!(column_type),
                },
                _ => column_type_not_supported!(column_type),
            },
            ColumnType::UserDefinedType {
                frozen: _,
                definition,
            } => {
                let type_name = UStr::new(&definition.name);
                Self::UserDefinedType(type_name)
            }
            ColumnType::Tuple(items) => {
                let mut type_infos = Vec::with_capacity(items.capacity());
                for item in items {
                    let type_info = Self::from_column_type(item)?;
                    type_infos.push(type_info);
                }
                let type_name = tuple_type_name(&type_infos);
                Self::Tuple(type_name)
            }
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

pub(crate) fn tuple_type_name(type_infos: &[ScyllaDBTypeInfo]) -> UStr {
    let mut type_name = String::from("TUPLE<");
    for (i, type_info) in type_infos.iter().enumerate() {
        if i > 0 {
            type_name.push_str(", ");
        }
        let name = type_info.name();
        type_name.push_str(name);
    }
    type_name.push_str(">");
    UStr::new(&type_name)
}
