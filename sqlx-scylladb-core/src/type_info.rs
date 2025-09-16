use std::{
    fmt::Display,
    sync::{LazyLock, RwLock},
};

use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};
use sqlx::TypeInfo;
use sqlx_core::ext::ustr::UStr;

use crate::ScyllaDBError;

#[derive(Debug, Clone, PartialEq)]
pub enum ScyllaDBTypeInfo {
    Any(UStr),
    Ascii,
    AsciiArray,
    Boolean,
    BooleanArray,
    Blob,
    BlobArray,
    Counter,
    Decimal,
    DecimalArray,
    Date,
    DateArray,
    Double,
    DoubleArray,
    Duration,
    DurationArray,
    Null,
    Float,
    FloatArray,
    Int,
    IntArray,
    BigInt,
    BigIntArray,
    Text,
    TextArray,
    Timestamp,
    TimestampArray,
    Inet,
    InetArray,
    SmallInt,
    SmallIntArray,
    TinyInt,
    TinyIntArray,
    Time,
    TimeArray,
    Timeuuid,
    TimeuuidArray,
    Vector,
    Uuid,
    UuidArray,
    Variant,
    Tuple(UStr),
    UserDefinedType(UStr),
    UserDefinedTypeArray(UStr),
    AsciiAsciiMap,
    AsciiTextMap,
    AsciiBooleanMap,
    AsciiTinyIntMap,
    AsciiSmallIntMap,
    AsciiIntMap,
    AsciiBigIntMap,
    AsciiFloatMap,
    AsciiDoubleMap,
    AsciiUuidMap,
    AsciiTimeuuidMap,
    AsciiInetMap,
    TextAsciiMap,
    TextTextMap,
    TextBooleanMap,
    TextTinyIntMap,
    TextSmallIntMap,
    TextIntMap,
    TextBigIntMap,
    TextFloatMap,
    TextDoubleMap,
    TextUuidMap,
    TextTimeuuidMap,
    TextInetMap,
}

impl TypeInfo for ScyllaDBTypeInfo {
    fn is_null(&self) -> bool {
        *self == Self::Null
    }

    fn name(&self) -> &str {
        match self {
            Self::Any(name) => name,
            Self::Ascii => "ASCII",
            Self::AsciiArray => "ASCII[]",
            Self::Text => "TEXT",
            Self::TextArray => "TEXT[]",
            Self::Boolean => "BOOLEAN",
            Self::BooleanArray => "BOOLEAN[]",
            Self::Blob => "BLOB",
            Self::BlobArray => "BLOB[]",
            Self::BigInt => "BIGINT",
            Self::BigIntArray => "BIGINT[]",
            Self::Counter => "COUNTER",
            Self::Decimal => "DECIMAL",
            Self::DecimalArray => "DECIMAL[]",
            Self::Date => "DATE",
            Self::DateArray => "DATE[]",
            Self::Double => "DOUBLE",
            Self::DoubleArray => "DOUBLE[]",
            Self::Duration => "DURATION",
            Self::DurationArray => "DURATION[]",
            Self::Null => "NULL",
            Self::Float => "FLOAT",
            Self::FloatArray => "FLOAT[]",
            Self::Int => "INT",
            Self::IntArray => "INT[]",
            Self::Timestamp => "TIMESTAMP",
            Self::TimestampArray => "TIMESTAMP[]",
            Self::Inet => "INET",
            Self::InetArray => "INET[]",
            Self::Vector => "VECTOR",
            Self::SmallInt => "SMALLINT",
            Self::SmallIntArray => "SMALLINT[]",
            Self::TinyInt => "TINYINT",
            Self::TinyIntArray => "TINYINT[]",
            Self::Time => "TIME",
            Self::TimeArray => "TIME[]",
            Self::Uuid => "UUID",
            Self::UuidArray => "UUID[]",
            Self::Timeuuid => "TIMEUUID",
            Self::TimeuuidArray => "TIMEUUID[]",
            Self::Variant => "VARIANT",
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
            Self::Ascii | Self::Text => *other == Self::Ascii || *other == Self::Text,
            Self::AsciiArray | Self::TextArray => {
                *other == Self::AsciiArray || *other == Self::TextArray
            }
            Self::BigInt => *other == Self::Counter || *other == Self::BigInt,
            Self::Uuid => *other == Self::Uuid || *other == Self::Timeuuid,
            Self::UuidArray => *other == Self::UuidArray || *other == Self::TimeuuidArray,
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

static ANY_TYPES: LazyLock<RwLock<Vec<(ColumnType<'static>, UStr)>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));

pub fn register_any_type(
    column_type: ColumnType<'static>,
    name: UStr,
) -> Result<(), ScyllaDBError> {
    let mut guard = ANY_TYPES
        .write()
        .map_err(|_| ScyllaDBError::ExclusiveLockError)?;
    guard.push((column_type, name));

    Ok(())
}

fn get_any_type(column_type: &ColumnType<'_>) -> Result<Option<ScyllaDBTypeInfo>, ScyllaDBError> {
    let guard = ANY_TYPES
        .read()
        .map_err(|_| ScyllaDBError::ExclusiveLockError)?;
    for (ty, name) in guard.iter() {
        if column_type == ty {
            let type_info = ScyllaDBTypeInfo::Any(name.clone());
            return Ok(Some(type_info));
        }
    }

    Ok(None)
}

macro_rules! column_type_not_supported {
    ($column_type:ident) => {{
        if let Some(type_info) = get_any_type($column_type)? {
            return Ok(type_info);
        }

        return Err(ScyllaDBError::ColumnTypeNotSupportedError(
            $column_type.clone().into_owned(),
        ));
    }};
}

impl ScyllaDBTypeInfo {
    pub(crate) fn from_column_type(column_type: &ColumnType) -> Result<Self, ScyllaDBError> {
        let type_info = match column_type {
            ColumnType::Native(native_type) => match native_type {
                NativeType::Ascii => Self::Ascii,
                NativeType::Boolean => Self::Boolean,
                NativeType::Blob => Self::Blob,
                NativeType::Counter => Self::Counter,
                NativeType::Date => Self::Date,
                NativeType::Decimal => Self::Decimal,
                NativeType::Double => Self::Double,
                NativeType::Duration => Self::Duration,
                NativeType::Float => Self::Float,
                NativeType::Int => Self::Int,
                NativeType::BigInt => Self::BigInt,
                NativeType::Text => Self::Text,
                NativeType::Timestamp => Self::Timestamp,
                NativeType::Inet => Self::Inet,
                NativeType::SmallInt => Self::SmallInt,
                NativeType::TinyInt => Self::TinyInt,
                NativeType::Time => Self::Time,
                NativeType::Timeuuid => Self::Timeuuid,
                NativeType::Uuid => Self::Uuid,
                NativeType::Varint => Self::Variant,
                _ => column_type_not_supported!(column_type),
            },
            ColumnType::Collection { frozen: _, typ } => match typ {
                CollectionType::List(inner) | CollectionType::Set(inner) => match &**inner {
                    ColumnType::Native(native_type) => match native_type {
                        NativeType::Ascii => Self::AsciiArray,
                        NativeType::Boolean => Self::BooleanArray,
                        NativeType::Blob => Self::BlobArray,
                        NativeType::Counter => column_type_not_supported!(column_type),
                        NativeType::Date => Self::DateArray,
                        NativeType::Decimal => Self::DecimalArray,
                        NativeType::Double => Self::DoubleArray,
                        NativeType::Duration => Self::DurationArray,
                        NativeType::Float => Self::FloatArray,
                        NativeType::Int => Self::IntArray,
                        NativeType::BigInt => Self::BigIntArray,
                        NativeType::Text => Self::TextArray,
                        NativeType::Timestamp => Self::TimestampArray,
                        NativeType::Inet => Self::InetArray,
                        NativeType::SmallInt => Self::SmallIntArray,
                        NativeType::TinyInt => Self::TinyIntArray,
                        NativeType::Time => Self::TimeArray,
                        NativeType::Timeuuid => Self::TimeuuidArray,
                        NativeType::Uuid => Self::UuidArray,
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
            ColumnType::Vector {
                typ: _,
                dimensions: _,
            } => Self::Vector,
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
