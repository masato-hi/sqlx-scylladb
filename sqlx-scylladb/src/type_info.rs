use std::fmt::Display;

use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};
use sqlx::TypeInfo;
use sqlx_core::ext::ustr::UStr;

use crate::ScyllaDBError;

#[derive(Debug, Clone, PartialEq)]
pub enum ScyllaDBTypeInfo {
    Ascii,
    AsciiArray,
    Boolean,
    BooleanArray,
    Blob,
    Counter,
    Decimal,
    DecimalArray,
    Date,
    DateArray,
    Double,
    DoubleArray,
    Duration,
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
    TextTextMap,
    TextBooleanMap,
    TextTinyIntMap,
    TextSmallIntMap,
    TextIntMap,
    TextBigIntMap,
    TextFloatMap,
    TextDoubleMap,
    TextUuidMap,
    UuidTextMap,
    UuidBooleanMap,
    UuidTinyIntMap,
    UuidSmallIntMap,
    UuidIntMap,
    UuidBigIntMap,
    UuidFloatMap,
    UuidDoubleMap,
    UuidUuidMap,
}

impl TypeInfo for ScyllaDBTypeInfo {
    fn is_null(&self) -> bool {
        *self == Self::Null
    }

    fn name(&self) -> &str {
        match self {
            Self::Ascii => "ASCII",
            Self::AsciiArray => "ASCII[]",
            Self::Text => "TEXT",
            Self::TextArray => "TEXT[]",
            Self::Boolean => "BOOLEAN",
            Self::BooleanArray => "BOOLEAN[]",
            Self::Blob => "BLOB",
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
            Self::TextTextMap => "MAP<TEXT, TEXT>",
            Self::TextBooleanMap => "MAP<TEXT, BOOLEAN>",
            Self::TextTinyIntMap => "MAP<TEXT, TINYINT>",
            Self::TextSmallIntMap => "MAP<TEXT, SMALLINT>",
            Self::TextIntMap => "MAP<TEXT, INT>",
            Self::TextBigIntMap => "MAP<TEXT, BIGINT>",
            Self::TextFloatMap => "MAP<TEXT, FLOAT>",
            Self::TextDoubleMap => "MAP<TEXT, DOUBLE>",
            Self::TextUuidMap => "MAP<TEXT, UUID>",
            Self::UuidTextMap => "MAP<UUID, TEXT>",
            Self::UuidBooleanMap => "MAP<UUID, BOOLEAN>",
            Self::UuidTinyIntMap => "MAP<UUID, TINYINT>",
            Self::UuidSmallIntMap => "MAP<UUID, SMALLINT>",
            Self::UuidIntMap => "MAP<UUID, INT>",
            Self::UuidBigIntMap => "MAP<UUID, BIGINT>",
            Self::UuidFloatMap => "MAP<UUID, FLOAT>",
            Self::UuidDoubleMap => "MAP<UUID, DOUBLE>",
            Self::UuidUuidMap => "MAP<UUID, UUID>",
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
            _ => self == other,
        }
    }
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
                _ => todo!(),
            },
            ColumnType::Collection { frozen: _, typ } => match typ {
                CollectionType::List(inner) | CollectionType::Set(inner) => match &**inner {
                    ColumnType::Native(native_type) => match native_type {
                        NativeType::Ascii => Self::AsciiArray,
                        NativeType::Boolean => Self::BooleanArray,
                        NativeType::Blob => todo!(),
                        NativeType::Counter => todo!(),
                        NativeType::Date => Self::DateArray,
                        NativeType::Decimal => Self::DecimalArray,
                        NativeType::Double => Self::DoubleArray,
                        NativeType::Duration => todo!(),
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
                        NativeType::Varint => todo!(),
                        _ => todo!(),
                    },
                    ColumnType::UserDefinedType {
                        frozen: _,
                        definition,
                    } => {
                        let type_name = format!("{}[]", definition.name);
                        let type_name = UStr::new(&type_name);
                        Self::UserDefinedType(type_name)
                    }
                    _ => todo!(),
                },
                CollectionType::Map(key_type, value_type) => match &**key_type {
                    ColumnType::Native(key_native_type) => match key_native_type {
                        NativeType::Ascii | NativeType::Text => match &**value_type {
                            ColumnType::Native(value_native_type) => match value_native_type {
                                NativeType::Ascii | NativeType::Text => Self::TextTextMap,
                                NativeType::Boolean => Self::TextBooleanMap,
                                NativeType::Double => Self::TextDoubleMap,
                                NativeType::Float => Self::TextFloatMap,
                                NativeType::Int => Self::TextIntMap,
                                NativeType::BigInt => Self::TextBigIntMap,
                                NativeType::SmallInt => Self::TextSmallIntMap,
                                NativeType::TinyInt => Self::TextTinyIntMap,
                                NativeType::Timeuuid | NativeType::Uuid => Self::TextUuidMap,
                                _ => todo!(),
                            },
                            _ => todo!(),
                        },
                        NativeType::Timeuuid | NativeType::Uuid => match &**value_type {
                            ColumnType::Native(value_native_type) => match value_native_type {
                                NativeType::Ascii | NativeType::Text => Self::UuidTextMap,
                                NativeType::Boolean => Self::UuidBooleanMap,
                                NativeType::Double => Self::UuidDoubleMap,
                                NativeType::Float => Self::UuidFloatMap,
                                NativeType::Int => Self::UuidIntMap,
                                NativeType::BigInt => Self::UuidBigIntMap,
                                NativeType::SmallInt => Self::UuidSmallIntMap,
                                NativeType::TinyInt => Self::UuidTinyIntMap,
                                NativeType::Timeuuid | NativeType::Uuid => Self::UuidUuidMap,
                                _ => todo!(),
                            },
                            _ => todo!(),
                        },
                        _ => todo!(),
                    },
                    _ => todo!(),
                },
                _ => todo!(),
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
            _ => todo!(),
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
