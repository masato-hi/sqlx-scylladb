use std::{
    borrow::Cow,
    net::IpAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use scylla::{
    cluster::metadata::{ColumnType, NativeType},
    errors::SerializationError,
    serialize::{
        row::{RowSerializationContext, SerializeRow},
        value::SerializeValue,
        writers::{CellWriter, RowWriter, WrittenCellProof},
    },
    value::{Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid},
};
use sqlx_core::{arguments::Arguments, encode::Encode, types::Type};
use uuid::Uuid;

use crate::{ScyllaDB, ScyllaDBTypeInfo};

/// Implementation of [sqlx::Arguments] for ScyllaDB.
#[derive(Default)]
pub struct ScyllaDBArguments {
    pub(crate) types: Vec<ScyllaDBTypeInfo>,
    pub(crate) buffer: ScyllaDBArgumentBuffer,
}

impl Arguments for ScyllaDBArguments {
    type Database = ScyllaDB;

    fn reserve(&mut self, additional: usize, size: usize) {
        self.types.reserve(additional);
        self.buffer.reserve(size);
    }

    fn add<'t, T>(&mut self, value: T) -> Result<(), sqlx_core::error::BoxDynError>
    where
        T: Encode<'t, Self::Database> + Type<Self::Database>,
    {
        let ty = value.produces().unwrap_or_else(T::type_info);
        let is_null = value.encode(&mut self.buffer)?;
        if is_null.is_null() {
            self.buffer.push(ScyllaDBArgument::Null);
        }

        self.types.push(ty);

        Ok(())
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl SerializeRow for ScyllaDBArguments {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        let columns = ctx.columns();
        for (i, column) in columns.iter().enumerate() {
            if let Some(argument) = self.buffer.get(i) {
                let cell_writer = writer.make_cell_writer();
                let typ = column.typ();
                argument.serialize(typ, cell_writer)?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// An array of [ScyllaDBArguments] used during encoding.
#[derive(Default)]
pub struct ScyllaDBArgumentBuffer {
    pub(crate) buffer: Vec<ScyllaDBArgument>,
}

impl Deref for ScyllaDBArgumentBuffer {
    type Target = Vec<ScyllaDBArgument>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl<'q> DerefMut for ScyllaDBArgumentBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

/// The enum of data types that can be handled by scylla-rust-driver.
#[allow(non_camel_case_types)]
pub enum ScyllaDBArgument {
    /// Internally used NULL.
    Null,
    /// Internally used Unset.
    Unset,
    /// `boolean` type.
    Boolean(bool),
    /// `tinyint` type.
    TinyInt(i8),
    /// `smallint` type.
    SmallInt(i16),
    /// `int` type.
    Int(i32),
    /// `bigint` type.
    BigInt(i64),
    /// `float` type.
    Float(f32),
    /// `double` type.
    Double(f64),
    /// Text held either as a static borrow or as an owned string.
    Text(Cow<'static, str>),
    /// Text held by an atomically reference-counted string slice.
    #[allow(non_camel_case_types)]
    Text_ArcStr(Arc<str>),
    /// Secret text held by value.
    #[cfg(feature = "secrecy-08")]
    #[allow(non_camel_case_types)]
    Text_Secrecy08(secrecy_08::SecretString),
    /// `blob` type.
    Blob(Vec<u8>),
    /// `blob` type implemented with [secrecy_08] crate.
    #[cfg(feature = "secrecy-08")]
    #[allow(non_camel_case_types)]
    Blob_Secrecy08(secrecy_08::SecretVec<u8>),
    /// `uuid` type.
    Uuid(Uuid),
    /// `timeuuid` type.
    Timeuuid(CqlTimeuuid),
    /// `inet` type.
    Inet(IpAddr),
    /// `duration` type.
    Duration(CqlDuration),
    /// `decimal` type.
    #[cfg(feature = "bigdecimal-04")]
    Decimal(bigdecimal_04::BigDecimal),
    /// ScyllaDB timestamp.
    Timestamp(CqlTimestamp),
    /// `time` crate timestamp.
    #[cfg(feature = "time-03")]
    #[allow(non_camel_case_types)]
    Timestamp_Time03(time_03::OffsetDateTime),
    /// `chrono` crate timestamp.
    #[cfg(feature = "chrono-04")]
    #[allow(non_camel_case_types)]
    Timestamp_Chrono04(chrono_04::DateTime<chrono_04::Utc>),
    /// ScyllaDB date.
    Date(CqlDate),
    /// `time` crate date.
    #[cfg(feature = "time-03")]
    #[allow(non_camel_case_types)]
    Date_Time03(time_03::Date),
    /// `chrono` crate date.
    #[cfg(feature = "chrono-04")]
    #[allow(non_camel_case_types)]
    Date_Chrono04(chrono_04::NaiveDate),
    /// ScyllaDB time.
    Time(CqlTime),
    /// `time` crate time.
    #[cfg(feature = "time-03")]
    #[allow(non_camel_case_types)]
    Time_Time03(time_03::Time),
    /// `chrono` crate time.
    #[cfg(feature = "chrono-04")]
    #[allow(non_camel_case_types)]
    Time_Chrono04(chrono_04::NaiveTime),
    /// array of `boolean` type.
    BooleanArray(Vec<bool>),
    /// array of `tinyint` type.
    TinyIntArray(Vec<i8>),
    /// array of `smallint` type.
    SmallIntArray(Vec<i16>),
    /// array of `int` type.
    IntArray(Vec<i32>),
    /// array of `bigint` type.
    BigIntArray(Vec<i64>),
    /// array of `float` type.
    FloatArray(Vec<f32>),
    /// array of `double` type.
    DoubleArray(Vec<f64>),
    /// array of `text` or `ascii` type.
    TextArray(Vec<String>),
    /// array of `text` or `ascii` type implemented with [secrecy_08] crate.
    #[cfg(feature = "secrecy-08")]
    TextArray_Secrecy08(Vec<secrecy_08::SecretString>),
    /// array of `blob` type.
    BlobArray(Vec<Vec<u8>>),
    /// array of `blob` type implemented with [secrecy_08] crate.
    #[cfg(feature = "secrecy-08")]
    BlobArray_Secrecy08(Vec<secrecy_08::SecretVec<u8>>),
    /// array of `uuid` type.
    UuidArray(Vec<Uuid>),
    /// array of `timeuuid` type.
    TimeuuidArray(Vec<CqlTimeuuid>),
    /// array of `inet` type.
    InetArray(Vec<IpAddr>),
    /// array of `duration` type.
    DurationArray(Vec<CqlDuration>),
    /// array of `decimal` type.
    #[cfg(feature = "bigdecimal-04")]
    DecimalArray(Vec<bigdecimal_04::BigDecimal>),
    /// array of `timestamp` type.
    TimestampArray(Vec<CqlTimestamp>),
    /// array of `timestamp` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    TimestampArray_Time03(Vec<time_03::OffsetDateTime>),
    /// array of `timestamp` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    TimestampArray_Chrono04(Vec<chrono_04::DateTime<chrono_04::Utc>>),
    /// array of `date` type.
    DateArray(Vec<CqlDate>),
    /// array of `date` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    DateArray_Time03(Vec<time_03::Date>),
    /// array of `date` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    DateArray_Chrono04(Vec<chrono_04::NaiveDate>),
    /// array of `time` type.
    TimeArray(Vec<CqlTime>),
    /// array of `time` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    TimeArray_Time03(Vec<time_03::Time>),
    /// array of `time` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    TimeArray_Chrono04(Vec<chrono_04::NaiveTime>),
    /// any map type.
    Map(Box<dyn SerializeValue + Send + Sync>),
    /// any tuple type.
    Tuple(Box<dyn SerializeValue + Send + Sync>),
    /// user-defined type.
    UserDefinedType(Box<dyn SerializeValue + Send + Sync>),
    /// array of user-defined type.
    UserDefinedTypeArray(Vec<Box<dyn SerializeValue + Send + Sync>>),
}

impl SerializeValue for ScyllaDBArgument {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Null => Ok(writer.set_null()),
            Self::Unset => Ok(writer.set_unset()),
            Self::Boolean(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TinyInt(value) => {
                if ColumnType::Native(NativeType::Counter) == *typ {
                    <_ as SerializeValue>::serialize(&Counter(*value as i64), typ, writer)
                } else {
                    <_ as SerializeValue>::serialize(value, typ, writer)
                }
            }
            Self::SmallInt(value) => {
                if ColumnType::Native(NativeType::Counter) == *typ {
                    <_ as SerializeValue>::serialize(&Counter(*value as i64), typ, writer)
                } else {
                    <_ as SerializeValue>::serialize(value, typ, writer)
                }
            }
            Self::Int(value) => {
                if ColumnType::Native(NativeType::Counter) == *typ {
                    <_ as SerializeValue>::serialize(&Counter(*value as i64), typ, writer)
                } else {
                    <_ as SerializeValue>::serialize(value, typ, writer)
                }
            }
            Self::BigInt(value) => {
                if ColumnType::Native(NativeType::Counter) == *typ {
                    <_ as SerializeValue>::serialize(&Counter(*value as i64), typ, writer)
                } else {
                    <_ as SerializeValue>::serialize(value, typ, writer)
                }
            }
            Self::Float(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Double(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Text(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Text_ArcStr(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::Text_Secrecy08(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Blob(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::Blob_Secrecy08(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Uuid(uuid) => <_ as SerializeValue>::serialize(uuid, typ, writer),
            Self::Timeuuid(timeuuid) => <_ as SerializeValue>::serialize(timeuuid, typ, writer),
            Self::Inet(ip_addr) => <_ as SerializeValue>::serialize(ip_addr, typ, writer),
            Self::Duration(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "bigdecimal-04")]
            Self::Decimal(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Timestamp(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Timestamp_Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Timestamp_Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Date(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Date_Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Date_Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Time(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Time_Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Time_Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BooleanArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TinyIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::SmallIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::IntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BigIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::FloatArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::DoubleArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::TextArray_Secrecy08(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            Self::BlobArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::BlobArray_Secrecy08(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            Self::UuidArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TimeuuidArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::InetArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::DurationArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "bigdecimal-04")]
            Self::DecimalArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TimestampArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::TimestampArray_Time03(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            #[cfg(feature = "chrono-04")]
            Self::TimestampArray_Chrono04(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            Self::DateArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::DateArray_Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::DateArray_Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TimeArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::TimeArray_Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::TimeArray_Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Map(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Tuple(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UserDefinedType(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UserDefinedTypeArray(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
        }
    }
}
