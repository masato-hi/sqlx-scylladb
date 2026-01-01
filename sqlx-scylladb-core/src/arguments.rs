use std::{
    collections::HashMap,
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
use sqlx::Arguments;
use uuid::Uuid;

use crate::{ScyllaDB, ScyllaDBTypeInfo};

/// Implementation of [sqlx::Arguments] for ScyllaDB.
#[derive(Default)]
pub struct ScyllaDBArguments {
    pub(crate) types: Vec<ScyllaDBTypeInfo>,
    pub(crate) buffer: ScyllaDBArgumentBuffer,
}

impl<'q> Arguments<'q> for ScyllaDBArguments {
    type Database = ScyllaDB;

    fn reserve(&mut self, additional: usize, size: usize) {
        self.types.reserve(additional);
        self.buffer.reserve(size);
    }

    fn add<T>(&mut self, value: T) -> Result<(), sqlx::error::BoxDynError>
    where
        T: 'q + sqlx::Encode<'q, Self::Database> + sqlx::Type<Self::Database>,
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
pub enum ScyllaDBArgument {
    /// Internally used NULL.
    Null,
    /// Internally used Unset.
    Unset,
    /// Any type can be used.
    Any(Arc<dyn SerializeValue + Send + Sync>),
    /// `boolean` type.
    Boolean(bool),
    /// array of `boolean` type.
    BooleanArray(Vec<bool>),
    /// `tinyint` type.
    TinyInt(i8),
    /// array of `tinyint` type.
    TinyIntArray(Vec<i8>),
    /// `smallint` type.
    SmallInt(i16),
    /// array of `smallint` type
    SmallIntArray(Vec<i16>),
    /// `int` type.
    Int(i32),
    /// array of `int` type.
    IntArray(Vec<i32>),
    /// `bigint` type.
    BigInt(i64),
    /// array of `bigint` type.
    BigIntArray(Vec<i64>),
    /// `float` type.
    Float(f32),
    /// array of `float` type.
    FloatArray(Vec<f32>),
    /// `double` type.
    Double(f64),
    /// array of `double` type.
    DoubleArray(Vec<f64>),
    /// `text` or `ascii` type.
    Text(String),
    /// array of `text` or `ascii` type.
    TextArray(Vec<String>),
    /// `text` or `ascii` type implemented with [secrecy_08] crate.
    #[cfg(feature = "secrecy-08")]
    SecretText(secrecy_08::SecretString),
    /// array of `text` or `ascii` type implemented with [secrecy_08] crate.
    #[cfg(feature = "secrecy-08")]
    SecretTextArray(Vec<secrecy_08::SecretString>),
    /// `blob` type.
    Blob(Vec<u8>),
    /// array of `blob` type.
    BlobArray(Vec<Vec<u8>>),
    /// `blob` type implemented with [secrecy_08] crate.
    #[cfg(feature = "secrecy-08")]
    SecretBlob(secrecy_08::SecretVec<u8>),
    /// array of `blob` type implemented with [secrecy_08] crate.
    #[cfg(feature = "secrecy-08")]
    SecretBlobArray(Vec<secrecy_08::SecretVec<u8>>),
    /// `uuid` type.
    Uuid(Uuid),
    /// array of `uuid` type.
    UuidArray(Vec<Uuid>),
    /// `timeuuid` type.
    Timeuuid(CqlTimeuuid),
    /// array of `timeuuid` type.
    TimeuuidArray(Vec<CqlTimeuuid>),
    /// `inet` type.
    IpAddr(IpAddr),
    /// array of `inet` type.
    IpAddrArray(Vec<IpAddr>),
    /// `duration` type.
    Duration(CqlDuration),
    /// array of `duration` type.
    DurationArray(Vec<CqlDuration>),
    /// `decimal` type.
    #[cfg(feature = "bigdecimal-04")]
    BigDecimal(bigdecimal_04::BigDecimal),
    /// array of `decimal` type.
    #[cfg(feature = "bigdecimal-04")]
    BigDecimalArray(Vec<bigdecimal_04::BigDecimal>),
    /// `timestamp` type.
    CqlTimestamp(CqlTimestamp),
    /// array of `timestamp` type.
    CqlTimestampArray(Vec<CqlTimestamp>),
    /// `timestamp` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    OffsetDateTime(time_03::OffsetDateTime),
    /// array of `timestamp` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    OffsetDateTimeArray(Vec<time_03::OffsetDateTime>),
    /// `timestamp` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    ChronoDateTimeUTC(chrono_04::DateTime<chrono_04::Utc>),
    /// array of `timestamp` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    ChronoDateTimeUTCArray(Vec<chrono_04::DateTime<chrono_04::Utc>>),
    /// `date` type.
    CqlDate(CqlDate),
    /// array of `date` type.
    CqlDateArray(Vec<CqlDate>),
    /// `date` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    Date(time_03::Date),
    /// array of `date` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    DateArray(Vec<time_03::Date>),
    /// `date` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    ChronoNaiveDate(chrono_04::NaiveDate),
    /// array of `date` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    ChronoNaiveDateArray(Vec<chrono_04::NaiveDate>),
    /// `time` type.
    CqlTime(CqlTime),
    /// array of `time` type.
    CqlTimeArray(Vec<CqlTime>),
    /// `time` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    Time(time_03::Time),
    /// array of `time` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    TimeArray(Vec<time_03::Time>),
    /// `time` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    ChronoNaiveTime(chrono_04::NaiveTime),
    /// array of `time` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    ChronoNaiveTimeArray(Vec<chrono_04::NaiveTime>),
    /// any tuple type.
    Tuple(Box<dyn SerializeValue + Send + Sync>),
    /// user-defined type.
    UserDefinedType(Box<dyn SerializeValue + Send + Sync>),
    /// array of user-defined type.
    UserDefinedTypeArray(Vec<Box<dyn SerializeValue + Send + Sync>>),
    /// map type for `text` and `text`.
    TextTextMap(HashMap<String, String>),
    /// map type for `text` and `boolean`.
    TextBooleanMap(HashMap<String, bool>),
    /// map type for `text` and `tinyint`.
    TextTinyIntMap(HashMap<String, i8>),
    /// map type for `text` and `smallint`.
    TextSmallIntMap(HashMap<String, i16>),
    /// map type for `text` and `int`.
    TextIntMap(HashMap<String, i32>),
    /// map type for `text` and `bigint`.
    TextBigIntMap(HashMap<String, i64>),
    /// map type for `text` and `float`.
    TextFloatMap(HashMap<String, f32>),
    /// map type for `text` and `double`.
    TextDoubleMap(HashMap<String, f64>),
    /// map type for `text` and `uuid`.
    TextUuidMap(HashMap<String, Uuid>),
    /// map type for `text` and `inet`.
    TextIpAddrMap(HashMap<String, IpAddr>),
}

impl SerializeValue for ScyllaDBArgument {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Any(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Null => Ok(writer.set_null()),
            Self::Unset => Ok(writer.set_unset()),
            Self::Boolean(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BooleanArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TinyInt(value) => {
                if ColumnType::Native(NativeType::Counter) == *typ {
                    <_ as SerializeValue>::serialize(&Counter(*value as i64), typ, writer)
                } else {
                    <_ as SerializeValue>::serialize(value, typ, writer)
                }
            }
            Self::TinyIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::SmallInt(value) => {
                if ColumnType::Native(NativeType::Counter) == *typ {
                    <_ as SerializeValue>::serialize(&Counter(*value as i64), typ, writer)
                } else {
                    <_ as SerializeValue>::serialize(value, typ, writer)
                }
            }
            Self::SmallIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Int(value) => {
                if ColumnType::Native(NativeType::Counter) == *typ {
                    <_ as SerializeValue>::serialize(&Counter(*value as i64), typ, writer)
                } else {
                    <_ as SerializeValue>::serialize(value, typ, writer)
                }
            }
            Self::IntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BigInt(value) => {
                if ColumnType::Native(NativeType::Counter) == *typ {
                    <_ as SerializeValue>::serialize(&Counter(*value as i64), typ, writer)
                } else {
                    <_ as SerializeValue>::serialize(value, typ, writer)
                }
            }
            Self::BigIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Float(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::FloatArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Double(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::DoubleArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Text(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::SecretText(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::SecretTextArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Blob(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BlobArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::SecretBlob(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::SecretBlobArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Uuid(uuid) => <_ as SerializeValue>::serialize(uuid, typ, writer),
            Self::UuidArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Timeuuid(timeuuid) => <_ as SerializeValue>::serialize(timeuuid, typ, writer),
            Self::TimeuuidArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::IpAddr(ip_addr) => <_ as SerializeValue>::serialize(ip_addr, typ, writer),
            Self::IpAddrArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Duration(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::DurationArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "bigdecimal-04")]
            Self::BigDecimal(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "bigdecimal-04")]
            Self::BigDecimalArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::CqlTimestamp(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::CqlTimestampArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::OffsetDateTime(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::OffsetDateTimeArray(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            #[cfg(feature = "chrono-04")]
            Self::ChronoDateTimeUTC(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoDateTimeUTCArray(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            Self::CqlTime(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::CqlTimeArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Time(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::TimeArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoNaiveTime(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoNaiveTimeArray(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            Self::CqlDate(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::CqlDateArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Date(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::DateArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoNaiveDate(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoNaiveDateArray(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            Self::Tuple(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UserDefinedType(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UserDefinedTypeArray(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            Self::TextTextMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextBooleanMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextTinyIntMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextSmallIntMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextIntMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextBigIntMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextFloatMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextDoubleMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextUuidMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextIpAddrMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}
