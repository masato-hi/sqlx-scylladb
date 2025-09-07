use std::{
    collections::HashMap,
    net::IpAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use scylla::{
    cluster::metadata::ColumnType,
    errors::SerializationError,
    serialize::{
        row::{RowSerializationContext, SerializeRow},
        value::SerializeValue,
        writers::{CellWriter, RowWriter, WrittenCellProof},
    },
    value::{CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid},
};
use sqlx::Arguments;
use uuid::Uuid;

use crate::{ScyllaDB, ScyllaDBTypeInfo};

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

pub enum ScyllaDBArgument {
    Null,
    Boolean(bool),
    BooleanArray(Arc<Vec<bool>>),
    TinyInt(i8),
    TinyIntArray(Arc<Vec<i8>>),
    SmallInt(i16),
    SmallIntArray(Arc<Vec<i16>>),
    Int(i32),
    IntArray(Arc<Vec<i32>>),
    BigInt(i64),
    BigIntArray(Arc<Vec<i64>>),
    Float(f32),
    FloatArray(Arc<Vec<f32>>),
    Double(f64),
    DoubleArray(Arc<Vec<f64>>),
    Text(Arc<String>),
    TextArray(Arc<Vec<String>>),
    Blob(Arc<Vec<u8>>),
    BlobArray(Arc<Vec<Vec<u8>>>),
    Uuid(Uuid),
    UuidArray(Arc<Vec<Uuid>>),
    Timeuuid(CqlTimeuuid),
    TimeuuidArray(Arc<Vec<CqlTimeuuid>>),
    IpAddr(IpAddr),
    IpAddrArray(Arc<Vec<IpAddr>>),
    Duration(CqlDuration),
    DurationArray(Arc<Vec<CqlDuration>>),
    #[cfg(feature = "bigdecimal-04")]
    BigDecimal(bigdecimal_04::BigDecimal),
    #[cfg(feature = "bigdecimal-04")]
    BigDecimalArray(Arc<Vec<bigdecimal_04::BigDecimal>>),
    CqlTimestamp(CqlTimestamp),
    CqlTimestampArray(Arc<Vec<CqlTimestamp>>),
    #[cfg(feature = "time-03")]
    OffsetDateTime(time_03::OffsetDateTime),
    #[cfg(feature = "time-03")]
    OffsetDateTimeArray(Arc<Vec<time_03::OffsetDateTime>>),
    #[cfg(feature = "chrono-04")]
    ChronoDateTimeUTC(chrono_04::DateTime<chrono_04::Utc>),
    #[cfg(feature = "chrono-04")]
    ChronoDateTimeUTCArray(Arc<Vec<chrono_04::DateTime<chrono_04::Utc>>>),
    CqlDate(CqlDate),
    CqlDateArray(Arc<Vec<CqlDate>>),
    #[cfg(feature = "time-03")]
    Date(time_03::Date),
    #[cfg(feature = "time-03")]
    DateArray(Arc<Vec<time_03::Date>>),
    #[cfg(feature = "chrono-04")]
    ChronoNaiveDate(chrono_04::NaiveDate),
    #[cfg(feature = "chrono-04")]
    ChronoNaiveDateArray(Arc<Vec<chrono_04::NaiveDate>>),
    CqlTime(CqlTime),
    CqlTimeArray(Arc<Vec<CqlTime>>),
    #[cfg(feature = "time-03")]
    Time(time_03::Time),
    #[cfg(feature = "time-03")]
    TimeArray(Arc<Vec<time_03::Time>>),
    #[cfg(feature = "chrono-04")]
    ChronoNaiveTime(chrono_04::NaiveTime),
    #[cfg(feature = "chrono-04")]
    ChronoNaiveTimeArray(Arc<Vec<chrono_04::NaiveTime>>),
    Tuple(Arc<dyn SerializeValue + Send + Sync>),
    UserDefinedType(Arc<dyn SerializeValue + Send + Sync>),
    UserDefinedTypeArray(Arc<dyn SerializeValue + Send + Sync>),
    TextTextMap(Arc<HashMap<String, String>>),
    TextBooleanMap(Arc<HashMap<String, bool>>),
    TextTinyIntMap(Arc<HashMap<String, i8>>),
    TextSmallIntMap(Arc<HashMap<String, i16>>),
    TextIntMap(Arc<HashMap<String, i32>>),
    TextBigIntMap(Arc<HashMap<String, i64>>),
    TextFloatMap(Arc<HashMap<String, f32>>),
    TextDoubleMap(Arc<HashMap<String, f64>>),
    TextUuidMap(Arc<HashMap<String, Uuid>>),
    UuidTextMap(Arc<HashMap<Uuid, String>>),
    UuidBooleanMap(Arc<HashMap<Uuid, bool>>),
    UuidTinyIntMap(Arc<HashMap<Uuid, i8>>),
    UuidSmallIntMap(Arc<HashMap<Uuid, i16>>),
    UuidIntMap(Arc<HashMap<Uuid, i32>>),
    UuidBigIntMap(Arc<HashMap<Uuid, i64>>),
    UuidFloatMap(Arc<HashMap<Uuid, f32>>),
    UuidDoubleMap(Arc<HashMap<Uuid, f64>>),
    UuidUuidMap(Arc<HashMap<Uuid, Uuid>>),
}

impl SerializeValue for ScyllaDBArgument {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Null => Ok(writer.set_null()),
            Self::Boolean(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BooleanArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TinyInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TinyIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::SmallInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::SmallIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Int(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::IntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BigInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BigIntArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Float(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::FloatArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Double(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::DoubleArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Text(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TextArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Blob(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BlobArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
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
            Self::UuidTextMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UuidBooleanMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UuidTinyIntMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UuidSmallIntMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UuidIntMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UuidBigIntMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UuidFloatMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UuidDoubleMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UuidUuidMap(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}
