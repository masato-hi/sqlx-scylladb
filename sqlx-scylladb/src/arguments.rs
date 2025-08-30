use std::{
    borrow::Cow,
    collections::HashMap,
    net::IpAddr,
    ops::{Deref, DerefMut},
};

use scylla::{
    cluster::metadata::ColumnType,
    errors::SerializationError,
    serialize::{
        row::{RowSerializationContext, SerializeRow},
        value::SerializeValue,
        writers::{CellWriter, RowWriter, WrittenCellProof},
    },
    value::CqlTimeuuid,
};
use sqlx::Arguments;
use uuid::Uuid;

use crate::{ScyllaDB, ScyllaDBTypeInfo};

#[derive(Default)]
pub struct ScyllaDBArguments<'q> {
    pub(crate) types: Vec<ScyllaDBTypeInfo>,
    pub(crate) buffer: ScyllaDBArgumentBuffer<'q>,
}

impl<'q> Arguments<'q> for ScyllaDBArguments<'q> {
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
        let _ = value.encode_by_ref(&mut self.buffer)?;
        self.types.push(ty);

        Ok(())
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl<'q> SerializeRow for ScyllaDBArguments<'q> {
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
pub struct ScyllaDBArgumentBuffer<'q> {
    pub(crate) buffer: Vec<ScyllaDBArgument<'q>>,
}

impl<'q> Deref for ScyllaDBArgumentBuffer<'q> {
    type Target = Vec<ScyllaDBArgument<'q>>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl<'q> DerefMut for ScyllaDBArgumentBuffer<'q> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

pub enum ScyllaDBArgument<'q> {
    Null,
    Boolean(bool),
    BooleanArray(&'q [bool]),
    TinyInt(i8),
    TinyIntArray(&'q [i8]),
    SmallInt(i16),
    SmallIntArray(&'q [i16]),
    Int(i32),
    IntArray(&'q [i32]),
    BigInt(i64),
    BigIntArray(&'q [i64]),
    Float(f32),
    FloatArray(&'q [f32]),
    Double(f64),
    DoubleArray(&'q [f64]),
    Text(Cow<'q, str>),
    TextArray(&'q [String]),
    Blob(Cow<'q, [u8]>),
    Uuid(Uuid),
    UuidArray(&'q [Uuid]),
    Timeuuid(CqlTimeuuid),
    TimeuuidArray(&'q [CqlTimeuuid]),
    IpAddr(IpAddr),
    IpAddrArray(&'q [IpAddr]),
    #[cfg(feature = "bigdecimal-04")]
    BigDecimal(bigdecimal_04::BigDecimal),
    #[cfg(feature = "bigdecimal-04")]
    BigDecimalArray(&'q [bigdecimal_04::BigDecimal]),
    #[cfg(feature = "time-03")]
    OffsetDateTime(time_03::OffsetDateTime),
    #[cfg(feature = "time-03")]
    OffsetDateTimeArray(&'q [time_03::OffsetDateTime]),
    #[cfg(feature = "chrono-04")]
    ChronoDateTimeUTC(chrono_04::DateTime<chrono_04::Utc>),
    #[cfg(feature = "chrono-04")]
    ChronoDateTimeUTCArray(&'q [chrono_04::DateTime<chrono_04::Utc>]),
    #[cfg(feature = "time-03")]
    Date(time_03::Date),
    #[cfg(feature = "time-03")]
    DateArray(&'q [time_03::Date]),
    #[cfg(feature = "chrono-04")]
    ChronoNaiveDate(chrono_04::NaiveDate),
    #[cfg(feature = "chrono-04")]
    ChronoNaiveDateArray(&'q [chrono_04::NaiveDate]),
    #[cfg(feature = "time-03")]
    Time(time_03::Time),
    #[cfg(feature = "time-03")]
    TimeArray(&'q [time_03::Time]),
    #[cfg(feature = "chrono-04")]
    ChronoNaiveTime(chrono_04::NaiveTime),
    #[cfg(feature = "chrono-04")]
    ChronoNaiveTimeArray(&'q [chrono_04::NaiveTime]),
    Tuple(Box<dyn SerializeValue + Send + Sync + 'q>),
    UserDefinedType(&'q (dyn SerializeValue + Send + Sync)),
    UserDefinedTypeArray(&'q (dyn SerializeValue + Send + Sync)),
    TextTextMap(&'q HashMap<String, String>),
    TextBooleanMap(&'q HashMap<String, bool>),
    TextTinyIntMap(&'q HashMap<String, i8>),
    TextSmallIntMap(&'q HashMap<String, i16>),
    TextIntMap(&'q HashMap<String, i32>),
    TextBigIntMap(&'q HashMap<String, i64>),
    TextFloatMap(&'q HashMap<String, f32>),
    TextDoubleMap(&'q HashMap<String, f64>),
    TextUuidMap(&'q HashMap<String, Uuid>),
    UuidTextMap(&'q HashMap<Uuid, String>),
    UuidBooleanMap(&'q HashMap<Uuid, bool>),
    UuidTinyIntMap(&'q HashMap<Uuid, i8>),
    UuidSmallIntMap(&'q HashMap<Uuid, i16>),
    UuidIntMap(&'q HashMap<Uuid, i32>),
    UuidBigIntMap(&'q HashMap<Uuid, i64>),
    UuidFloatMap(&'q HashMap<Uuid, f32>),
    UuidDoubleMap(&'q HashMap<Uuid, f64>),
    UuidUuidMap(&'q HashMap<Uuid, Uuid>),
}

impl<'q> SerializeValue for ScyllaDBArgument<'q> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Null => Ok(writer.set_null()),
            Self::Boolean(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BooleanArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::TinyInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TinyIntArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::SmallInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::SmallIntArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::Int(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::IntArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::BigInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BigIntArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::Float(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::FloatArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::Double(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::DoubleArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::Text(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::TextArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::Blob(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::Uuid(uuid) => <_ as SerializeValue>::serialize(uuid, typ, writer),
            Self::UuidArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::Timeuuid(timeuuid) => <_ as SerializeValue>::serialize(timeuuid, typ, writer),
            Self::TimeuuidArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            Self::IpAddr(ip_addr) => <_ as SerializeValue>::serialize(ip_addr, typ, writer),
            Self::IpAddrArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            #[cfg(feature = "bigdecimal-04")]
            Self::BigDecimal(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "bigdecimal-04")]
            Self::BigDecimalArray(value) => {
                <_ as SerializeValue>::serialize(&&**value, typ, writer)
            }
            #[cfg(feature = "time-03")]
            Self::OffsetDateTime(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::OffsetDateTimeArray(value) => {
                <_ as SerializeValue>::serialize(&&**value, typ, writer)
            }
            #[cfg(feature = "chrono-04")]
            Self::ChronoDateTimeUTC(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoDateTimeUTCArray(value) => {
                <_ as SerializeValue>::serialize(&&**value, typ, writer)
            }
            #[cfg(feature = "time-03")]
            Self::Time(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::TimeArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoNaiveTime(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoNaiveTimeArray(value) => {
                <_ as SerializeValue>::serialize(&&**value, typ, writer)
            }
            #[cfg(feature = "time-03")]
            Self::Date(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::DateArray(value) => <_ as SerializeValue>::serialize(&&**value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoNaiveDate(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::ChronoNaiveDateArray(value) => {
                <_ as SerializeValue>::serialize(&&**value, typ, writer)
            }
            Self::Tuple(dynamic) => <_ as SerializeValue>::serialize(dynamic, typ, writer),
            Self::UserDefinedType(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UserDefinedTypeArray(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
            Self::TextTextMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::TextBooleanMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::TextTinyIntMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::TextSmallIntMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::TextIntMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::TextBigIntMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::TextFloatMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::TextDoubleMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::TextUuidMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidTextMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidBooleanMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidTinyIntMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidSmallIntMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidIntMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidBigIntMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidFloatMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidDoubleMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
            Self::UuidUuidMap(value) => <_ as SerializeValue>::serialize(&**value, typ, writer),
        }
    }
}
