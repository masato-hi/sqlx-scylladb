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

/// Text values supported by a native ScyllaDB argument.
#[derive(Debug)]
pub enum ScyllaDBArgumentNativeText {
    /// Text held either as a static borrow or as an owned string.
    Text(Cow<'static, str>),
    /// Text held by an atomically reference-counted string slice.
    ArcStr(Arc<str>),
    /// Secret text held by value.
    #[cfg(feature = "secrecy-08")]
    Secrecy08(secrecy_08::SecretString),
    /// Secret text held by value using secrecy 0.10.
    #[cfg(feature = "secrecy-10")]
    Secrecy10(secrecy_10::SecretString),
}

/// Blob values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeBlob {
    /// `blob` type.
    Blob(Vec<u8>),
    /// Secret `blob` type.
    #[cfg(feature = "secrecy-08")]
    Secrecy08(secrecy_08::SecretVec<u8>),
    /// Secret `blob` type using secrecy 0.10.
    #[cfg(feature = "secrecy-10")]
    Secrecy10(secrecy_10::SecretBox<Vec<u8>>),
}

/// Decimal values supported by a native ScyllaDB argument.
#[cfg(feature = "bigdecimal-04")]
pub enum ScyllaDBArgumentNativeDecimal {
    /// `decimal` type.
    Decimal(bigdecimal_04::BigDecimal),
}

/// Timestamp values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeTimestamp {
    /// ScyllaDB timestamp.
    Timestamp(CqlTimestamp),
    /// `time` crate timestamp.
    #[cfg(feature = "time-03")]
    Time03(time_03::OffsetDateTime),
    /// `chrono` crate timestamp.
    #[cfg(feature = "chrono-04")]
    Chrono04(chrono_04::DateTime<chrono_04::Utc>),
}

/// Date values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeDate {
    /// ScyllaDB date.
    Date(CqlDate),
    /// `time` crate date.
    #[cfg(feature = "time-03")]
    Time03(time_03::Date),
    /// `chrono` crate date.
    #[cfg(feature = "chrono-04")]
    Chrono04(chrono_04::NaiveDate),
}

/// Time values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeTime {
    /// ScyllaDB time.
    Time(CqlTime),
    /// `time` crate time.
    #[cfg(feature = "time-03")]
    Time03(time_03::Time),
    /// `chrono` crate time.
    #[cfg(feature = "chrono-04")]
    Chrono04(chrono_04::NaiveTime),
}

/// Text array values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeArrayText {
    /// array of `text` or `ascii` type.
    Text(Vec<String>),
    /// secret array of `text` or `ascii` type.
    #[cfg(feature = "secrecy-08")]
    Secrecy08(Vec<secrecy_08::SecretString>),
    /// Secret text array using secrecy 0.10.
    #[cfg(feature = "secrecy-10")]
    Secrecy10(Vec<secrecy_10::SecretString>),
}

/// Blob array values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeArrayBlob {
    /// array of `blob` type.
    Blob(Vec<Vec<u8>>),
    /// secret array of `blob` type.
    #[cfg(feature = "secrecy-08")]
    Secrecy08(Vec<secrecy_08::SecretVec<u8>>),
    /// Secret `blob` array using secrecy 0.10.
    #[cfg(feature = "secrecy-10")]
    Secrecy10(Vec<secrecy_10::SecretBox<Vec<u8>>>),
}

/// Decimal array values supported by a native ScyllaDB argument.
#[cfg(feature = "bigdecimal-04")]
pub enum ScyllaDBArgumentNativeArrayDecimal {
    /// array of `decimal` type.
    Decimal(Vec<bigdecimal_04::BigDecimal>),
}

/// Timestamp array values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeArrayTimestamp {
    /// array of `timestamp` type.
    Timestamp(Vec<CqlTimestamp>),
    /// array of `timestamp` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    Time03(Vec<time_03::OffsetDateTime>),
    /// array of `timestamp` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    Chrono04(Vec<chrono_04::DateTime<chrono_04::Utc>>),
}

/// Date array values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeArrayDate {
    /// array of `date` type.
    Date(Vec<CqlDate>),
    /// array of `date` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    Time03(Vec<time_03::Date>),
    /// array of `date` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    Chrono04(Vec<chrono_04::NaiveDate>),
}

/// Time array values supported by a native ScyllaDB argument.
pub enum ScyllaDBArgumentNativeArrayTime {
    /// array of `time` type.
    Time(Vec<CqlTime>),
    /// array of `time` type implemented with [time_03] crate.
    #[cfg(feature = "time-03")]
    Time03(Vec<time_03::Time>),
    /// array of `time` type implemented with [chrono_04] crate.
    #[cfg(feature = "chrono-04")]
    Chrono04(Vec<chrono_04::NaiveTime>),
}

/// A native ScyllaDB value that can be handled by scylla-rust-driver.
pub enum ScyllaDBArgumentNative {
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
    /// Text value.
    Text(ScyllaDBArgumentNativeText),
    /// `blob` type.
    Blob(ScyllaDBArgumentNativeBlob),
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
    Decimal(ScyllaDBArgumentNativeDecimal),
    /// ScyllaDB timestamp.
    Timestamp(ScyllaDBArgumentNativeTimestamp),
    /// ScyllaDB date.
    Date(ScyllaDBArgumentNativeDate),
    /// ScyllaDB time.
    Time(ScyllaDBArgumentNativeTime),
}

/// An array of native ScyllaDB values.
pub enum ScyllaDBArgumentNativeArray {
    /// array of `boolean` type.
    Boolean(Vec<bool>),
    /// array of `tinyint` type.
    TinyInt(Vec<i8>),
    /// array of `smallint` type.
    SmallInt(Vec<i16>),
    /// array of `int` type.
    Int(Vec<i32>),
    /// array of `bigint` type.
    BigInt(Vec<i64>),
    /// array of `float` type.
    Float(Vec<f32>),
    /// array of `double` type.
    Double(Vec<f64>),
    /// array of `text` or `ascii` type.
    Text(ScyllaDBArgumentNativeArrayText),
    /// array of `blob` type.
    Blob(ScyllaDBArgumentNativeArrayBlob),
    /// array of `uuid` type.
    Uuid(Vec<Uuid>),
    /// array of `timeuuid` type.
    Timeuuid(Vec<CqlTimeuuid>),
    /// array of `inet` type.
    Inet(Vec<IpAddr>),
    /// array of `duration` type.
    Duration(Vec<CqlDuration>),
    /// array of `decimal` type.
    #[cfg(feature = "bigdecimal-04")]
    Decimal(ScyllaDBArgumentNativeArrayDecimal),
    /// array of `timestamp` type.
    Timestamp(ScyllaDBArgumentNativeArrayTimestamp),
    /// array of `date` type.
    Date(ScyllaDBArgumentNativeArrayDate),
    /// array of `time` type.
    Time(ScyllaDBArgumentNativeArrayTime),
}

/// The enum of data types that can be handled by scylla-rust-driver.
pub enum ScyllaDBArgument {
    /// Internally used NULL.
    Null,
    /// Internally used Unset.
    Unset,
    /// A native ScyllaDB value.
    Native(ScyllaDBArgumentNative),
    /// An array of native ScyllaDB values.
    NativeArray(ScyllaDBArgumentNativeArray),
    /// any map type.
    Map(Box<dyn SerializeValue + Send + Sync>),
    /// any tuple type.
    Tuple(Box<dyn SerializeValue + Send + Sync>),
    /// user-defined type.
    UserDefinedType(Box<dyn SerializeValue + Send + Sync>),
    /// array of user-defined type.
    UserDefinedTypeArray(Box<dyn SerializeValue + Send + Sync>),
}

impl From<ScyllaDBArgumentNative> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNative) -> Self {
        Self::Native(value)
    }
}

impl From<ScyllaDBArgumentNativeArray> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeArray) -> Self {
        Self::NativeArray(value)
    }
}

impl From<ScyllaDBArgumentNativeText> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeText) -> Self {
        Self::Native(ScyllaDBArgumentNative::Text(value))
    }
}

impl From<ScyllaDBArgumentNativeBlob> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeBlob) -> Self {
        Self::Native(ScyllaDBArgumentNative::Blob(value))
    }
}

#[cfg(feature = "bigdecimal-04")]
impl From<ScyllaDBArgumentNativeDecimal> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeDecimal) -> Self {
        Self::Native(ScyllaDBArgumentNative::Decimal(value))
    }
}

impl From<ScyllaDBArgumentNativeTimestamp> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeTimestamp) -> Self {
        Self::Native(ScyllaDBArgumentNative::Timestamp(value))
    }
}

impl From<ScyllaDBArgumentNativeDate> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeDate) -> Self {
        Self::Native(ScyllaDBArgumentNative::Date(value))
    }
}

impl From<ScyllaDBArgumentNativeTime> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeTime) -> Self {
        Self::Native(ScyllaDBArgumentNative::Time(value))
    }
}

impl From<ScyllaDBArgumentNativeArrayText> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeArrayText) -> Self {
        Self::NativeArray(ScyllaDBArgumentNativeArray::Text(value))
    }
}

impl From<ScyllaDBArgumentNativeArrayBlob> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeArrayBlob) -> Self {
        Self::NativeArray(ScyllaDBArgumentNativeArray::Blob(value))
    }
}

#[cfg(feature = "bigdecimal-04")]
impl From<ScyllaDBArgumentNativeArrayDecimal> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeArrayDecimal) -> Self {
        Self::NativeArray(ScyllaDBArgumentNativeArray::Decimal(value))
    }
}

impl From<ScyllaDBArgumentNativeArrayTimestamp> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeArrayTimestamp) -> Self {
        Self::NativeArray(ScyllaDBArgumentNativeArray::Timestamp(value))
    }
}

impl From<ScyllaDBArgumentNativeArrayDate> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeArrayDate) -> Self {
        Self::NativeArray(ScyllaDBArgumentNativeArray::Date(value))
    }
}

impl From<ScyllaDBArgumentNativeArrayTime> for ScyllaDBArgument {
    fn from(value: ScyllaDBArgumentNativeArrayTime) -> Self {
        Self::NativeArray(ScyllaDBArgumentNativeArray::Time(value))
    }
}

impl SerializeValue for ScyllaDBArgumentNativeText {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Text(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::ArcStr(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::Secrecy08(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-10")]
            Self::Secrecy10(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeBlob {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Blob(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::Secrecy08(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-10")]
            Self::Secrecy10(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

#[cfg(feature = "bigdecimal-04")]
impl SerializeValue for ScyllaDBArgumentNativeDecimal {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Decimal(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeTimestamp {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Timestamp(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeDate {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Date(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeTime {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Time(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNative {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Boolean(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TinyInt(value) => serialize_counter_compatible(value, typ, writer),
            Self::SmallInt(value) => serialize_counter_compatible(value, typ, writer),
            Self::Int(value) => serialize_counter_compatible(value, typ, writer),
            Self::BigInt(value) => serialize_counter_compatible(value, typ, writer),
            Self::Float(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Double(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Text(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Blob(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Uuid(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Timeuuid(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Inet(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Duration(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "bigdecimal-04")]
            Self::Decimal(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Timestamp(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Date(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Time(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

fn serialize_counter_compatible<'b, T>(
    value: &T,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError>
where
    T: Copy + Into<i64> + SerializeValue,
{
    if ColumnType::Native(NativeType::Counter) == *typ {
        <_ as SerializeValue>::serialize(&Counter((*value).into()), typ, writer)
    } else {
        <_ as SerializeValue>::serialize(value, typ, writer)
    }
}

impl SerializeValue for ScyllaDBArgumentNativeArrayText {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Text(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::Secrecy08(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-10")]
            Self::Secrecy10(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeArrayBlob {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Blob(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-08")]
            Self::Secrecy08(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "secrecy-10")]
            Self::Secrecy10(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

#[cfg(feature = "bigdecimal-04")]
impl SerializeValue for ScyllaDBArgumentNativeArrayDecimal {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Decimal(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeArrayTimestamp {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Timestamp(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeArrayDate {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Date(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeArrayTime {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Time(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "time-03")]
            Self::Time03(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "chrono-04")]
            Self::Chrono04(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
}

impl SerializeValue for ScyllaDBArgumentNativeArray {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self {
            Self::Boolean(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::TinyInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::SmallInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Int(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::BigInt(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Float(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Double(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Text(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Blob(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Uuid(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Timeuuid(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Inet(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Duration(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            #[cfg(feature = "bigdecimal-04")]
            Self::Decimal(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Timestamp(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Date(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Time(value) => <_ as SerializeValue>::serialize(value, typ, writer),
        }
    }
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
            Self::Native(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::NativeArray(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Map(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::Tuple(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UserDefinedType(value) => <_ as SerializeValue>::serialize(value, typ, writer),
            Self::UserDefinedTypeArray(value) => {
                <_ as SerializeValue>::serialize(value, typ, writer)
            }
        }
    }
}
