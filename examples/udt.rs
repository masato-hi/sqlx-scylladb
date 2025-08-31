use scylla::{DeserializeValue, SerializeValue, serialize::value::SerializeValue};
use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

use sqlx_core::ext::ustr::UStr;
use sqlx_scylladb::{
    ScyllaDB, ScyllaDBTypeInfo, ScyllaDBValueRef, {ScyllaDBArgument, ScyllaDBArgumentBuffer},
};

#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub(crate) struct UDT {
    pub id: i64,
    pub name: String,
}

impl Type<ScyllaDB> for UDT {
    fn type_info() -> ScyllaDBTypeInfo {
        let type_name = UStr::new("udt");
        ScyllaDBTypeInfo::UserDefinedType(type_name)
    }
}

impl<'r> Encode<'r, ScyllaDB> for &'r UDT {
    fn encode(self, buf: &mut ScyllaDBArgumentBuffer<'r>) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::UserDefinedType(self);
        buf.push(argument);

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer<'r>) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::UserDefinedType(*self);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for UDT {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let value: Self = value.deserialize()?;
        Ok(value)
    }
}

struct UDTArray(Vec<UDT>);

impl Type<ScyllaDB> for UDTArray {
    fn type_info() -> ScyllaDBTypeInfo {
        let type_name = UStr::new("udt");
        ScyllaDBTypeInfo::UserDefinedTypeArray(type_name)
    }
}

impl<'r> Encode<'r, ScyllaDB> for &'r UDTArray {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer<'r>) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::UserDefinedTypeArray(*self);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl SerializeValue for UDTArray {
    fn serialize<'b>(
        &self,
        typ: &scylla::cluster::metadata::ColumnType,
        writer: scylla_cql::serialize::CellWriter<'b>,
    ) -> Result<scylla::serialize::writers::WrittenCellProof<'b>, scylla::errors::SerializationError>
    {
        <_ as SerializeValue>::serialize(&self.0, typ, writer)
    }
}

impl Decode<'_, ScyllaDB> for UDTArray {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let value: Vec<UDT> = value.deserialize()?;
        Ok(Self(value))
    }
}

fn main() {}
