use scylla::{deserialize::value::DeserializeValue, serialize::value::SerializeValue};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    ext::ustr::UStr,
};

use crate::{ScyllaDB, ScyllaDBArgument, ScyllaDBArgumentBuffer};

pub trait UserDefinedType<'r>:
    SerializeValue + DeserializeValue<'r, 'r> + Clone + Send + Sync
{
    #![allow(missing_docs)]
    fn type_name() -> UStr;
    fn box_cloned(&self) -> Box<dyn SerializeValue + Send + Sync>
    where
        Self: 'static,
    {
        Box::from(self.clone())
    }
}

impl<'r, T> Decode<'r, ScyllaDB> for Vec<T>
where
    T: UserDefinedType<'r>,
{
    fn decode(
        value: <ScyllaDB as sqlx_core::database::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx_core::error::BoxDynError> {
        let value: Self = value.deserialize()?;
        Ok(value)
    }
}

impl<'r, T> Encode<'_, ScyllaDB> for [T]
where
    T: UserDefinedType<'r> + Clone + 'static,
{
    fn encode_by_ref(
        &self,
        buf: &mut ScyllaDBArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        let mut values = Vec::with_capacity(self.len());
        for value in self {
            values.push(value.box_cloned());
        }
        let argument = ScyllaDBArgument::UserDefinedTypeArray(values);
        buf.push(argument);
        Ok(IsNull::No)
    }
}

impl<'r, T, const N: usize> Encode<'_, ScyllaDB> for [T; N]
where
    T: UserDefinedType<'r> + Clone + 'static,
{
    fn encode_by_ref(
        &self,
        buf: &mut ScyllaDBArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}

impl<'r, T> Encode<'_, ScyllaDB> for &[T]
where
    T: UserDefinedType<'r> + Clone + 'static,
{
    fn encode_by_ref(
        &self,
        buf: &mut ScyllaDBArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(*self, buf)
    }
}

impl<'r, T> Encode<'_, ScyllaDB> for Vec<T>
where
    T: UserDefinedType<'r> + Clone + 'static,
{
    fn encode_by_ref(
        &self,
        buf: &mut ScyllaDBArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}
