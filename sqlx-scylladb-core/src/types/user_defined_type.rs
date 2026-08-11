use scylla::{deserialize::value::DeserializeValue, serialize::value::SerializeValue};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
};

use crate::{ScyllaDB, ScyllaDBArgument, ScyllaDBArgumentBuffer};

#[allow(missing_docs)]
pub trait UserDefinedType<'r>:
    SerializeValue + DeserializeValue<'r, 'r> + Clone + Send + Sync
{
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
    T: UserDefinedType<'r> + 'static,
{
    fn encode_by_ref(
        &self,
        buf: &mut ScyllaDBArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        let argument = ScyllaDBArgument::UserDefinedTypeArray(Box::new(self.to_vec()));
        buf.push(argument);
        Ok(IsNull::No)
    }
}

impl<'r, T, const N: usize> Encode<'_, ScyllaDB> for [T; N]
where
    Self: Clone,
    T: UserDefinedType<'r> + 'static,
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
    Self: Clone,
    T: UserDefinedType<'r> + 'static,
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
    fn encode(
        self,
        buf: &mut ScyllaDBArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(ScyllaDBArgument::UserDefinedTypeArray(Box::new(self)));
        Ok(IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut ScyllaDBArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}
