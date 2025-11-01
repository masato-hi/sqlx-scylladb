use scylla::{deserialize::value::DeserializeValue, serialize::value::SerializeValue};
use sqlx::{Decode, Encode, encode::IsNull};
use sqlx_core::ext::ustr::UStr;

use crate::{
    ScyllaDB, ScyllaDBArgument, ScyllaDBArgumentBuffer, ScyllaDBHasArrayType, ScyllaDBTypeInfo,
};

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

impl<'r, T> ScyllaDBHasArrayType for T
where
    T: UserDefinedType<'r>,
{
    fn array_type_info() -> crate::ScyllaDBTypeInfo {
        let ty = T::type_name();
        let ty = UStr::new(&format!("{}[]", ty));
        ScyllaDBTypeInfo::UserDefinedTypeArray(ty)
    }
}

impl<'r, T> Encode<'_, ScyllaDB> for Vec<T>
where
    T: UserDefinedType<'r> + Clone + 'static,
{
    fn encode_by_ref(
        &self,
        buf: &mut ScyllaDBArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let mut values = Vec::with_capacity(self.len());
        for value in self {
            values.push(value.box_cloned());
        }
        let argument = ScyllaDBArgument::UserDefinedTypeArray(values);
        buf.push(argument);
        Ok(IsNull::No)
    }
}

impl<'r, T> Decode<'r, ScyllaDB> for Vec<T>
where
    T: UserDefinedType<'r>,
{
    fn decode(
        value: <ScyllaDB as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: Self = value.deserialize()?;
        Ok(value)
    }
}
