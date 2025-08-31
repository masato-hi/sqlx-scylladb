use std::borrow::Cow;

use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

use crate::{
    ScyllaDB, ScyllaDBHasArrayType, ScyllaDBTypeInfo, ScyllaDBValueRef,
    arguments::{ScyllaDBArgument, ScyllaDBArgumentBuffer},
};

impl Type<ScyllaDB> for &str {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl<'r> Encode<'r, ScyllaDB> for &'r str {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer<'r>) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Cow::Borrowed(self));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, ScyllaDB> for &'r str {
    fn decode(value: ScyllaDBValueRef<'r>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl Type<ScyllaDB> for String {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl Encode<'_, ScyllaDB> for String {
    fn encode(self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Cow::Owned(self));
        buf.push(argument);

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Cow::Owned(self.clone()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for String {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl ScyllaDBHasArrayType for &str {
    fn array_type_info() -> crate::ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::TextArray
    }
}

impl<'r> Encode<'r, ScyllaDB> for &'r [&'r str] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer<'r>) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::TextRefArray(self);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl<'r, const N: usize> Encode<'r, ScyllaDB> for &'r [&'r str; N] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer<'r>) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::TextRefArray(*self);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl_array_type!(
    String,
    ScyllaDBTypeInfo::TextArray,
    ScyllaDBArgument::TextArray
);
