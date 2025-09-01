use std::sync::Arc;

use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

use crate::{
    ScyllaDB, ScyllaDBTypeInfo, ScyllaDBValueRef,
    arguments::{ScyllaDBArgument, ScyllaDBArgumentBuffer},
};

impl Type<ScyllaDB> for [u8] {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl<'r> Encode<'r, ScyllaDB> for &'r [u8] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Blob(Arc::new(self.to_vec()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, ScyllaDB> for &'r [u8] {
    fn decode(value: ScyllaDBValueRef<'r>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl Type<ScyllaDB> for Vec<u8> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl<'r> Encode<'r, ScyllaDB> for Vec<u8> {
    fn encode(self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Blob(Arc::new(self));
        buf.push(argument);

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Blob(Arc::new(self.clone()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for Vec<u8> {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}
