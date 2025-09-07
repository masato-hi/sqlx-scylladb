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

impl<const N: usize> Type<ScyllaDB> for [u8; N] {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for [u8; N] {
    #[inline(always)]
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Blob(Arc::new(self.to_vec()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Encode<'_, ScyllaDB> for &[u8] {
    #[inline(always)]
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Blob(Arc::new(self.to_vec()));
        buf.push(argument);

        Ok(IsNull::No)
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

impl<'r> Encode<'r, ScyllaDB> for Arc<Vec<u8>> {
    #[inline(always)]
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Blob(self.clone());
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

impl<const N: usize, const M: usize> Type<ScyllaDB> for [[u8; N]; M] {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl<const N: usize, const M: usize> Encode<'_, ScyllaDB> for [[u8; N]; M] {
    #[inline(always)]
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut blobs = Vec::with_capacity(self.len());
        for blob in self.iter() {
            blobs.push(blob.to_vec());
        }
        let argument = ScyllaDBArgument::BlobArray(Arc::new(blobs));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Type<ScyllaDB> for Vec<Vec<u8>> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::BlobArray
    }
}

impl<'r> Encode<'r, ScyllaDB> for Vec<Vec<u8>> {
    fn encode(self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::BlobArray(Arc::new(self));
        buf.push(argument);

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::BlobArray(Arc::new(self.clone()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl<'r> Encode<'r, ScyllaDB> for Arc<Vec<Vec<u8>>> {
    #[inline(always)]
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::BlobArray(self.clone());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for Vec<Vec<u8>> {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}
