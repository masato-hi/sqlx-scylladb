use std::{ops::Deref, rc::Rc, sync::Arc};

use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

use crate::{
    ScyllaDB, ScyllaDBTypeInfo, ScyllaDBValueRef,
    arguments::{ScyllaDBArgument, ScyllaDBArgumentBuffer},
};

impl<const N: usize> Type<ScyllaDB> for [u8; N] {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl Type<ScyllaDB> for [u8] {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl Type<ScyllaDB> for Vec<u8> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl Decode<'_, ScyllaDB> for Vec<u8> {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for [u8; N] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}

impl Encode<'_, ScyllaDB> for [u8] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Blob(self.to_vec());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Encode<'_, ScyllaDB> for &[u8] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(*self, buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Rc<[u8; N]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Arc<[u8; N]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Rc<&[u8; N]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Arc<&[u8; N]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Rc<&[u8]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Arc<&[u8]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Rc<[u8]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Arc<[u8]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Vec<u8> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Rc<Vec<u8>> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Arc<Vec<u8>> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}

impl<const N: usize, const M: usize> Type<ScyllaDB> for [[u8; N]; M] {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl<const N: usize> Type<ScyllaDB> for [[u8; N]] {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Blob
    }
}

impl Type<ScyllaDB> for Vec<Vec<u8>> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::BlobArray
    }
}

impl Decode<'_, ScyllaDB> for Vec<Vec<u8>> {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl<const N: usize, const M: usize> Encode<'_, ScyllaDB> for [[u8; N]; M] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}

impl<const N: usize, const M: usize> Encode<'_, ScyllaDB> for Rc<[[u8; N]; M]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize, const M: usize> Encode<'_, ScyllaDB> for Arc<[[u8; N]; M]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for [[u8; N]] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut blobs = Vec::with_capacity(self.len());
        for blob in self.iter() {
            blobs.push(blob.to_vec());
        }
        let argument = ScyllaDBArgument::BlobArray(blobs);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Rc<[[u8; N]]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Arc<[[u8; N]]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for &[[u8; N]] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(*self, buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Rc<&[[u8; N]]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Arc<&[[u8; N]]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for [&[u8]; N] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut blobs = Vec::with_capacity(self.len());
        for blob in self.iter() {
            blobs.push(blob.to_vec());
        }
        let argument = ScyllaDBArgument::BlobArray(blobs);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Rc<[&[u8]; N]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Arc<[&[u8]; N]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for [&[u8]] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut blobs = Vec::with_capacity(self.len());
        for blob in self.iter() {
            blobs.push(blob.to_vec());
        }
        let argument = ScyllaDBArgument::BlobArray(blobs);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Encode<'_, ScyllaDB> for Rc<[&[u8]]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Arc<[&[u8]]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for &[&[u8]] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(*self, buf)
    }
}

impl Encode<'_, ScyllaDB> for Rc<&[&[u8]]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Arc<&[&[u8]]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for [Vec<u8>; N] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Rc<[Vec<u8>; N]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for Arc<[Vec<u8>; N]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for [Vec<u8>] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut blobs = Vec::with_capacity(self.len());
        for blob in self.iter() {
            blobs.push(blob.to_vec());
        }
        let argument = ScyllaDBArgument::BlobArray(blobs);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Encode<'_, ScyllaDB> for Rc<[Vec<u8>]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Arc<[Vec<u8>]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for &[Vec<u8>] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(*self, buf)
    }
}

impl Encode<'_, ScyllaDB> for Rc<&[Vec<u8>]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Arc<&[Vec<u8>]> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
    }
}

impl Encode<'_, ScyllaDB> for Vec<Vec<u8>> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::BlobArray(self.clone());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Encode<'_, ScyllaDB> for Rc<Vec<Vec<u8>>> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::BlobArray(self.to_vec());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Encode<'_, ScyllaDB> for Arc<Vec<Vec<u8>>> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::BlobArray(self.to_vec());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

#[cfg(feature = "secrecy-08")]
mod secrecy {
    use std::{rc::Rc, sync::Arc};

    use secrecy_08::SecretVec;
    use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

    use crate::{
        ScyllaDB, ScyllaDBTypeInfo, ScyllaDBValueRef,
        arguments::{ScyllaDBArgument, ScyllaDBArgumentBuffer},
    };

    impl Type<ScyllaDB> for SecretVec<u8> {
        fn type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::Blob
        }
    }

    impl Decode<'_, ScyllaDB> for SecretVec<u8> {
        fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
            let val: Self = value.deserialize()?;
            Ok(val)
        }
    }

    impl Encode<'_, ScyllaDB> for SecretVec<u8> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            use secrecy_08::ExposeSecret;

            let value = self.expose_secret().to_vec();
            let value = SecretVec::new(value);
            let argument = ScyllaDBArgument::SecretBlob(value);
            buf.push(argument);

            Ok(IsNull::No)
        }
    }

    impl Encode<'_, ScyllaDB> for Rc<SecretVec<u8>> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            use ::std::ops::Deref;

            <_ as ::sqlx::Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
        }
    }

    impl Encode<'_, ScyllaDB> for Arc<SecretVec<u8>> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            use ::std::ops::Deref;

            <_ as ::sqlx::Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
        }
    }

    impl<const N: usize> Type<ScyllaDB> for [SecretVec<u8>; N] {
        fn type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::Blob
        }
    }

    impl Type<ScyllaDB> for [SecretVec<u8>] {
        fn type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::Blob
        }
    }

    impl Type<ScyllaDB> for Vec<SecretVec<u8>> {
        fn type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::BlobArray
        }
    }

    impl Decode<'_, ScyllaDB> for Vec<SecretVec<u8>> {
        fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
            let val: Self = value.deserialize()?;
            Ok(val)
        }
    }

    impl<const N: usize> Encode<'_, ScyllaDB> for [SecretVec<u8>; N] {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <_ as ::sqlx::Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
        }
    }

    impl Encode<'_, ScyllaDB> for [SecretVec<u8>] {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            use secrecy_08::ExposeSecret;

            let mut items = Vec::with_capacity(self.len());
            for value in self.iter() {
                let value = value.expose_secret();
                let item = SecretVec::new(value.to_vec());
                items.push(item);
            }
            let argument = ScyllaDBArgument::SecretBlobArray(items);
            buf.push(argument);

            Ok(IsNull::No)
        }
    }

    impl Encode<'_, ScyllaDB> for &[SecretVec<u8>] {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <_ as ::sqlx::Encode<'_, ScyllaDB>>::encode_by_ref(*self, buf)
        }
    }

    impl Encode<'_, ScyllaDB> for Vec<SecretVec<u8>> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <_ as ::sqlx::Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
        }
    }

    impl Encode<'_, ScyllaDB> for Rc<Vec<SecretVec<u8>>> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            use ::std::ops::Deref;

            <_ as ::sqlx::Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
        }
    }

    impl Encode<'_, ScyllaDB> for Arc<Vec<SecretVec<u8>>> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            use ::std::ops::Deref;

            <_ as ::sqlx::Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
    };

    #[test]
    fn it_can_encode_blob() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        // [u8; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([0x00u8, 0x61, 0x73, 0x6d], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new([0x00u8, 0x61, 0x73, 0x6d]), &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(Arc::new([0x00u8, 0x61, 0x73, 0x6d]), &mut buf)?;

        // &[u8; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[0x00u8, 0x61, 0x73, 0x6d], &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(&[0x00u8, 0x61, 0x73, 0x6d]), &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(&[0x00u8, 0x61, 0x73, 0x6d]), &mut buf)?;

        // &[u8]
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode([0x00u8, 0x61, 0x73, 0x6d].as_slice(), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([0x00u8, 0x61, 0x73, 0x6d].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([0x00u8, 0x61, 0x73, 0x6d].as_slice()),
            &mut buf,
        )?;

        // Vec<u8>
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![0x00u8, 0x61, 0x73, 0x6d], &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![0x00u8, 0x61, 0x73, 0x6d]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![0x00u8, 0x61, 0x73, 0x6d]),
            &mut buf,
        )?;

        // [[u8; N]; M]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [[0x00u8, 0x61, 0x73, 0x6d], [0x00u8, 0x61, 0x73, 0x6e]],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([[0x00u8, 0x61, 0x73, 0x6d], [0x00u8, 0x61, 0x73, 0x6e]]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([[0x00u8, 0x61, 0x73, 0x6d], [0x00u8, 0x61, 0x73, 0x6e]]),
            &mut buf,
        )?;

        // &[[u8; N]]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [[0x00u8, 0x61, 0x73, 0x6d], [0x00u8, 0x61, 0x73, 0x6e]].as_slice(),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([[0x00u8, 0x61, 0x73, 0x6d], [0x00u8, 0x61, 0x73, 0x6e]].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([[0x00u8, 0x61, 0x73, 0x6d], [0x00u8, 0x61, 0x73, 0x6e]].as_slice()),
            &mut buf,
        )?;

        // [&[u8; N]]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [
                [0x00u8, 0x61, 0x73, 0x6d].as_slice(),
                [0x00u8, 0x61, 0x73, 0x6e].as_slice(),
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([
                [0x00u8, 0x61, 0x73, 0x6d].as_slice(),
                [0x00u8, 0x61, 0x73, 0x6e].as_slice(),
            ]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([
                [0x00u8, 0x61, 0x73, 0x6d].as_slice(),
                [0x00u8, 0x61, 0x73, 0x6e].as_slice(),
            ]),
            &mut buf,
        )?;

        // &[&[u8; N]]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [
                [0x00u8, 0x61, 0x73, 0x6d].as_slice(),
                [0x00u8, 0x61, 0x73, 0x6e].as_slice(),
            ]
            .as_slice(),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(
                [
                    [0x00u8, 0x61, 0x73, 0x6d].as_slice(),
                    [0x00u8, 0x61, 0x73, 0x6e].as_slice(),
                ]
                .as_slice(),
            ),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(
                [
                    [0x00u8, 0x61, 0x73, 0x6d].as_slice(),
                    [0x00u8, 0x61, 0x73, 0x6e].as_slice(),
                ]
                .as_slice(),
            ),
            &mut buf,
        )?;

        // [Vec<u8>; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ]),
            &mut buf,
        )?;

        // &[Vec<u8>; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ]
            .as_slice(),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(
                [
                    vec![0x00u8, 0x61, 0x73, 0x6d],
                    vec![0x00u8, 0x61, 0x73, 0x6e],
                ]
                .as_slice(),
            ),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(
                [
                    vec![0x00u8, 0x61, 0x73, 0x6d],
                    vec![0x00u8, 0x61, 0x73, 0x6e],
                ]
                .as_slice(),
            ),
            &mut buf,
        )?;

        // Vec<Vec<u8>>
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            vec![
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ]),
            &mut buf,
        )?;

        Ok(())
    }

    #[test]
    fn it_can_decode_blob() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Blob);
        let raw_value = serialize_value(&[0x00u8, 0x61, 0x73, 0x6d], &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_blob"),
            ScyllaDBTypeInfo::Blob,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<u8> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, vec![0x00u8, 0x61, 0x73, 0x6d]);

        Ok(())
    }

    #[test]
    fn it_can_decode_blob_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Blob))),
        };
        let raw_value = serialize_value(
            &vec![
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ],
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_blob"),
            ScyllaDBTypeInfo::BlobArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<Vec<u8>> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            vec![
                vec![0x00u8, 0x61, 0x73, 0x6d],
                vec![0x00u8, 0x61, 0x73, 0x6e],
            ]
        );

        Ok(())
    }

    #[cfg(feature = "secrecy-08")]
    mod secrecy {
        use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};
        use secrecy_08::{ExposeSecret, SecretVec};
        use sqlx::{Decode, Encode, error::BoxDynError};
        use sqlx_core::ext::ustr::UStr;

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

        #[test]
        fn it_can_encode_secret_blob() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                SecretVec::new(vec![0x00u8, 0x61, 0x73, 0x6d]),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [SecretVec::new(vec![0x00u8, 0x61, 0x73, 0x6d])],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [SecretVec::new(vec![0x00u8, 0x61, 0x73, 0x6d])].as_slice(),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![SecretVec::new(vec![0x00u8, 0x61, 0x73, 0x6d])],
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_secret_blob() -> Result<(), BoxDynError> {
            use secrecy_08::ExposeSecret;

            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Blob);
            let raw_value = serialize_value(
                &SecretVec::from(vec![0x00u8, 0x61, 0x73, 0x6d]),
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_blob"),
                ScyllaDBTypeInfo::Blob,
                &raw_value,
                &column_type,
            );
            let decoded: SecretVec<u8> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded.expose_secret(), &vec![0x00u8, 0x61, 0x73, 0x6d]);

            Ok(())
        }

        #[test]
        fn it_can_decode_secret_blob_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Blob))),
            };
            let raw_value = serialize_value(
                &vec![
                    SecretVec::from(vec![0x00u8, 0x61, 0x73, 0x6d]),
                    SecretVec::from(vec![0x00u8, 0x61, 0x73, 0x6e]),
                ],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_blob"),
                ScyllaDBTypeInfo::BlobArray,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<SecretVec<u8>> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(
                decoded.get(0).unwrap().expose_secret(),
                &vec![0x00u8, 0x61, 0x73, 0x6d]
            );
            assert_eq!(
                decoded.get(1).unwrap().expose_secret(),
                &vec![0x00u8, 0x61, 0x73, 0x6e]
            );

            Ok(())
        }
    }
}
