use crate::{
    ScyllaDBTypeInfo, ScyllaDBTypeInfoNative, ScyllaDBTypeInfoNativeArray,
    arguments::{ScyllaDBArgumentNative, ScyllaDBArgumentNativeArray},
};

impl_native_type!(
    bool,
    ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Boolean),
    ScyllaDBArgumentNative::Boolean
);

impl_native_array_type!(
    bool,
    ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::Boolean),
    ScyllaDBArgumentNativeArray::Boolean
);

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx_core::ext::ustr::UStr;
    use sqlx_core::{decode::Decode, encode::Encode, error::BoxDynError};

    use crate::{ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBValueRef, types::serialize_value};

    #[test]
    fn it_can_encode_bool() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(true, &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([true, false], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[true, false], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![true, false], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![true, false]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![true, false]), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_bool() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Boolean);
        let raw_value = serialize_value(&true, &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_boolean"),
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: bool = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert!(decoded);

        Ok(())
    }

    #[test]
    fn it_can_decode_bool_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Boolean))),
        };
        let raw_value = serialize_value(&vec![true, false], &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_boolean"),
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<bool> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [true, false]);

        Ok(())
    }
}
