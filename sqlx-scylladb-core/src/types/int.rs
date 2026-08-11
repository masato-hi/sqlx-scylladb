use crate::{
    ScyllaDBTypeInfo, ScyllaDBTypeInfoNative, ScyllaDBTypeInfoNativeArray,
    arguments::{ScyllaDBArgumentNative, ScyllaDBArgumentNativeArray},
};

impl_native_type!(
    i8,
    ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::TinyInt),
    ScyllaDBArgumentNative::TinyInt
);

impl_native_array_type!(
    i8,
    ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::TinyInt),
    ScyllaDBArgumentNativeArray::TinyInt
);

impl_native_type!(
    i16,
    ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::SmallInt),
    ScyllaDBArgumentNative::SmallInt
);

impl_native_array_type!(
    i16,
    ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::SmallInt),
    ScyllaDBArgumentNativeArray::SmallInt
);

impl_native_type!(
    i32,
    ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Int),
    ScyllaDBArgumentNative::Int
);

impl_native_array_type!(
    i32,
    ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::Int),
    ScyllaDBArgumentNativeArray::Int
);

impl_native_type!(
    i64,
    ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::BigInt),
    ScyllaDBArgumentNative::BigInt
);

impl_native_array_type!(
    i64,
    ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::BigInt),
    ScyllaDBArgumentNativeArray::BigInt
);

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx_core::{decode::Decode, encode::Encode, error::BoxDynError};

    use crate::{ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBValueRef, types::serialize_value};

    #[test]
    fn it_can_encode_tinyint() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(11i8, &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([11i8, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[11i8, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![11i8, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![11i8, 4]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![11i8, 4]), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_tinyint() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::TinyInt);
        let raw_value = serialize_value(&11i8, &column_type)?;

        let value = ScyllaDBValueRef::new(
            "my_tinyint",
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: i8 = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, 11i8);

        Ok(())
    }

    #[test]
    fn it_can_decode_tinyint_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::TinyInt))),
        };
        let raw_value = serialize_value(&vec![11i8, 4], &column_type)?;

        let value = ScyllaDBValueRef::new(
            "my_tinyint",
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<i8> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [11i8, 4]);

        Ok(())
    }

    #[test]
    fn it_can_encode_smallint() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(11i16, &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([11i16, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[11i16, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![11i16, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![11i16, 4]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![11i16, 4]), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_smallint() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::SmallInt);
        let raw_value = serialize_value(&11i16, &column_type)?;

        let value = ScyllaDBValueRef::new(
            "my_smallint",
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: i16 = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, 11i16);

        Ok(())
    }

    #[test]
    fn it_can_decode_smallint_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::SmallInt))),
        };
        let raw_value = serialize_value(&vec![11i16, 4], &column_type)?;

        let value = ScyllaDBValueRef::new(
            "my_smallint",
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<i16> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [11i16, 4]);

        Ok(())
    }

    #[test]
    fn it_can_encode_int() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(11i32, &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([11i32, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[11i32, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![11i32, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![11i32, 4]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![11i32, 4]), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_int() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Int);
        let raw_value = serialize_value(&11i32, &column_type)?;

        let value = ScyllaDBValueRef::new(
            "my_int",
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: i32 = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, 11i32);

        Ok(())
    }

    #[test]
    fn it_can_decode_int_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
        };
        let raw_value = serialize_value(&vec![11i32, 4], &column_type)?;

        let value = ScyllaDBValueRef::new(
            "my_int",
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<i32> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [11i32, 4]);

        Ok(())
    }

    #[test]
    fn it_can_encode_bigint() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(11i64, &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([11i64, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[11i64, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![11i64, 4], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![11i64, 4]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![11i64, 4]), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_bigint() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::BigInt);
        let raw_value = serialize_value(&11i64, &column_type)?;

        let value = ScyllaDBValueRef::new(
            "my_bigint",
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: i64 = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, 11i64);

        Ok(())
    }

    #[test]
    fn it_can_decode_bigint_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::BigInt))),
        };
        let raw_value = serialize_value(&vec![11i64, 4], &column_type)?;

        let value = ScyllaDBValueRef::new(
            "my_bigint",
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<i64> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [11i64, 4]);

        Ok(())
    }
}
