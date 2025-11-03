use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(i8, ScyllaDBTypeInfo::TinyInt, ScyllaDBArgument::TinyInt);

impl_array_type!(
    i8,
    ScyllaDBTypeInfo::TinyIntArray,
    ScyllaDBArgument::TinyIntArray
);

impl_type!(i16, ScyllaDBTypeInfo::SmallInt, ScyllaDBArgument::SmallInt);

impl_array_type!(
    i16,
    ScyllaDBTypeInfo::SmallIntArray,
    ScyllaDBArgument::SmallIntArray
);

impl_type!(i32, ScyllaDBTypeInfo::Int, ScyllaDBArgument::Int);

impl_array_type!(i32, ScyllaDBTypeInfo::IntArray, ScyllaDBArgument::IntArray);

impl_type!(i64, ScyllaDBTypeInfo::BigInt, ScyllaDBArgument::BigInt);

impl_array_type!(
    i64,
    ScyllaDBTypeInfo::BigIntArray,
    ScyllaDBArgument::BigIntArray
);

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
            UStr::new("my_tinyint"),
            ScyllaDBTypeInfo::TinyInt,
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
            UStr::new("my_tinyint"),
            ScyllaDBTypeInfo::TinyIntArray,
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
            UStr::new("my_smallint"),
            ScyllaDBTypeInfo::SmallInt,
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
            UStr::new("my_smallint"),
            ScyllaDBTypeInfo::SmallIntArray,
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
            UStr::new("my_int"),
            ScyllaDBTypeInfo::Int,
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
            UStr::new("my_int"),
            ScyllaDBTypeInfo::IntArray,
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
            UStr::new("my_bigint"),
            ScyllaDBTypeInfo::BigInt,
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
            UStr::new("my_bigint"),
            ScyllaDBTypeInfo::BigIntArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<i64> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [11i64, 4]);

        Ok(())
    }
}
