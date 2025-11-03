use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(f32, ScyllaDBTypeInfo::Float, ScyllaDBArgument::Float);

impl_array_type!(
    f32,
    ScyllaDBTypeInfo::FloatArray,
    ScyllaDBArgument::FloatArray
);

impl_type!(f64, ScyllaDBTypeInfo::Double, ScyllaDBArgument::Double);

impl_array_type!(
    f64,
    ScyllaDBTypeInfo::DoubleArray,
    ScyllaDBArgument::DoubleArray
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
    fn it_can_encode_float() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(117.5f32, &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([11.5f32, 4.25], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[11.5f32, 4.25], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![11.5f32, 4.25], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![11.5f32, 4.25]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![11.5f32, 4.25]), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_float() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Float);
        let raw_value = serialize_value(&117.5f32, &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_float"),
            ScyllaDBTypeInfo::Float,
            &raw_value,
            &column_type,
        );
        let decoded: f32 = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, 117.5f32);

        Ok(())
    }

    #[test]
    fn it_can_decode_float_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Float))),
        };
        let raw_value = serialize_value(&vec![11.5f32, 4.25], &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_float"),
            ScyllaDBTypeInfo::FloatArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<f32> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [11.5f32, 4.25]);

        Ok(())
    }

    #[test]
    fn it_can_encode_double() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(117.5f64, &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([11.5f64, 4.25], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[11.5f64, 4.25], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![11.5f64, 4.25], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![11.5f64, 4.25]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![11.5f64, 4.25]), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_double() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Double);
        let raw_value = serialize_value(&117.5f64, &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_float"),
            ScyllaDBTypeInfo::Double,
            &raw_value,
            &column_type,
        );
        let decoded: f64 = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, 117.5f64);

        Ok(())
    }

    #[test]
    fn it_can_decode_double_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Double))),
        };
        let raw_value = serialize_value(&vec![11.5f64, 4.25], &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_float"),
            ScyllaDBTypeInfo::DoubleArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<f64> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [11.5f64, 4.25]);

        Ok(())
    }
}
