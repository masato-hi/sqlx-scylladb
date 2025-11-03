#[cfg(feature = "bigdecimal-04")]
pub mod bigdecimal {
    impl_type!(
        bigdecimal_04::BigDecimal,
        crate::ScyllaDBTypeInfo::Decimal,
        crate::ScyllaDBArgument::BigDecimal
    );

    impl_array_type!(
        bigdecimal_04::BigDecimal,
        crate::ScyllaDBTypeInfo::DecimalArray,
        crate::ScyllaDBArgument::BigDecimalArray
    );
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "bigdecimal-04")]
    mod bigdecimal {
        use std::{rc::Rc, str::FromStr, sync::Arc};

        use bigdecimal_04::BigDecimal;
        use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

        use sqlx::{Decode, Encode, error::BoxDynError};
        use sqlx_core::ext::ustr::UStr;

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

        #[test]
        fn it_can_encode_bigdecimal() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(BigDecimal::from_str("5e9")?, &mut buf)?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [
                    BigDecimal::from_str("123.45")?,
                    BigDecimal::from_str("5e9")?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                &[
                    BigDecimal::from_str("123.45")?,
                    BigDecimal::from_str("5e9")?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![
                    BigDecimal::from_str("123.45")?,
                    BigDecimal::from_str("5e9")?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Rc::new(vec![
                    BigDecimal::from_str("123.45")?,
                    BigDecimal::from_str("5e9")?,
                ]),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Arc::new(vec![
                    BigDecimal::from_str("123.45")?,
                    BigDecimal::from_str("5e9")?,
                ]),
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_bigdecimal() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Decimal);
            let raw_value = serialize_value(&BigDecimal::from_str("5e9")?, &column_type)?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_decimal"),
                ScyllaDBTypeInfo::Decimal,
                &raw_value,
                &column_type,
            );
            let decoded: BigDecimal = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded, BigDecimal::from_str("5e9")?,);

            Ok(())
        }

        #[test]
        fn it_can_decode_bigdecimal_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Decimal))),
            };
            let raw_value = serialize_value(
                &vec![
                    BigDecimal::from_str("123.45")?,
                    BigDecimal::from_str("5e9")?,
                ],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_decimal"),
                ScyllaDBTypeInfo::DecimalArray,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<BigDecimal> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(
                decoded,
                [
                    BigDecimal::from_str("123.45")?,
                    BigDecimal::from_str("5e9")?,
                ]
            );

            Ok(())
        }
    }
}
