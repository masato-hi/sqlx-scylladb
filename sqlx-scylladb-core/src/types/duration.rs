use scylla::value::CqlDuration;

use crate::{
    ScyllaDBTypeInfo, ScyllaDBTypeInfoNative, ScyllaDBTypeInfoNativeArray,
    arguments::ScyllaDBArgument,
};

impl_native_type!(
    CqlDuration,
    ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Duration),
    ScyllaDBArgument::Duration
);

impl_native_array_type!(
    CqlDuration,
    ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::Duration),
    ScyllaDBArgument::DurationArray
);

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use scylla::{
        cluster::metadata::{CollectionType, ColumnType, NativeType},
        value::CqlDuration,
    };

    use sqlx_core::{decode::Decode, encode::Encode, error::BoxDynError, ext::ustr::UStr};

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBTypeInfoNative,
        ScyllaDBTypeInfoNativeArray, ScyllaDBValueRef, types::serialize_value,
    };

    #[test]
    fn it_can_encode_duration() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            CqlDuration {
                months: 1,
                days: 15,
                nanoseconds: 300000000,
            },
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [
                CqlDuration {
                    months: 1,
                    days: 15,
                    nanoseconds: 300000000,
                },
                CqlDuration {
                    months: 2,
                    days: 16,
                    nanoseconds: 400000000,
                },
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            &[
                CqlDuration {
                    months: 1,
                    days: 15,
                    nanoseconds: 300000000,
                },
                CqlDuration {
                    months: 2,
                    days: 16,
                    nanoseconds: 400000000,
                },
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            vec![
                CqlDuration {
                    months: 1,
                    days: 15,
                    nanoseconds: 300000000,
                },
                CqlDuration {
                    months: 2,
                    days: 16,
                    nanoseconds: 400000000,
                },
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![
                CqlDuration {
                    months: 1,
                    days: 15,
                    nanoseconds: 300000000,
                },
                CqlDuration {
                    months: 2,
                    days: 16,
                    nanoseconds: 400000000,
                },
            ]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![
                CqlDuration {
                    months: 1,
                    days: 15,
                    nanoseconds: 300000000,
                },
                CqlDuration {
                    months: 2,
                    days: 16,
                    nanoseconds: 400000000,
                },
            ]),
            &mut buf,
        )?;

        Ok(())
    }

    #[test]
    fn it_can_decode_duration() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Duration);
        let raw_value = serialize_value(
            &CqlDuration {
                months: 1,
                days: 15,
                nanoseconds: 300000000,
            },
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_duration"),
            ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Duration),
            &raw_value,
            &column_type,
        );
        let decoded: CqlDuration = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            CqlDuration {
                months: 1,
                days: 15,
                nanoseconds: 300000000,
            }
        );

        Ok(())
    }

    #[test]
    fn it_can_decode_duration_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Duration))),
        };
        let raw_value = serialize_value(
            &vec![
                CqlDuration {
                    months: 1,
                    days: 15,
                    nanoseconds: 300000000,
                },
                CqlDuration {
                    months: 2,
                    days: 16,
                    nanoseconds: 400000000,
                },
            ],
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_duration"),
            ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::Duration),
            &raw_value,
            &column_type,
        );
        let decoded: Vec<CqlDuration> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            [
                CqlDuration {
                    months: 1,
                    days: 15,
                    nanoseconds: 300000000,
                },
                CqlDuration {
                    months: 2,
                    days: 16,
                    nanoseconds: 400000000,
                },
            ]
        );

        Ok(())
    }
}
