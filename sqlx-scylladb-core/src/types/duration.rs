use scylla::value::CqlDuration;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(
    CqlDuration,
    ScyllaDBTypeInfo::Duration,
    ScyllaDBArgument::Duration
);

impl_array_type!(
    CqlDuration,
    ScyllaDBTypeInfo::DurationArray,
    ScyllaDBArgument::DurationArray
);

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use scylla::{
        cluster::metadata::{CollectionType, ColumnType, NativeType},
        value::CqlDuration,
    };

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
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
            ScyllaDBTypeInfo::Duration,
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
            ScyllaDBTypeInfo::DurationArray,
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
