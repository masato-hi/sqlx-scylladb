use scylla::value::CqlTimestamp;

use crate::{ScyllaDBArgument, ScyllaDBTypeInfo};

impl_type!(
    CqlTimestamp,
    ScyllaDBTypeInfo::Timestamp,
    ScyllaDBArgument::CqlTimestamp
);

impl_array_type!(
    CqlTimestamp,
    ScyllaDBTypeInfo::TimestampArray,
    ScyllaDBArgument::CqlTimestampArray
);

#[cfg(feature = "chrono-04")]
pub mod chrono {
    impl_type!(
        chrono_04::DateTime<chrono_04::Utc>,
        crate::ScyllaDBTypeInfo::Timestamp,
        crate::ScyllaDBArgument::ChronoDateTimeUTC
    );

    impl_array_type!(
        chrono_04::DateTime<chrono_04::Utc>,
        crate::ScyllaDBTypeInfo::TimestampArray,
        crate::ScyllaDBArgument::ChronoDateTimeUTCArray
    );
}

#[cfg(feature = "time-03")]
pub mod time {
    impl_type!(
        time_03::OffsetDateTime,
        crate::ScyllaDBTypeInfo::Timestamp,
        crate::ScyllaDBArgument::OffsetDateTime
    );

    impl_array_type!(
        time_03::OffsetDateTime,
        crate::ScyllaDBTypeInfo::TimestampArray,
        crate::ScyllaDBArgument::OffsetDateTimeArray
    );
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use scylla::{
        cluster::metadata::{CollectionType, ColumnType, NativeType},
        value::CqlTimestamp,
    };

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
    };

    #[test]
    fn it_can_encode_timestamp() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(CqlTimestamp(1756625358255), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [CqlTimestamp(1756625358255), CqlTimestamp(1756625378304)],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            &[CqlTimestamp(1756625358255), CqlTimestamp(1756625378304)],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            vec![CqlTimestamp(1756625358255), CqlTimestamp(1756625378304)],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![
                CqlTimestamp(1756625358255),
                CqlTimestamp(1756625378304),
            ]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![
                CqlTimestamp(1756625358255),
                CqlTimestamp(1756625378304),
            ]),
            &mut buf,
        )?;

        Ok(())
    }

    #[test]
    fn it_can_decode_timestamp() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Timestamp);
        let raw_value = serialize_value(&CqlTimestamp(1756625358255), &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_timestamp"),
            ScyllaDBTypeInfo::Timestamp,
            &raw_value,
            &column_type,
        );
        let decoded: CqlTimestamp = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, CqlTimestamp(1756625358255));

        Ok(())
    }

    #[test]
    fn it_can_decode_timestamp_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Timestamp))),
        };
        let raw_value = serialize_value(
            &vec![CqlTimestamp(1756625358255), CqlTimestamp(1756625378304)],
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_timestamp"),
            ScyllaDBTypeInfo::TimestampArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<CqlTimestamp> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            [CqlTimestamp(1756625358255), CqlTimestamp(1756625378304)]
        );

        Ok(())
    }

    #[cfg(feature = "chrono-04")]
    mod chrono {
        use std::{rc::Rc, sync::Arc};

        use chrono_04::{DateTime, Utc};
        use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

        use sqlx::{Decode, Encode, error::BoxDynError};
        use sqlx_core::ext::ustr::UStr;

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

        #[test]
        fn it_can_encode_chrono_datetime_utc() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [
                    DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                    DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                &[
                    DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                    DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![
                    DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                    DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Rc::new(vec![
                    DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                    DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
                ]),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Arc::new(vec![
                    DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                    DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
                ]),
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_chrono_datetime_utc() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Timestamp);
            let raw_value = serialize_value(
                &DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_timestamp"),
                ScyllaDBTypeInfo::Timestamp,
                &raw_value,
                &column_type,
            );
            let decoded: DateTime<Utc> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded.to_rfc3339(), "2025-08-31T16:44:34+00:00");

            Ok(())
        }

        #[test]
        fn it_can_decode_chrono_datetime_utc_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Timestamp))),
            };
            let raw_value = serialize_value(
                &vec![
                    DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                    DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
                ],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_timestamp"),
                ScyllaDBTypeInfo::TimestampArray,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<DateTime<Utc>> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(
                decoded,
                [
                    DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
                    DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
                ]
            );

            Ok(())
        }
    }

    #[cfg(feature = "time-03")]
    mod time {
        use std::{rc::Rc, sync::Arc};

        use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

        use sqlx::{Decode, Encode, error::BoxDynError};
        use sqlx_core::ext::ustr::UStr;
        use time_03::OffsetDateTime;

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

        #[test]
        fn it_can_encode_time_offset_date_time() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                OffsetDateTime::from_unix_timestamp(1756626948)?,
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [
                    OffsetDateTime::from_unix_timestamp(1756626948)?,
                    OffsetDateTime::from_unix_timestamp(1756626953)?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                &[
                    OffsetDateTime::from_unix_timestamp(1756626948)?,
                    OffsetDateTime::from_unix_timestamp(1756626953)?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![
                    OffsetDateTime::from_unix_timestamp(1756626948)?,
                    OffsetDateTime::from_unix_timestamp(1756626953)?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Rc::new(vec![
                    OffsetDateTime::from_unix_timestamp(1756626948)?,
                    OffsetDateTime::from_unix_timestamp(1756626953)?,
                ]),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Arc::new(vec![
                    OffsetDateTime::from_unix_timestamp(1756626948)?,
                    OffsetDateTime::from_unix_timestamp(1756626953)?,
                ]),
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_time_offset_date_time() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Timestamp);
            let raw_value = serialize_value(
                &OffsetDateTime::from_unix_timestamp(1756626948)?,
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_timestamp"),
                ScyllaDBTypeInfo::Timestamp,
                &raw_value,
                &column_type,
            );
            let decoded: OffsetDateTime = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded, OffsetDateTime::from_unix_timestamp(1756626948)?);

            Ok(())
        }

        #[test]
        fn it_can_decode_time_offset_date_time_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Timestamp))),
            };
            let raw_value = serialize_value(
                &vec![
                    OffsetDateTime::from_unix_timestamp(1756626948)?,
                    OffsetDateTime::from_unix_timestamp(1756626953)?,
                ],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_timestamp"),
                ScyllaDBTypeInfo::TimestampArray,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<OffsetDateTime> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(
                decoded,
                [
                    OffsetDateTime::from_unix_timestamp(1756626948)?,
                    OffsetDateTime::from_unix_timestamp(1756626953)?,
                ]
            );

            Ok(())
        }
    }
}
