use scylla::value::CqlDate;

use crate::{ScyllaDBArgument, ScyllaDBTypeInfo};

impl_type!(CqlDate, ScyllaDBTypeInfo::Date, ScyllaDBArgument::CqlDate);

impl_array_type!(
    CqlDate,
    ScyllaDBTypeInfo::DateArray,
    ScyllaDBArgument::CqlDateArray
);

#[cfg(feature = "chrono-04")]
pub mod chrono {
    impl_type!(
        chrono_04::NaiveDate,
        crate::ScyllaDBTypeInfo::Date,
        crate::ScyllaDBArgument::ChronoNaiveDate
    );

    impl_array_type!(
        chrono_04::NaiveDate,
        crate::ScyllaDBTypeInfo::DateArray,
        crate::ScyllaDBArgument::ChronoNaiveDateArray
    );
}

#[cfg(feature = "time-03")]
pub mod time {
    impl_type!(
        time_03::Date,
        crate::ScyllaDBTypeInfo::Date,
        crate::ScyllaDBArgument::Date
    );

    impl_array_type!(
        time_03::Date,
        crate::ScyllaDBTypeInfo::DateArray,
        crate::ScyllaDBArgument::DateArray
    );
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use scylla::{
        cluster::metadata::{CollectionType, ColumnType, NativeType},
        value::CqlDate,
    };

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
    };

    #[test]
    fn it_can_encode_date() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(CqlDate(20330), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([CqlDate(20330), CqlDate(13149)], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[CqlDate(20330), CqlDate(13149)], &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(vec![CqlDate(20330), CqlDate(13149)], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![CqlDate(20330), CqlDate(13149)]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![CqlDate(20330), CqlDate(13149)]),
            &mut buf,
        )?;

        Ok(())
    }

    #[test]
    fn it_can_decode_date() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Date);
        let raw_value = serialize_value(&CqlDate(20330), &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_date"),
            ScyllaDBTypeInfo::Date,
            &raw_value,
            &column_type,
        );
        let decoded: CqlDate = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, CqlDate(20330));

        Ok(())
    }

    #[test]
    fn it_can_decode_date_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Date))),
        };
        let raw_value = serialize_value(&vec![CqlDate(20330), CqlDate(13149)], &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_date"),
            ScyllaDBTypeInfo::DateArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<CqlDate> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [CqlDate(20330), CqlDate(13149),]);

        Ok(())
    }

    #[cfg(feature = "chrono-04")]
    mod chrono {
        use std::{rc::Rc, sync::Arc};

        use chrono_04::NaiveDate;
        use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

        use sqlx::{Decode, Encode, error::BoxDynError};
        use sqlx_core::ext::ustr::UStr;

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

        #[test]
        fn it_can_encode_chrono_naive_date() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [
                    NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
                    NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                &[
                    NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
                    NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![
                    NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
                    NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Rc::new(vec![
                    NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
                    NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
                ]),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Arc::new(vec![
                    NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
                    NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
                ]),
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_chrono_naive_date() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Date);
            let raw_value =
                serialize_value(&NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(), &column_type)?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_date"),
                ScyllaDBTypeInfo::Date,
                &raw_value,
                &column_type,
            );
            let decoded: NaiveDate = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded.to_string(), "2025-08-31");

            Ok(())
        }

        #[test]
        fn it_can_decode_chrono_naive_date_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Date))),
            };
            let raw_value = serialize_value(
                &vec![
                    NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
                    NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
                ],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_date"),
                ScyllaDBTypeInfo::DateArray,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<NaiveDate> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(
                decoded,
                [
                    NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
                    NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
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
        use time_03::{Date, Month::August, Month::January};

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

        #[test]
        fn it_can_encode_date_date() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Date::from_calendar_date(2025, August, 31)?,
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [
                    Date::from_calendar_date(2025, August, 31)?,
                    Date::from_calendar_date(2006, January, 2)?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                &[
                    Date::from_calendar_date(2025, August, 31)?,
                    Date::from_calendar_date(2006, January, 2)?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![
                    Date::from_calendar_date(2025, August, 31)?,
                    Date::from_calendar_date(2006, January, 2)?,
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Rc::new(vec![
                    Date::from_calendar_date(2025, August, 31)?,
                    Date::from_calendar_date(2006, January, 2)?,
                ]),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Arc::new(vec![
                    Date::from_calendar_date(2025, August, 31)?,
                    Date::from_calendar_date(2006, January, 2)?,
                ]),
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_date_date() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Date);
            let raw_value =
                serialize_value(&Date::from_calendar_date(2025, August, 31)?, &column_type)?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_date"),
                ScyllaDBTypeInfo::Date,
                &raw_value,
                &column_type,
            );
            let decoded: Date = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded, Date::from_calendar_date(2025, August, 31)?,);

            Ok(())
        }

        #[test]
        fn it_can_decode_date_date_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Date))),
            };
            let raw_value = serialize_value(
                &vec![
                    Date::from_calendar_date(2025, August, 31)?,
                    Date::from_calendar_date(2006, January, 2)?,
                ],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_date"),
                ScyllaDBTypeInfo::DateArray,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<Date> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(
                decoded,
                [
                    Date::from_calendar_date(2025, August, 31)?,
                    Date::from_calendar_date(2006, January, 2)?,
                ]
            );

            Ok(())
        }
    }
}
