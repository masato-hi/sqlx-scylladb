use scylla::value::CqlTime;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(CqlTime, ScyllaDBTypeInfo::Time, ScyllaDBArgument::CqlTime);

impl_array_type!(
    CqlTime,
    ScyllaDBTypeInfo::TimeArray,
    ScyllaDBArgument::CqlTimeArray
);

#[cfg(feature = "chrono-04")]
pub mod chrono {
    impl_type!(
        chrono_04::NaiveTime,
        crate::ScyllaDBTypeInfo::Time,
        crate::ScyllaDBArgument::ChronoNaiveTime
    );

    impl_array_type!(
        chrono_04::NaiveTime,
        crate::ScyllaDBTypeInfo::TimeArray,
        crate::ScyllaDBArgument::ChronoNaiveTimeArray
    );
}

#[cfg(feature = "time-03")]
pub mod time {
    impl_type!(
        time_03::Time,
        crate::ScyllaDBTypeInfo::Time,
        crate::ScyllaDBArgument::Time
    );

    impl_array_type!(
        time_03::Time,
        crate::ScyllaDBTypeInfo::TimeArray,
        crate::ScyllaDBArgument::TimeArray
    );
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use scylla::{
        cluster::metadata::{CollectionType, ColumnType, NativeType},
        value::CqlTime,
    };

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
    };

    #[test]
    fn it_can_encode_time() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(CqlTime(27874), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([CqlTime(27874), CqlTime(21845)], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[CqlTime(27874), CqlTime(21845)], &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(vec![CqlTime(27874), CqlTime(21845)], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![CqlTime(27874), CqlTime(21845)]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![CqlTime(27874), CqlTime(21845)]),
            &mut buf,
        )?;

        Ok(())
    }

    #[test]
    fn it_can_decode_time() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Time);
        let raw_value = serialize_value(&CqlTime(27874), &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_time"),
            ScyllaDBTypeInfo::Time,
            &raw_value,
            &column_type,
        );
        let decoded: CqlTime = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, CqlTime(27874));

        Ok(())
    }

    #[test]
    fn it_can_decode_time_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Time))),
        };
        let raw_value = serialize_value(&vec![CqlTime(27874), CqlTime(21845)], &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_time"),
            ScyllaDBTypeInfo::TimeArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<CqlTime> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, [CqlTime(27874), CqlTime(21845)]);

        Ok(())
    }

    #[cfg(feature = "chrono-04")]
    mod chrono {
        use std::{rc::Rc, sync::Arc};

        use chrono_04::NaiveTime;
        use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

        use sqlx::{Decode, Encode, error::BoxDynError};
        use sqlx_core::ext::ustr::UStr;

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

        #[test]
        fn it_can_encode_chrono_naive_time() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [
                    NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
                    NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                &[
                    NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
                    NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![
                    NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
                    NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
                ],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Rc::new(vec![
                    NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
                    NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
                ]),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Arc::new(vec![
                    NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
                    NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
                ]),
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_chrono_naive_time() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Time);
            let raw_value =
                serialize_value(&NaiveTime::from_hms_opt(16, 44, 34).unwrap(), &column_type)?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_time"),
                ScyllaDBTypeInfo::Time,
                &raw_value,
                &column_type,
            );
            let decoded: NaiveTime = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded.to_string(), "16:44:34");

            Ok(())
        }

        #[test]
        fn it_can_decode_chrono_naive_time_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Time))),
            };
            let raw_value = serialize_value(
                &vec![
                    NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
                    NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
                ],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_time"),
                ScyllaDBTypeInfo::TimeArray,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<NaiveTime> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(
                decoded,
                [
                    NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
                    NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
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
        use time_03::Time;

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

        #[test]
        fn it_can_encode_time_time() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(Time::from_hms(16, 44, 34)?, &mut buf)?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [Time::from_hms(16, 44, 34)?, Time::from_hms(15, 04, 05)?],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                &[Time::from_hms(16, 44, 34)?, Time::from_hms(15, 04, 05)?],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![Time::from_hms(16, 44, 34)?, Time::from_hms(15, 04, 05)?],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Rc::new(vec![
                    Time::from_hms(16, 44, 34)?,
                    Time::from_hms(15, 04, 05)?,
                ]),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                Arc::new(vec![
                    Time::from_hms(16, 44, 34)?,
                    Time::from_hms(15, 04, 05)?,
                ]),
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_time_time() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Time);
            let raw_value = serialize_value(&Time::from_hms(16, 44, 34)?, &column_type)?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_time"),
                ScyllaDBTypeInfo::Time,
                &raw_value,
                &column_type,
            );
            let decoded: Time = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded, Time::from_hms(16, 44, 34)?);

            Ok(())
        }

        #[test]
        fn it_can_decode_time_time_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Time))),
            };
            let raw_value = serialize_value(
                &vec![Time::from_hms(16, 44, 34)?, Time::from_hms(15, 04, 05)?],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_time"),
                ScyllaDBTypeInfo::TimeArray,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<Time> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(
                decoded,
                [Time::from_hms(16, 44, 34)?, Time::from_hms(15, 04, 05)?]
            );

            Ok(())
        }
    }
}
