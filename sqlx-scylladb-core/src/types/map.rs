use std::net::IpAddr;

use uuid::Uuid;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

// String
impl_map_type!(
    String,
    String,
    ScyllaDBTypeInfo::TextTextMap,
    ScyllaDBArgument::TextTextMap
);

impl_map_type!(
    String,
    bool,
    ScyllaDBTypeInfo::TextBooleanMap,
    ScyllaDBArgument::TextBooleanMap
);

impl_map_type!(
    String,
    i8,
    ScyllaDBTypeInfo::TextTinyIntMap,
    ScyllaDBArgument::TextTinyIntMap
);

impl_map_type!(
    String,
    i16,
    ScyllaDBTypeInfo::TextSmallIntMap,
    ScyllaDBArgument::TextSmallIntMap
);

impl_map_type!(
    String,
    i32,
    ScyllaDBTypeInfo::TextIntMap,
    ScyllaDBArgument::TextIntMap
);

impl_map_type!(
    String,
    i64,
    ScyllaDBTypeInfo::TextBigIntMap,
    ScyllaDBArgument::TextBigIntMap
);

impl_map_type!(
    String,
    f32,
    ScyllaDBTypeInfo::TextFloatMap,
    ScyllaDBArgument::TextFloatMap
);

impl_map_type!(
    String,
    f64,
    ScyllaDBTypeInfo::TextDoubleMap,
    ScyllaDBArgument::TextDoubleMap
);

impl_map_type!(
    String,
    Uuid,
    ScyllaDBTypeInfo::TextUuidMap,
    ScyllaDBArgument::TextUuidMap
);

impl_map_type!(
    String,
    IpAddr,
    ScyllaDBTypeInfo::TextInetMap,
    ScyllaDBArgument::TextIpAddrMap
);

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::IpAddr, rc::Rc, str::FromStr, sync::Arc};

    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;
    use uuid::Uuid;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
    };

    #[test]
    fn it_can_encode_text_hashmap() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            HashMap::from([(String::from("Hello"), String::from("World!"))]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            HashMap::from([(String::from("Hello"), true)]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            HashMap::from([(String::from("Hello"), 7i8)]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            HashMap::from([(String::from("Hello"), 7i16)]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            HashMap::from([(String::from("Hello"), 7i32)]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            HashMap::from([(String::from("Hello"), 7i64)]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            HashMap::from([(String::from("Hello"), Uuid::new_v4())]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            HashMap::from([(String::from("Hello"), IpAddr::from_str("2001:db8::3")?)]),
            &mut buf,
        )?;

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(HashMap::from([(
                String::from("Hello"),
                String::from("World!"),
            )])),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(HashMap::from([(
                String::from("Hello"),
                String::from("World!"),
            )])),
            &mut buf,
        )?;

        Ok(())
    }

    #[test]
    fn it_can_decode_text_text_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Text)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(String::from("Hello"), String::from("World!"))]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextTextMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, String> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            HashMap::from([(String::from("Hello"), String::from("World!"))]),
        );

        Ok(())
    }

    #[test]
    fn it_can_decode_text_bool_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Boolean)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(String::from("Hello"), true)]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextTextMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, bool> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, HashMap::from([(String::from("Hello"), true)]),);

        Ok(())
    }

    #[test]
    fn it_can_decode_text_tinyint_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::TinyInt)),
            ),
        };
        let raw_value =
            serialize_value(&HashMap::from([(String::from("Hello"), 7i8)]), &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextTinyIntMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, i8> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, HashMap::from([(String::from("Hello"), 7i8)]),);

        Ok(())
    }

    #[test]
    fn it_can_decode_text_smallint_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::SmallInt)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(String::from("Hello"), 7i16)]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextSmallIntMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, i16> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, HashMap::from([(String::from("Hello"), 7i16)]),);

        Ok(())
    }

    #[test]
    fn it_can_decode_text_int_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Int)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(String::from("Hello"), 7i32)]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextIntMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, i32> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, HashMap::from([(String::from("Hello"), 7i32)]),);

        Ok(())
    }

    #[test]
    fn it_can_decode_text_bigint_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::BigInt)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(String::from("Hello"), 7i64)]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextBigIntMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, i64> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, HashMap::from([(String::from("Hello"), 7i64)]),);

        Ok(())
    }

    #[test]
    fn it_can_decode_text_float_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Float)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(String::from("Hello"), 11.5f32)]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextFloatMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, f32> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, HashMap::from([(String::from("Hello"), 11.5f32)]),);

        Ok(())
    }

    #[test]
    fn it_can_decode_text_double_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Double)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(String::from("Hello"), 11.5f64)]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextFloatMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, f64> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, HashMap::from([(String::from("Hello"), 11.5f64)]),);

        Ok(())
    }

    #[test]
    fn it_can_decode_text_uuid_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Uuid)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(
                String::from("Hello"),
                Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
            )]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextUuidMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, Uuid> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            HashMap::from([(
                String::from("Hello"),
                Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
            )]),
        );

        Ok(())
    }

    #[test]
    fn it_can_decode_text_inet_hashmap() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Inet)),
            ),
        };
        let raw_value = serialize_value(
            &HashMap::from([(String::from("Hello"), IpAddr::from_str("2001:db8::3")?)]),
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_hashmap"),
            ScyllaDBTypeInfo::TextInetMap,
            &raw_value,
            &column_type,
        );
        let decoded: HashMap<String, IpAddr> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            HashMap::from([(String::from("Hello"), IpAddr::from_str("2001:db8::3")?)]),
        );

        Ok(())
    }
}
