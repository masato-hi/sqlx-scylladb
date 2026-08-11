use std::{collections::HashMap, hash::Hash};

use scylla::{deserialize::value::DeserializeValue, serialize::value::SerializeValue};
use sqlx_core::{
    decode::Decode, encode::Encode, ext::ustr::UStr, type_info::TypeInfo, types::Type,
};

use crate::{ScyllaDB, ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

fn build_map_type_info_name(
    key_type_info: ScyllaDBTypeInfo,
    value_type_info: ScyllaDBTypeInfo,
) -> UStr {
    let key_type_info_name = key_type_info.name();
    let value_type_info_name = value_type_info.name();

    let type_info_name = format!("<{},{}>", key_type_info_name, value_type_info_name);
    UStr::from(type_info_name)
}

impl<K, V> Type<ScyllaDB> for HashMap<K, V>
where
    K: Type<ScyllaDB>,
    V: Type<ScyllaDB>,
{
    fn type_info() -> <ScyllaDB as sqlx_core::database::Database>::TypeInfo {
        let key_type_info = K::type_info();
        let value_type_info = V::type_info();

        let type_info_name = build_map_type_info_name(key_type_info, value_type_info);

        ScyllaDBTypeInfo::Map(type_info_name)
    }
}

impl<K, V> Encode<'_, ScyllaDB> for HashMap<K, V>
where
    K: SerializeValue + Clone + Send + Sync + 'static,
    V: SerializeValue + Clone + Send + Sync + 'static,
{
    fn encode(
        self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
        let argument = ScyllaDBArgument::Map(Box::new(self));
        buf.push(argument);
        Ok(::sqlx_core::encode::IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        let argument = ScyllaDBArgument::Map(Box::new((*self).clone()));
        buf.push(argument);

        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl<K, V> Decode<'_, ScyllaDB> for HashMap<K, V>
where
    K: for<'a> DeserializeValue<'a, 'a> + Hash + Eq,
    V: for<'a> DeserializeValue<'a, 'a>,
{
    fn decode(
        value: <ScyllaDB as sqlx_core::database::Database>::ValueRef<'_>,
    ) -> Result<Self, sqlx_core::error::BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::IpAddr, rc::Rc, str::FromStr, sync::Arc};

    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx_core::{decode::Decode, encode::Encode, error::BoxDynError, ext::ustr::UStr};
    use uuid::Uuid;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBValueRef,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
            (&column_type).try_into()?,
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
