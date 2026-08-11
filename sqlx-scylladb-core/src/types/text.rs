use std::{borrow::Cow, sync::Arc};

use sqlx_core::{decode::Decode, encode::Encode, error::BoxDynError, types::Type};

use crate::{
    ScyllaDB, ScyllaDBArgument, ScyllaDBHasArrayType, ScyllaDBTypeInfo, ScyllaDBTypeInfoNative,
    ScyllaDBTypeInfoNativeArray, ScyllaDBValueRef,
};

impl Decode<'_, ScyllaDB> for String {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl Decode<'_, ScyllaDB> for Vec<String> {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl Type<ScyllaDB> for &str {
    fn type_info() -> <ScyllaDB as sqlx_core::database::Database>::TypeInfo {
        ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Text)
    }
}

impl Encode<'_, ScyllaDB> for &str {
    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Cow::Owned(self.to_string()));
        buf.push(argument);

        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl Type<ScyllaDB> for Cow<'static, str> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Text)
    }
}

impl Encode<'_, ScyllaDB> for Cow<'static, str> {
    fn encode(
        self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(self);
        buf.push(argument);
        Ok(sqlx_core::encode::IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        self.clone().encode(buf)
    }
}

impl Type<ScyllaDB> for String {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Text)
    }
}

impl Encode<'_, ScyllaDB> for String {
    fn encode(
        self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Cow::Owned(self));
        buf.push(argument);
        Ok(sqlx_core::encode::IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        <String as Encode<'_, ScyllaDB>>::encode(self.clone(), buf)
    }
}

impl Type<ScyllaDB> for Arc<str> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Text)
    }
}

impl Encode<'_, ScyllaDB> for Arc<str> {
    fn encode(
        self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text_ArcStr(self);
        buf.push(argument);
        Ok(sqlx_core::encode::IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text_ArcStr(self.clone());
        buf.push(argument);
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl ScyllaDBHasArrayType for String {
    fn array_type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::Text)
    }
}

impl Encode<'_, ScyllaDB> for Vec<String> {
    fn encode(
        self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        let strings = self
            .into_iter()
            .map(crate::types::IntoScyllaText::into_scylla_text)
            .collect();
        let argument = ScyllaDBArgument::TextArray(strings);
        buf.push(argument);
        Ok(sqlx_core::encode::IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        let strings = self.clone();
        <Vec<String> as Encode<'_, ScyllaDB>>::encode(strings, buf)
    }
}

impl ScyllaDBHasArrayType for &'static str {
    fn array_type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::Text)
    }
}

impl Encode<'_, ScyllaDB> for [&'static str] {
    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        let strings = self.iter().map(|value| (*value).to_owned()).collect();
        let argument = ScyllaDBArgument::TextArray(strings);
        buf.push(argument);
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl Encode<'_, ScyllaDB> for &[&'static str] {
    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        <[&'static str] as Encode<'_, ScyllaDB>>::encode_by_ref(self, buf)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for [&'static str; N] {
    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<sqlx_core::encode::IsNull, BoxDynError> {
        <[&'static str] as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
    }
}

#[cfg(feature = "secrecy-08")]
pub mod secrecy {
    use secrecy_08::SecretString;
    use sqlx_core::{
        decode::Decode,
        encode::{Encode, IsNull},
        error::BoxDynError,
        types::Type,
    };

    use crate::{
        ScyllaDB, ScyllaDBHasArrayType, ScyllaDBTypeInfo, ScyllaDBTypeInfoNative,
        ScyllaDBTypeInfoNativeArray, ScyllaDBValueRef,
        arguments::{ScyllaDBArgument, ScyllaDBArgumentBuffer},
    };

    impl Type<ScyllaDB> for SecretString {
        fn type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::Native(ScyllaDBTypeInfoNative::Text)
        }
    }

    impl ScyllaDBHasArrayType for SecretString {
        fn array_type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::NativeArray(ScyllaDBTypeInfoNativeArray::Text)
        }
    }

    impl Decode<'_, ScyllaDB> for SecretString {
        fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
            let val: Self = value.deserialize()?;
            Ok(val)
        }
    }

    impl Decode<'_, ScyllaDB> for Vec<SecretString> {
        fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
            let val: Self = value.deserialize()?;
            Ok(val)
        }
    }

    impl Encode<'_, ScyllaDB> for SecretString {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            let argument = ScyllaDBArgument::Text_Secrecy08(self.clone());
            buf.push(argument);

            Ok(IsNull::No)
        }
    }

    impl<const N: usize> Encode<'_, ScyllaDB> for [SecretString; N] {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
        }
    }

    impl Encode<'_, ScyllaDB> for [SecretString] {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            use secrecy_08::ExposeSecret;

            let mut strings = Vec::with_capacity(self.len());
            for value in self.iter() {
                let value = value.expose_secret();
                let value = SecretString::new(value.to_string());
                strings.push(value);
            }
            let argument = ScyllaDBArgument::TextArray_Secrecy08(strings);
            buf.push(argument);

            Ok(IsNull::No)
        }
    }

    impl Encode<'_, ScyllaDB> for &[SecretString] {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <_ as Encode<'_, ScyllaDB>>::encode_by_ref(*self, buf)
        }
    }

    impl Encode<'_, ScyllaDB> for Vec<secrecy_08::SecretString> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx_core::{decode::Decode, encode::Encode, error::BoxDynError, ext::ustr::UStr};

    use crate::{ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBValueRef, types::serialize_value};

    #[test]
    fn it_can_encode_string() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![String::from("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(["Hello!"], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(["Hello!"].as_slice(), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_string() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Text);
        let raw_value = serialize_value(&String::from("Hello!"), &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_text"),
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: String = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, "Hello!");

        Ok(())
    }

    #[test]
    fn it_can_decode_string_compatible() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Ascii);
        let raw_value = serialize_value(&String::from("Hello!"), &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_text"),
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: String = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, "Hello!");

        Ok(())
    }

    #[test]
    fn it_can_decode_string_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Text))),
        };
        let raw_value = serialize_value(
            &vec![String::from("Hello"), String::from("World!")],
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_text"),
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<String> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, ["Hello", "World!"]);

        Ok(())
    }

    #[test]
    fn it_can_decode_string_array_compatible() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Ascii))),
        };
        let raw_value = serialize_value(
            &vec![String::from("Hello"), String::from("World!")],
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_text"),
            (&column_type).try_into()?,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<String> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, ["Hello", "World!"]);

        Ok(())
    }

    #[cfg(feature = "secrecy-08")]
    mod secrecy {
        use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};
        use secrecy_08::{ExposeSecret, SecretString};
        use sqlx_core::{decode::Decode, encode::Encode, error::BoxDynError, ext::ustr::UStr};

        use crate::{ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBValueRef, types::serialize_value};

        #[test]
        fn it_can_encode_secret_string() -> Result<(), BoxDynError> {
            let mut buf = ScyllaDBArgumentBuffer::default();

            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                secrecy_08::SecretString::new(String::from("Hello!")),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [secrecy_08::SecretString::new(String::from("Hello!"))],
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                [secrecy_08::SecretString::new(String::from("Hello!"))].as_slice(),
                &mut buf,
            )?;
            let _ = <_ as Encode<'_, ScyllaDB>>::encode(
                vec![secrecy_08::SecretString::new(String::from("Hello!"))],
                &mut buf,
            )?;

            Ok(())
        }

        #[test]
        fn it_can_decode_secret_string() -> Result<(), BoxDynError> {
            use secrecy_08::{ExposeSecret, SecretString};

            let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Text);
            let raw_value =
                serialize_value(&SecretString::from(String::from("Hello!")), &column_type)?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_text"),
                (&column_type).try_into()?,
                &raw_value,
                &column_type,
            );
            let decoded: SecretString = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded.expose_secret(), "Hello!");

            Ok(())
        }

        #[test]
        fn it_can_decode_secret_string_array() -> Result<(), BoxDynError> {
            let column_type: ColumnType<'_> = ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Text))),
            };
            let raw_value = serialize_value(
                &vec![
                    SecretString::from(String::from("Hello")),
                    SecretString::from(String::from("World!")),
                ],
                &column_type,
            )?;

            let value = ScyllaDBValueRef::new(
                UStr::new("my_text"),
                (&column_type).try_into()?,
                &raw_value,
                &column_type,
            );
            let decoded: Vec<SecretString> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
            assert_eq!(decoded.get(0).unwrap().expose_secret(), "Hello");
            assert_eq!(decoded.get(1).unwrap().expose_secret(), "World!");

            Ok(())
        }
    }
}
