use sqlx::{Decode, error::BoxDynError};

use crate::{ScyllaDB, ScyllaDBValueRef};

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

macro_rules! impl_string_type {
    ($typ:ty) => {
        impl ::sqlx::Type<$crate::ScyllaDB> for $typ {
            fn type_info() -> $crate::ScyllaDBTypeInfo {
                $crate::ScyllaDBTypeInfo::Text
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for $typ {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                let argument = $crate::ScyllaDBArgument::Text(self.to_string());
                buf.push(argument);

                Ok(::sqlx::encode::IsNull::No)
            }
        }

        impl $crate::ScyllaDBHasArrayType for $typ {
            fn array_type_info() -> $crate::ScyllaDBTypeInfo {
                $crate::ScyllaDBTypeInfo::TextArray
            }
        }

        // slice
        impl<const N: usize> ::sqlx::Encode<'_, crate::ScyllaDB> for [$typ; N] {
            #[inline(always)]
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for [$typ] {
            #[inline(always)]
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                let mut strings = ::std::vec::Vec::with_capacity(self.len());
                for value in self.iter() {
                    strings.push(value.to_string());
                }
                let argument = $crate::ScyllaDBArgument::TextArray(strings);
                buf.push(argument);

                Ok(::sqlx::encode::IsNull::No)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for &[$typ] {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(*self, buf)
            }
        }

        impl<const N: usize> ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::boxed::Box<[$typ; N]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use ::std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        impl<const N: usize> ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::rc::Rc<[$typ; N]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use ::std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        impl<const N: usize> ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::sync::Arc<[$typ; N]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use ::std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::boxed::Box<[$typ]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::rc::Rc<[$typ]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::sync::Arc<[$typ]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use ::std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::boxed::Box<&[$typ]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::rc::Rc<&[$typ]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use ::std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::sync::Arc<&[$typ]> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                use ::std::ops::Deref;

                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.deref(), buf)
            }
        }

        // Vec
        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::vec::Vec<$typ> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::rc::Rc<::std::vec::Vec<$typ>> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
            }
        }

        impl ::sqlx::Encode<'_, $crate::ScyllaDB> for ::std::sync::Arc<::std::vec::Vec<$typ>> {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                <_ as ::sqlx::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(self.as_slice(), buf)
            }
        }
    };
}

impl_string_type!(&str);
impl_string_type!(String);
impl_string_type!(std::borrow::Cow<'_, str>);
impl_string_type!(std::boxed::Box<&str>);
impl_string_type!(std::boxed::Box<String>);
impl_string_type!(std::rc::Rc<&str>);
impl_string_type!(std::rc::Rc<String>);
impl_string_type!(std::sync::Arc<&str>);
impl_string_type!(std::sync::Arc<String>);

#[cfg(feature = "secrecy-08")]
pub mod secrecy {
    use std::{ops::Deref, rc::Rc, sync::Arc};

    use secrecy_08::SecretString;
    use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

    use crate::{
        ScyllaDB, ScyllaDBHasArrayType, ScyllaDBTypeInfo, ScyllaDBValueRef,
        arguments::{ScyllaDBArgument, ScyllaDBArgumentBuffer},
    };

    impl Type<ScyllaDB> for SecretString {
        fn type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::Text
        }
    }

    impl Type<ScyllaDB> for Rc<SecretString> {
        fn type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::Text
        }
    }

    impl Type<ScyllaDB> for Arc<SecretString> {
        fn type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::Text
        }
    }

    impl ScyllaDBHasArrayType for SecretString {
        fn array_type_info() -> ScyllaDBTypeInfo {
            ScyllaDBTypeInfo::TextArray
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
            let argument = ScyllaDBArgument::SecretText(self.clone());
            buf.push(argument);

            Ok(IsNull::No)
        }
    }

    impl Encode<'_, ScyllaDB> for Rc<SecretString> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
        }
    }

    impl Encode<'_, ScyllaDB> for Arc<SecretString> {
        fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <_ as Encode<'_, ScyllaDB>>::encode_by_ref(self.deref(), buf)
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
            let argument = ScyllaDBArgument::SecretTextArray(strings);
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
    use std::{borrow::Cow, rc::Rc, sync::Arc};

    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
    };

    #[test]
    fn it_can_encode_string() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        // &str
        let _ = <_ as Encode<'_, ScyllaDB>>::encode("Hello!", &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new("Hello!"), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new("Hello!"), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Cow::from("Hello!"), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Box::new("Hello!"), &mut buf)?;

        // String
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(String::from("Hello!"), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(String::from("Hello!")), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(String::from("Hello!")), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Cow::from(String::from("Hello!")), &mut buf)?;

        // [&str; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(["Hello!"], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&["Hello!"], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Box::new(["Hello!"]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(["Hello!"]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(["Hello!"]), &mut buf)?;

        // &[&str]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(["Hello!"].as_slice(), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Box::new(["Hello!"].as_slice()), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(["Hello!"].as_slice()), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(["Hello!"].as_slice()), &mut buf)?;

        // Vec<&str>
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec!["Hello!"], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec!["Hello!"]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec!["Hello!"]), &mut buf)?;

        // [String; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([String::from("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[String::from("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Box::new([String::from("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new([String::from("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new([String::from("Hello!")]), &mut buf)?;

        // &[String]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([String::from("Hello!")].as_slice(), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([String::from("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([String::from("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([String::from("Hello!")].as_slice()),
            &mut buf,
        )?;

        // Vec<String>
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![String::from("Hello!")], &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![String::from("Hello!")]), &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![String::from("Hello!")]), &mut buf)?;

        // [Rc<&str>; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Rc::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[Rc::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Box::new([Rc::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new([Rc::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new([Rc::new("Hello!")]), &mut buf)?;

        // &[Rc<&str>]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Rc::new("Hello!")].as_slice(), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([Rc::new("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(Rc::new([Rc::new("Hello!")].as_slice()), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Rc::new("Hello!")].as_slice()),
            &mut buf,
        )?;

        // Vec<Rc<&str>>
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![Rc::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![Rc::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![Rc::new("Hello!")]), &mut buf)?;

        // [Arc<&str>; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Arc::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[Arc::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Box::new([Arc::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new([Arc::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new([Arc::new("Hello!")]), &mut buf)?;

        // &[Arc<&str>]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Arc::new("Hello!")].as_slice(), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([Arc::new("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Arc::new("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Arc::new("Hello!")].as_slice()),
            &mut buf,
        )?;

        // Vec<Arc<&str>>
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![Arc::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![Arc::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![Arc::new("Hello!")]), &mut buf)?;

        // [Box<&str>; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Box::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[Box::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Box::new([Box::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new([Box::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new([Box::new("Hello!")]), &mut buf)?;

        // &[Box<&str>]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Box::new("Hello!")].as_slice(), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([Box::new("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Box::new("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Box::new("Hello!")].as_slice()),
            &mut buf,
        )?;

        // Vec<Box<&str>>
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![Box::new("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![Box::new("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![Box::new("Hello!")]), &mut buf)?;

        // [Rc<String>; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Rc::new(String::from("Hello!"))], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[Rc::new(String::from("Hello!"))], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([Rc::new(String::from("Hello!"))]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([Rc::new(String::from("Hello!"))]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Rc::new(String::from("Hello!"))]),
            &mut buf,
        )?;

        // &[Rc<String>]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [Rc::new(String::from("Hello!"))].as_slice(),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([Rc::new(String::from("Hello!"))].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([Rc::new(String::from("Hello!"))].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Rc::new(String::from("Hello!"))].as_slice()),
            &mut buf,
        )?;

        // Vec<Rc<String>>
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(vec![Rc::new(String::from("Hello!"))], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![Rc::new(String::from("Hello!"))]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![Rc::new(String::from("Hello!"))]),
            &mut buf,
        )?;

        // [Arc<String>; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Arc::new(String::from("Hello!"))], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[Arc::new(String::from("Hello!"))], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([Arc::new(String::from("Hello!"))]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([Arc::new(String::from("Hello!"))]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Arc::new(String::from("Hello!"))]),
            &mut buf,
        )?;

        // &[Arc<String>]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [Arc::new(String::from("Hello!"))].as_slice(),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([Arc::new(String::from("Hello!"))].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([Arc::new(String::from("Hello!"))].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Arc::new(String::from("Hello!"))].as_slice()),
            &mut buf,
        )?;

        // Vec<Arc<String>>
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(vec![Arc::new(String::from("Hello!"))], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![Arc::new(String::from("Hello!"))]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![Arc::new(String::from("Hello!"))]),
            &mut buf,
        )?;

        // [Cow<'_, str>; N]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Cow::from("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[Cow::from("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Box::new([Cow::from("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new([Cow::from("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new([Cow::from("Hello!")]), &mut buf)?;

        // &[Cow<'_, str>]
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Cow::from("Hello!")].as_slice(), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Box::new([Cow::from("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new([Cow::from("Hello!")].as_slice()),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new([Cow::from("Hello!")].as_slice()),
            &mut buf,
        )?;

        // Vec<Cow<'_, str>>
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(vec![Cow::from("Hello!")], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Rc::new(vec![Cow::from("Hello!")]), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Arc::new(vec![Cow::from("Hello!")]), &mut buf)?;

        Ok(())
    }

    #[test]
    fn it_can_decode_string() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Text);
        let raw_value = serialize_value(&String::from("Hello!"), &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_text"),
            ScyllaDBTypeInfo::Text,
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
            ScyllaDBTypeInfo::Text,
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
            ScyllaDBTypeInfo::TextArray,
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
            ScyllaDBTypeInfo::TextArray,
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
        use sqlx::{Decode, Encode, error::BoxDynError};
        use sqlx_core::ext::ustr::UStr;

        use crate::{
            ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
            types::serialize_value,
        };

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
                ScyllaDBTypeInfo::Text,
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
                ScyllaDBTypeInfo::TextArray,
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
