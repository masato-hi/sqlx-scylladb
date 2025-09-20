use std::{borrow::Cow, rc::Rc, sync::Arc};

use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

use crate::{
    ScyllaDB, ScyllaDBHasArrayType, ScyllaDBTypeInfo, ScyllaDBValueRef,
    arguments::{ScyllaDBArgument, ScyllaDBArgumentBuffer},
};

impl Type<ScyllaDB> for &str {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl<'r> Encode<'r, ScyllaDB> for &'r str {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Arc::new(self.to_string()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, ScyllaDB> for &'r str {
    fn decode(value: ScyllaDBValueRef<'r>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl Type<ScyllaDB> for String {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl Encode<'_, ScyllaDB> for String {
    fn encode(self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Arc::new(self));
        buf.push(argument);

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Arc::new(self.clone()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for String {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

impl Type<ScyllaDB> for Arc<String> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl Encode<'_, ScyllaDB> for Arc<String> {
    fn encode(self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(self);
        buf.push(argument);

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(self.clone());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Type<ScyllaDB> for Rc<String> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl Encode<'_, ScyllaDB> for Rc<String> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Arc::new(self.to_string()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Type<ScyllaDB> for Arc<str> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl<'r> Encode<'r, ScyllaDB> for Arc<str> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Arc::new(self.to_string()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Type<ScyllaDB> for Rc<str> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl<'r> Encode<'r, ScyllaDB> for Rc<str> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Arc::new(self.to_string()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Type<ScyllaDB> for Cow<'_, str> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl<'r> Encode<'r, ScyllaDB> for Cow<'_, str> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Arc::new(self.to_string()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Type<ScyllaDB> for Box<str> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

impl<'r> Encode<'r, ScyllaDB> for Box<str> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Text(Arc::new(self.to_string()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

#[cfg(feature = "secrecy-08")]
impl Type<ScyllaDB> for secrecy_08::SecretString {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

#[cfg(feature = "secrecy-08")]
impl Encode<'_, ScyllaDB> for secrecy_08::SecretString {
    fn encode(self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::SecretText(Arc::new(self));
        buf.push(argument);

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::SecretText(Arc::new(self.clone()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

#[cfg(feature = "secrecy-08")]
impl Decode<'_, ScyllaDB> for secrecy_08::SecretString {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}

#[cfg(feature = "secrecy-08")]
impl Type<ScyllaDB> for Arc<secrecy_08::SecretString> {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Text
    }
}

#[cfg(feature = "secrecy-08")]
impl Encode<'_, ScyllaDB> for Arc<secrecy_08::SecretString> {
    fn encode(self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::SecretText(self);
        buf.push(argument);

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::SecretText(self.clone());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl ScyllaDBHasArrayType for &str {
    fn array_type_info() -> crate::ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::TextArray
    }
}

impl Encode<'_, ScyllaDB> for &[&str] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut strings = Vec::with_capacity(self.len());
        for value in self.iter() {
            strings.push(value.to_string());
        }
        let argument = ScyllaDBArgument::TextArray(Arc::new(strings));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl<const N: usize> Encode<'_, ScyllaDB> for [&str; N] {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut strings = Vec::with_capacity(self.len());
        for value in self.iter() {
            strings.push(value.to_string());
        }
        let argument = ScyllaDBArgument::TextArray(Arc::new(strings));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Encode<'_, ScyllaDB> for Vec<&str> {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut strings = Vec::with_capacity(self.len());
        for value in self.iter() {
            strings.push(value.to_string());
        }
        let argument = ScyllaDBArgument::TextArray(Arc::new(strings));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl_array_type!(
    String,
    ScyllaDBTypeInfo::TextArray,
    ScyllaDBArgument::TextArray
);
