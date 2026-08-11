pub(crate) trait IntoScyllaText {
    fn into_scylla_text(self) -> String;
}

impl IntoScyllaText for String {
    fn into_scylla_text(self) -> String {
        self
    }
}

impl<'a> IntoScyllaText for &'a str {
    fn into_scylla_text(self) -> String {
        self.to_owned()
    }
}

impl<'a> IntoScyllaText for std::borrow::Cow<'a, str> {
    fn into_scylla_text(self) -> String {
        self.into_owned()
    }
}

impl IntoScyllaText for std::sync::Arc<str> {
    fn into_scylla_text(self) -> String {
        self.to_string()
    }
}

macro_rules! impl_native_type {
    ($typ:ty, $typ_info:path, $arg_typ:expr) => {
        impl ::sqlx_core::types::Type<$crate::ScyllaDB> for $typ {
            fn type_info() -> $crate::ScyllaDBTypeInfo {
                $typ_info
            }
        }

        impl ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB> for $typ {
            fn encode(
                self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                buf.push(($arg_typ)(self));
                Ok(::sqlx_core::encode::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                let argument = ($arg_typ)(self.clone());
                buf.push(argument);

                Ok(::sqlx_core::encode::IsNull::No)
            }
        }

        impl ::sqlx_core::decode::Decode<'_, crate::ScyllaDB> for $typ {
            fn decode(
                value: $crate::ScyllaDBValueRef<'_>,
            ) -> Result<Self, ::sqlx_core::error::BoxDynError> {
                let val: Self = value.deserialize()?;
                Ok(val)
            }
        }
    };
}

macro_rules! impl_native_array_type {
    ($typ:ty, $typ_info:path, $arg_typ:path) => {
        impl $crate::ScyllaDBHasArrayType for $typ {
            fn array_type_info() -> $crate::ScyllaDBTypeInfo {
                $typ_info
            }
        }

        impl<const N: usize> ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB> for [$typ; N] {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                <_ as ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(
                    self.as_slice(),
                    buf,
                )
            }
        }

        impl ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB> for [$typ] {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                let argument = $arg_typ(self.to_vec());
                buf.push(argument);

                Ok(::sqlx_core::encode::IsNull::No)
            }
        }

        impl ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB> for &[$typ] {
            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                <_ as ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(*self, buf)
            }
        }

        impl ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB> for ::std::vec::Vec<$typ> {
            fn encode(
                self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                buf.push($arg_typ(self));
                Ok(::sqlx_core::encode::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                <_ as ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB>>::encode_by_ref(
                    self.as_slice(),
                    buf,
                )
            }
        }

        impl ::sqlx_core::decode::Decode<'_, $crate::ScyllaDB> for ::std::vec::Vec<$typ> {
            fn decode(
                value: $crate::ScyllaDBValueRef<'_>,
            ) -> Result<Self, ::sqlx_core::error::BoxDynError> {
                let val: Self = value.deserialize()?;
                Ok(val)
            }
        }
    };
}

macro_rules! impl_map_type {
    ($key_typ:ty, $value_typ:ty, $typ_info:path, $arg_typ:path) => {
        impl ::sqlx_core::types::Type<$crate::ScyllaDB>
            for ::std::collections::HashMap<$key_typ, $value_typ>
        {
            fn type_info() -> $crate::ScyllaDBTypeInfo {
                $typ_info
            }
        }

        impl ::sqlx_core::decode::Decode<'_, $crate::ScyllaDB>
            for ::std::collections::HashMap<$key_typ, $value_typ>
        {
            fn decode(
                value: $crate::ScyllaDBValueRef<'_>,
            ) -> Result<Self, ::sqlx_core::error::BoxDynError> {
                let val: Self = value.deserialize()?;
                Ok(val)
            }
        }

        impl ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB>
            for ::std::collections::HashMap<$key_typ, $value_typ>
        {
            fn encode(
                self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                buf.push($arg_typ(self));
                Ok(::sqlx_core::encode::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                let argument = $arg_typ(self.clone());
                buf.push(argument);

                Ok(::sqlx_core::encode::IsNull::No)
            }
        }
    };
}

pub mod array;
pub mod blob;
pub mod bool;
pub mod counter;
pub mod date;
pub mod decimal;
pub mod duration;
pub mod float;
pub mod inet;
pub mod int;
pub mod map;
pub mod text;
pub mod time;
pub mod timestamp;
pub mod tuple;
pub mod unset;
pub mod user_defined_type;
pub mod uuid;

#[cfg(test)]
fn serialize_value<T>(
    value: &T,
    column_type: &::scylla::cluster::metadata::ColumnType<'_>,
) -> ::std::result::Result<::bytes::Bytes, ::sqlx_core::error::BoxDynError>
where
    T: ::scylla::serialize::value::SerializeValue,
{
    let mut values = ::scylla_cql::serialize::row::SerializedValues::new();
    values.add_value(value, column_type)?;
    let mut buf = std::vec::Vec::new();
    for value in values.iter() {
        let val = value.as_value().ok_or("expect non-null value.")?;
        buf.extend_from_slice(val);
    }

    Ok(::bytes::Bytes::from(buf))
}
