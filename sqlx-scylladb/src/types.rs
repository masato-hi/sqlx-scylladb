macro_rules! impl_type {
    ($typ:ty, $typ_info:path, $arg_typ:path) => {
        impl sqlx::Type<crate::ScyllaDB> for $typ {
            fn type_info() -> crate::ScyllaDBTypeInfo {
                $typ_info
            }
        }

        impl sqlx::Encode<'_, crate::ScyllaDB> for $typ {
            fn encode(
                self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(self);
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(self.clone());
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }
        }

        impl sqlx::Decode<'_, crate::ScyllaDB> for $typ {
            fn decode(
                value: crate::ScyllaDBValueRef<'_>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let val: Self = value.deserialize()?;
                Ok(val)
            }
        }
    };
}

macro_rules! impl_array_type {
    ($typ:ty, $typ_info:path, $arg_typ:path) => {
        impl crate::ScyllaDBHasArrayType for $typ {
            fn array_type_info() -> crate::ScyllaDBTypeInfo {
                $typ_info
            }
        }

        impl<const N: usize> sqlx::Encode<'_, crate::ScyllaDB> for [$typ; N] {
            #[inline(always)]
            fn encode_by_ref(
                &self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(std::sync::Arc::new(self.to_vec()));
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }
        }

        impl<'r> sqlx::Encode<'r, crate::ScyllaDB> for &'r [$typ] {
            #[inline(always)]
            fn encode_by_ref(
                &self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(std::sync::Arc::new(self.to_vec()));
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }
        }

        impl sqlx::Encode<'_, crate::ScyllaDB> for Vec<$typ> {
            fn encode(
                self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(std::sync::Arc::new(self));
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(std::sync::Arc::new(self.clone()));
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }
        }

        impl sqlx::Encode<'_, crate::ScyllaDB> for std::sync::Arc<Vec<$typ>> {
            #[inline(always)]
            fn encode_by_ref(
                &self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(self.clone());
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }
        }

        impl<'r> sqlx::Decode<'r, crate::ScyllaDB> for Vec<$typ> {
            fn decode(
                value: crate::ScyllaDBValueRef<'r>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let val: Self = value.deserialize()?;
                Ok(val)
            }
        }
    };
}

macro_rules! impl_map_type {
    ($key_typ:ty, $value_typ:ty, $typ_info:path, $arg_typ:path) => {
        impl sqlx::Type<crate::ScyllaDB> for std::collections::HashMap<$key_typ, $value_typ> {
            fn type_info() -> crate::ScyllaDBTypeInfo {
                $typ_info
            }
        }

        impl sqlx::Encode<'_, crate::ScyllaDB> for std::collections::HashMap<$key_typ, $value_typ> {
            fn encode(
                self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(std::sync::Arc::new(self));
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = $arg_typ(std::sync::Arc::new(self.clone()));
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }
        }

        impl sqlx::Decode<'_, crate::ScyllaDB> for std::collections::HashMap<$key_typ, $value_typ> {
            fn decode(
                value: crate::ScyllaDBValueRef<'_>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let val: Self = value.deserialize()?;
                Ok(val)
            }
        }
    };
}

pub mod array;
pub mod blob;
pub mod bool;
pub mod counter;
pub mod date;
#[cfg(feature = "bigdecimal-04")]
pub mod decimal;
pub mod float;
pub mod inet;
pub mod int;
pub mod map;
pub mod text;
pub mod time;
pub mod timestamp;
pub mod tuple;
pub mod uuid;
