macro_rules! impl_tuple {
    (
        $($typs:ident),*;
        $length:expr
    ) => {
        impl <$($typs),*> sqlx::Type<crate::ScyllaDB> for ($($typs,)*)
        where $($typs: sqlx::Type<crate::ScyllaDB>),* {
            fn type_info() -> crate::ScyllaDBTypeInfo {
                let type_infos = vec![$($typs::type_info()),*];
                let type_name = crate::type_info::tuple_type_name(&type_infos);

                crate::ScyllaDBTypeInfo::Tuple(type_name)
            }
        }

        impl<$($typs), *> sqlx::Encode<'_, crate::ScyllaDB> for ($($typs,)*)
        where $($typs: scylla::serialize::value::SerializeValue + Clone + Send + Sync + 'static,)* {
            fn encode(
                self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = crate::ScyllaDBArgument::Tuple(std::sync::Arc::new(self));
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut crate::ScyllaDBArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let argument = crate::ScyllaDBArgument::Tuple(std::sync::Arc::new(self.clone()));
                buf.push(argument);

                Ok(sqlx::encode::IsNull::No)
            }
        }

        impl<$($typs),*> sqlx::Decode<'_, crate::ScyllaDB> for ($($typs,)*)
        where $($typs: for<'a> scylla::deserialize::value::DeserializeValue<'a, 'a>),* {
            fn decode(
                value: crate::ScyllaDBValueRef<'_>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let val: ($($typs,)*) = value.deserialize()?;
                Ok(val)
            }
        }
    };
}

macro_rules! impl_tuples {
    (;$length:expr) => {};
    (
        $typ:ident$(, $($typs:ident),*)?;
        $length:expr
    ) => {
        impl_tuples!(
            $($($typs),*)?;
            $length - 1
        );
        impl_tuple!(
            $typ$(, $($typs),*)?;
            $length
        );
    };
}

impl_tuples!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
    16
);
