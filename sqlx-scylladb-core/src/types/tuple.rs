macro_rules! impl_tuple {
    (
        $($typs:ident),*;
        $($fidents:ident),*;
        $($tidents:ident),*;
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
    (;;;$length:expr) => {};
    (
        $typ:ident$(, $($typs:ident),*)?;
        $fident:ident$(, $($fidents:ident),*)?;
        $tident:ident$(, $($tidents:ident),*)?;
        $length:expr
    ) => {
        impl_tuples!(
            $($($typs),*)?;
            $($($fidents),*)?;
            $($($tidents),*)?;
            $length - 1
        );
        impl_tuple!(
            $typ$(, $($typs),*)?;
            $fident$(, $($fidents),*)?;
            $tident$(, $($tidents),*)?;
            $length
        );
    };
}

impl_tuples!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
    f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15;
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    16
);
