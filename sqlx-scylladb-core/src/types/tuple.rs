use std::{
    any::TypeId,
    sync::{LazyLock, RwLock},
};

use sqlx_core::{ext::ustr::UStr, type_info::TypeInfo};

use crate::ScyllaDBTypeInfo;

static TYPE_NAMES: LazyLock<RwLock<Vec<(TypeId, UStr)>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));

pub fn get_tuple_type_name(type_id: TypeId) -> Option<UStr> {
    TYPE_NAMES
        .read()
        .expect("tuple type name cache lock poisoned")
        .iter()
        .find(|(cached_type_id, _)| *cached_type_id == type_id)
        .map(|(_, type_name)| type_name.clone())
}

pub fn register_tuple_type_name(type_id: TypeId, type_infos: &[ScyllaDBTypeInfo]) -> UStr {
    let type_name = build_tuple_type_name(type_infos);

    TYPE_NAMES
        .write()
        .expect("tuple type name cache lock poisoned")
        .push((type_id, type_name.clone()));

    type_name
}

pub(crate) fn build_tuple_type_name(type_infos: &[ScyllaDBTypeInfo]) -> UStr {
    let mut type_name = String::from("(");
    for (i, type_info) in type_infos.iter().enumerate() {
        if i > 0 {
            type_name.push_str(", ");
        }
        let name = type_info.name();
        type_name.push_str(name);
    }
    type_name.push(')');
    UStr::new(&type_name)
}

macro_rules! impl_tuple {
    (
        $($typs:ident),*;
        $length:expr
    ) => {
        impl <$($typs),*> ::sqlx_core::types::Type<$crate::ScyllaDB> for ($($typs,)*)
        where $($typs: ::sqlx_core::types::Type<$crate::ScyllaDB> + 'static),* {
            fn type_info() -> $crate::ScyllaDBTypeInfo {
                let type_id = ::std::any::TypeId::of::<Self>();
                let type_name = $crate::types::tuple::get_tuple_type_name(type_id);

                match type_name {
                    Some(type_name) => {
                        $crate::ScyllaDBTypeInfo::Tuple(type_name)
                    }
                    None => {
                        let type_infos = &[$($typs::type_info()),*];
                        let type_name = $crate::types::tuple::register_tuple_type_name(type_id, type_infos);
                        $crate::ScyllaDBTypeInfo::Tuple(type_name)

                    }
                }
            }
        }

        impl<$($typs), *> ::sqlx_core::encode::Encode<'_, $crate::ScyllaDB> for ($($typs,)*)
        where $($typs: ::scylla::serialize::value::SerializeValue + Clone + Send + Sync + 'static,)* {
            fn encode(
                self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                buf.push($crate::ScyllaDBArgument::Tuple(::std::boxed::Box::new(self)));
                Ok(::sqlx_core::encode::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut $crate::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_core::encode::IsNull, ::sqlx_core::error::BoxDynError> {
                let argument = $crate::ScyllaDBArgument::Tuple(::std::boxed::Box::new(self.clone()));
                buf.push(argument);

                Ok(::sqlx_core::encode::IsNull::No)
            }
        }

        impl<$($typs),*> ::sqlx_core::decode::Decode<'_, $crate::ScyllaDB> for ($($typs,)*)
        where $($typs: for<'a> ::scylla::deserialize::value::DeserializeValue<'a, 'a>),* {
            fn decode(
                value: $crate::ScyllaDBValueRef<'_>,
            ) -> Result<Self, ::sqlx_core::error::BoxDynError> {
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
