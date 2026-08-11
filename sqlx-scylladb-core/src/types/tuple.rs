use std::{
    any::TypeId,
    sync::{LazyLock, RwLock},
};

use rustc_hash::FxHashMap;
use scylla::cluster::metadata::ColumnType;
use sqlx_core::{ext::ustr::UStr, type_info::TypeInfo};

use crate::{ScyllaDBError, ScyllaDBTypeInfo};

static TYPE_INFO_NAMES: LazyLock<RwLock<Vec<(TypeId, UStr)>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));
static TYPE_INFO_NAMES_FROM_COLUMN_TYPES: LazyLock<RwLock<FxHashMap<Vec<ScyllaDBTypeInfo>, UStr>>> =
    LazyLock::new(|| RwLock::new(FxHashMap::default()));

fn get_tuple_type_info_name(type_id: TypeId) -> Option<UStr> {
    TYPE_INFO_NAMES
        .read()
        .expect("tuple type name cache lock poisoned")
        .iter()
        .find(|(cached_type_id, _)| *cached_type_id == type_id)
        .map(|(_, type_name)| type_name.clone())
}

fn register_tuple_type_info_name(type_id: TypeId, type_infos: &[ScyllaDBTypeInfo]) -> UStr {
    let type_info_name = build_tuple_type_info_name(type_infos);

    TYPE_INFO_NAMES
        .write()
        .expect("tuple type name cache lock poisoned")
        .push((type_id, type_info_name.clone()));

    type_info_name
}

fn build_tuple_type_info_name(type_infos: &[ScyllaDBTypeInfo]) -> UStr {
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

impl ScyllaDBTypeInfo {
    pub(crate) fn tuple_type_info_name_from_column_types(
        items: &Vec<ColumnType<'_>>,
    ) -> Result<Self, ScyllaDBError> {
        let mut type_infos = Vec::with_capacity(items.capacity());
        for item in items {
            let type_info = Self::from_column_type(item)?;
            type_infos.push(type_info);
        }

        if let Some(type_name) = TYPE_INFO_NAMES_FROM_COLUMN_TYPES
            .read()
            .expect("tuple type name cache lock poisoned")
            .get(&type_infos)
            .cloned()
        {
            return Ok(Self::Tuple(type_name));
        }

        let type_name = build_tuple_type_info_name(&type_infos);

        TYPE_INFO_NAMES_FROM_COLUMN_TYPES
            .write()
            .expect("tuple type name cache lock poisoned")
            .insert(type_infos, type_name.clone());

        Ok(Self::Tuple(type_name))
    }
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
                let type_name = $crate::types::tuple::get_tuple_type_info_name(type_id);

                match type_name {
                    Some(type_name) => {
                        $crate::ScyllaDBTypeInfo::Tuple(type_name)
                    }
                    None => {
                        let type_infos = &[$($typs::type_info()),*];
                        let type_name = $crate::types::tuple::register_tuple_type_info_name(type_id, type_infos);
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
