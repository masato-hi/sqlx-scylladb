use sqlx::{Type, TypeInfo};

use crate::{ScyllaDB, ScyllaDBTypeInfo};

/// Provides information necessary to encode and decode ScyllaDB arrays as compatible Rust types.
pub trait ScyllaDBHasArrayType {
    #![allow(missing_docs)]
    fn array_type_info() -> ScyllaDBTypeInfo;
    fn array_compatible(ty: &ScyllaDBTypeInfo) -> bool {
        Self::array_type_info().type_compatible(ty)
    }
}

impl<T> ScyllaDBHasArrayType for &[T]
where
    T: ScyllaDBHasArrayType,
{
    fn array_type_info() -> ScyllaDBTypeInfo {
        T::array_type_info()
    }

    fn array_compatible(ty: &ScyllaDBTypeInfo) -> bool {
        T::array_compatible(ty)
    }
}

impl<T> ScyllaDBHasArrayType for Option<T>
where
    T: ScyllaDBHasArrayType,
{
    fn array_type_info() -> ScyllaDBTypeInfo {
        T::array_type_info()
    }

    fn array_compatible(ty: &ScyllaDBTypeInfo) -> bool {
        T::array_compatible(ty)
    }
}

impl<T> Type<ScyllaDB> for [T]
where
    T: ScyllaDBHasArrayType,
{
    fn type_info() -> ScyllaDBTypeInfo {
        T::array_type_info()
    }

    fn compatible(ty: &ScyllaDBTypeInfo) -> bool {
        T::array_compatible(ty)
    }
}

impl<T> Type<ScyllaDB> for Vec<T>
where
    T: ScyllaDBHasArrayType,
{
    fn type_info() -> ScyllaDBTypeInfo {
        T::array_type_info()
    }

    fn compatible(ty: &ScyllaDBTypeInfo) -> bool {
        T::array_compatible(ty)
    }
}

impl<T, const N: usize> Type<ScyllaDB> for [T; N]
where
    T: ScyllaDBHasArrayType,
{
    fn type_info() -> ScyllaDBTypeInfo {
        T::array_type_info()
    }

    fn compatible(ty: &ScyllaDBTypeInfo) -> bool {
        T::array_compatible(ty)
    }
}
