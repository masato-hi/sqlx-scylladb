use sqlx::Type;

use crate::{ScyllaDB, ScyllaDBTypeInfo};

pub trait ScyllaDBHasArrayType {
    fn array_type_info() -> ScyllaDBTypeInfo;
    fn array_compatible(ty: &ScyllaDBTypeInfo) -> bool {
        *ty == Self::array_type_info()
    }
}

impl<T> ScyllaDBHasArrayType for &T
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
