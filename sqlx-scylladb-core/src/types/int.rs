use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(i8, ScyllaDBTypeInfo::TinyInt, ScyllaDBArgument::TinyInt);

impl_array_type!(
    i8,
    ScyllaDBTypeInfo::TinyIntArray,
    ScyllaDBArgument::TinyIntArray
);

impl_type!(i16, ScyllaDBTypeInfo::SmallInt, ScyllaDBArgument::SmallInt);

impl_array_type!(
    i16,
    ScyllaDBTypeInfo::SmallIntArray,
    ScyllaDBArgument::SmallIntArray
);

impl_type!(i32, ScyllaDBTypeInfo::Int, ScyllaDBArgument::Int);

impl_array_type!(i32, ScyllaDBTypeInfo::IntArray, ScyllaDBArgument::IntArray);

impl_type!(i64, ScyllaDBTypeInfo::BigInt, ScyllaDBArgument::BigInt);

impl_array_type!(
    i64,
    ScyllaDBTypeInfo::BigIntArray,
    ScyllaDBArgument::BigIntArray
);
