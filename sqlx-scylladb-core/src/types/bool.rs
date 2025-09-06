use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(bool, ScyllaDBTypeInfo::Boolean, ScyllaDBArgument::Boolean);

impl_array_type!(
    bool,
    ScyllaDBTypeInfo::BooleanArray,
    ScyllaDBArgument::BooleanArray
);
