use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(f32, ScyllaDBTypeInfo::Float, ScyllaDBArgument::Float);

impl_array_type!(
    f32,
    ScyllaDBTypeInfo::FloatArray,
    ScyllaDBArgument::FloatArray
);

impl_type!(f64, ScyllaDBTypeInfo::Double, ScyllaDBArgument::Double);

impl_array_type!(
    f64,
    ScyllaDBTypeInfo::DoubleArray,
    ScyllaDBArgument::DoubleArray
);
