use scylla::value::CqlDuration;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(
    CqlDuration,
    ScyllaDBTypeInfo::Duration,
    ScyllaDBArgument::Duration
);

impl_array_type!(
    CqlDuration,
    ScyllaDBTypeInfo::DurationArray,
    ScyllaDBArgument::DurationArray
);
