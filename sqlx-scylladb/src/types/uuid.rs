use scylla::value::CqlTimeuuid;
use uuid::Uuid;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(Uuid, ScyllaDBTypeInfo::Uuid, ScyllaDBArgument::Uuid);

impl_array_type!(
    Uuid,
    ScyllaDBTypeInfo::UuidArray,
    ScyllaDBArgument::UuidArray
);

impl_type!(
    CqlTimeuuid,
    ScyllaDBTypeInfo::Timeuuid,
    ScyllaDBArgument::Timeuuid
);

impl_array_type!(
    CqlTimeuuid,
    ScyllaDBTypeInfo::TimeuuidArray,
    ScyllaDBArgument::TimeuuidArray
);
