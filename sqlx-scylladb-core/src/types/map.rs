use std::net::IpAddr;

use uuid::Uuid;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

// String
impl_map_type!(
    String,
    String,
    ScyllaDBTypeInfo::TextTextMap,
    ScyllaDBArgument::TextTextMap
);

impl_map_type!(
    String,
    bool,
    ScyllaDBTypeInfo::TextBooleanMap,
    ScyllaDBArgument::TextBooleanMap
);

impl_map_type!(
    String,
    i8,
    ScyllaDBTypeInfo::TextTinyIntMap,
    ScyllaDBArgument::TextTinyIntMap
);

impl_map_type!(
    String,
    i16,
    ScyllaDBTypeInfo::TextSmallIntMap,
    ScyllaDBArgument::TextSmallIntMap
);

impl_map_type!(
    String,
    i32,
    ScyllaDBTypeInfo::TextIntMap,
    ScyllaDBArgument::TextIntMap
);

impl_map_type!(
    String,
    i64,
    ScyllaDBTypeInfo::TextBigIntMap,
    ScyllaDBArgument::TextBigIntMap
);

impl_map_type!(
    String,
    f32,
    ScyllaDBTypeInfo::TextFloatMap,
    ScyllaDBArgument::TextFloatMap
);

impl_map_type!(
    String,
    f64,
    ScyllaDBTypeInfo::TextDoubleMap,
    ScyllaDBArgument::TextDoubleMap
);

impl_map_type!(
    String,
    Uuid,
    ScyllaDBTypeInfo::TextUuidMap,
    ScyllaDBArgument::TextUuidMap
);

impl_map_type!(
    String,
    IpAddr,
    ScyllaDBTypeInfo::TextInetMap,
    ScyllaDBArgument::TextInetMap
);
