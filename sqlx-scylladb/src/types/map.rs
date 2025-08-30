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

// UUID
impl_map_type!(
    Uuid,
    String,
    ScyllaDBTypeInfo::UuidTextMap,
    ScyllaDBArgument::UuidTextMap
);

impl_map_type!(
    Uuid,
    bool,
    ScyllaDBTypeInfo::UuidBooleanMap,
    ScyllaDBArgument::UuidBooleanMap
);

impl_map_type!(
    Uuid,
    i8,
    ScyllaDBTypeInfo::UuidTinyIntMap,
    ScyllaDBArgument::UuidTinyIntMap
);

impl_map_type!(
    Uuid,
    i16,
    ScyllaDBTypeInfo::UuidSmallIntMap,
    ScyllaDBArgument::UuidSmallIntMap
);

impl_map_type!(
    Uuid,
    i32,
    ScyllaDBTypeInfo::UuidIntMap,
    ScyllaDBArgument::UuidIntMap
);

impl_map_type!(
    Uuid,
    i64,
    ScyllaDBTypeInfo::UuidBigIntMap,
    ScyllaDBArgument::UuidBigIntMap
);

impl_map_type!(
    Uuid,
    f32,
    ScyllaDBTypeInfo::UuidFloatMap,
    ScyllaDBArgument::UuidFloatMap
);

impl_map_type!(
    Uuid,
    f64,
    ScyllaDBTypeInfo::UuidDoubleMap,
    ScyllaDBArgument::UuidDoubleMap
);

impl_map_type!(
    Uuid,
    Uuid,
    ScyllaDBTypeInfo::UuidUuidMap,
    ScyllaDBArgument::UuidUuidMap
);
