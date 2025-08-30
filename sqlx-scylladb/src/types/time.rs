use time_03::{Date, OffsetDateTime, Time};

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(
    OffsetDateTime,
    ScyllaDBTypeInfo::Timestamp,
    ScyllaDBArgument::OffsetDateTime
);

impl_array_type!(
    OffsetDateTime,
    ScyllaDBTypeInfo::TimestampArray,
    ScyllaDBArgument::OffsetDateTimeArray
);

impl_type!(Date, ScyllaDBTypeInfo::Date, ScyllaDBArgument::Date);

impl_array_type!(
    Date,
    ScyllaDBTypeInfo::DateArray,
    ScyllaDBArgument::DateArray
);

impl_type!(Time, ScyllaDBTypeInfo::Time, ScyllaDBArgument::Time);

impl_array_type!(
    Time,
    ScyllaDBTypeInfo::TimeArray,
    ScyllaDBArgument::TimeArray
);
