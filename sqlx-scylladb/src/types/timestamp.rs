use scylla::value::CqlTimestamp;

use crate::{ScyllaDBArgument, ScyllaDBTypeInfo};

impl_type!(
    CqlTimestamp,
    ScyllaDBTypeInfo::Timestamp,
    ScyllaDBArgument::CqlTimestamp
);

impl_array_type!(
    CqlTimestamp,
    ScyllaDBTypeInfo::TimestampArray,
    ScyllaDBArgument::CqlTimestampArray
);

#[cfg(feature = "chrono-04")]
impl_type!(
    chrono_04::DateTime<chrono_04::Utc>,
    ScyllaDBTypeInfo::Timestamp,
    ScyllaDBArgument::ChronoDateTimeUTC
);

#[cfg(feature = "chrono-04")]
impl_array_type!(
    chrono_04::DateTime<chrono_04::Utc>,
    ScyllaDBTypeInfo::TimestampArray,
    ScyllaDBArgument::ChronoDateTimeUTCArray
);

#[cfg(feature = "time-03")]
impl_type!(
    time_03::OffsetDateTime,
    ScyllaDBTypeInfo::Timestamp,
    ScyllaDBArgument::OffsetDateTime
);

#[cfg(feature = "time-03")]
impl_array_type!(
    time_03::OffsetDateTime,
    ScyllaDBTypeInfo::TimestampArray,
    ScyllaDBArgument::OffsetDateTimeArray
);
