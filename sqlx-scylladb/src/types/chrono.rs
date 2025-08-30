use chrono_04::{DateTime, NaiveDate, NaiveTime, Utc};

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(
    DateTime<Utc>,
    ScyllaDBTypeInfo::Timestamp,
    ScyllaDBArgument::ChronoDateTimeUTC
);

impl_array_type!(
    DateTime<Utc>,
    ScyllaDBTypeInfo::TimestampArray,
    ScyllaDBArgument::ChronoDateTimeUTCArray
);

impl_type!(
    NaiveDate,
    ScyllaDBTypeInfo::Date,
    ScyllaDBArgument::ChronoNaiveDate
);

impl_array_type!(
    NaiveDate,
    ScyllaDBTypeInfo::DateArray,
    ScyllaDBArgument::ChronoNaiveDateArray
);

impl_type!(
    NaiveTime,
    ScyllaDBTypeInfo::Time,
    ScyllaDBArgument::ChronoNaiveTime
);

impl_array_type!(
    NaiveTime,
    ScyllaDBTypeInfo::TimeArray,
    ScyllaDBArgument::ChronoNaiveTimeArray
);
