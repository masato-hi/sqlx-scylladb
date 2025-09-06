use scylla::value::CqlTime;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(CqlTime, ScyllaDBTypeInfo::Time, ScyllaDBArgument::CqlTime);

impl_array_type!(
    CqlTime,
    ScyllaDBTypeInfo::TimeArray,
    ScyllaDBArgument::CqlTimeArray
);

#[cfg(feature = "chrono-04")]
impl_type!(
    chrono_04::NaiveTime,
    ScyllaDBTypeInfo::Time,
    ScyllaDBArgument::ChronoNaiveTime
);

#[cfg(feature = "chrono-04")]
impl_array_type!(
    chrono_04::NaiveTime,
    ScyllaDBTypeInfo::TimeArray,
    ScyllaDBArgument::ChronoNaiveTimeArray
);

#[cfg(feature = "time-03")]
impl_type!(
    time_03::Time,
    ScyllaDBTypeInfo::Time,
    ScyllaDBArgument::Time
);

#[cfg(feature = "time-03")]
impl_array_type!(
    time_03::Time,
    ScyllaDBTypeInfo::TimeArray,
    ScyllaDBArgument::TimeArray
);
