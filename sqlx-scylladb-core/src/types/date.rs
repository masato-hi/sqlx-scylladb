use scylla::value::CqlDate;

use crate::{ScyllaDBArgument, ScyllaDBTypeInfo};

impl_type!(CqlDate, ScyllaDBTypeInfo::Date, ScyllaDBArgument::CqlDate);

impl_array_type!(
    CqlDate,
    ScyllaDBTypeInfo::DateArray,
    ScyllaDBArgument::CqlDateArray
);

#[cfg(feature = "chrono-04")]
impl_type!(
    chrono_04::NaiveDate,
    ScyllaDBTypeInfo::Date,
    ScyllaDBArgument::ChronoNaiveDate
);

#[cfg(feature = "chrono-04")]
impl_array_type!(
    chrono_04::NaiveDate,
    ScyllaDBTypeInfo::DateArray,
    ScyllaDBArgument::ChronoNaiveDateArray
);

#[cfg(feature = "time-03")]
impl_type!(
    time_03::Date,
    ScyllaDBTypeInfo::Date,
    ScyllaDBArgument::Date
);

#[cfg(feature = "time-03")]
impl_array_type!(
    time_03::Date,
    ScyllaDBTypeInfo::DateArray,
    ScyllaDBArgument::DateArray
);
