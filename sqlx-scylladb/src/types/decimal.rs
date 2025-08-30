use bigdecimal_04::BigDecimal;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(
    BigDecimal,
    ScyllaDBTypeInfo::Decimal,
    ScyllaDBArgument::BigDecimal
);

impl_array_type!(
    BigDecimal,
    ScyllaDBTypeInfo::DecimalArray,
    ScyllaDBArgument::BigDecimalArray
);
