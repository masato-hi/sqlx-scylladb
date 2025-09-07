#[cfg(feature = "bigdecimal-04")]
impl_type!(
    bigdecimal_04::BigDecimal,
    crate::ScyllaDBTypeInfo::Decimal,
    crate::ScyllaDBArgument::BigDecimal
);

#[cfg(feature = "bigdecimal-04")]
impl_array_type!(
    bigdecimal_04::BigDecimal,
    crate::ScyllaDBTypeInfo::DecimalArray,
    crate::ScyllaDBArgument::BigDecimalArray
);
