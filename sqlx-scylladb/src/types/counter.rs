use scylla::value::Counter;
use sqlx::{Decode, Type, error::BoxDynError};

use crate::{ScyllaDB, ScyllaDBTypeInfo, ScyllaDBValueRef};

impl Type<ScyllaDB> for Counter {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Counter
    }
}

impl Decode<'_, ScyllaDB> for Counter {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}
