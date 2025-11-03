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

#[cfg(test)]
mod tests {
    use scylla::{
        cluster::metadata::{ColumnType, NativeType},
        value::Counter,
    };

    use sqlx::{Decode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;

    use crate::{ScyllaDB, ScyllaDBTypeInfo, ScyllaDBValueRef, types::serialize_value};

    #[test]
    fn it_can_decode_counter() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Counter);
        let raw_value = serialize_value(&Counter(7), &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_counter"),
            ScyllaDBTypeInfo::Counter,
            &raw_value,
            &column_type,
        );
        let decoded: Counter = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded.0, 7);

        Ok(())
    }
}
