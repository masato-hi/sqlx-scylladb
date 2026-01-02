use scylla::{
    deserialize::value::DeserializeValue,
    value::{MaybeUnset, Unset},
};
use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

use crate::{
    ScyllaDB, ScyllaDBArgument, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
};

impl<T> Type<ScyllaDB> for MaybeUnset<T>
where
    T: Type<ScyllaDB>,
{
    fn type_info() -> ScyllaDBTypeInfo {
        T::type_info()
    }
}

impl Type<ScyllaDB> for Unset {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Unset
    }
}

impl<T> Decode<'_, ScyllaDB> for MaybeUnset<T>
where
    T: for<'de> DeserializeValue<'de, 'de>,
{
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Option<T> = value.deserialize()?;
        match val {
            Some(val) => Ok(MaybeUnset::Set(val)),
            None => Ok(MaybeUnset::Unset),
        }
    }
}

impl<'r, T> Encode<'r, ScyllaDB> for MaybeUnset<T>
where
    T: Encode<'r, ScyllaDB>,
{
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        match self {
            MaybeUnset::Set(val) => val.encode_by_ref(buf),
            MaybeUnset::Unset => {
                let argument = ScyllaDBArgument::Unset;
                buf.push(argument);

                Ok(IsNull::No)
            }
        }
    }
}

impl Encode<'_, ScyllaDB> for Unset {
    fn encode_by_ref(&self, buf: &mut ScyllaDBArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::Unset;
        buf.push(argument);

        Ok(IsNull::No)
    }
}
