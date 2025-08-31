use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use sqlx::{Decode, Encode, Type, encode::IsNull, error::BoxDynError};

use crate::{
    ScyllaDB, ScyllaDBError, ScyllaDBTypeInfo, ScyllaDBValueRef, arguments::ScyllaDBArgument,
};

impl Type<ScyllaDB> for Ipv4Addr {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Inet
    }
}

impl Encode<'_, ScyllaDB> for Ipv4Addr {
    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::IpAddr((*self).into());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for Ipv4Addr {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: IpAddr = value.deserialize()?;
        if let IpAddr::V4(val) = val {
            Ok(val)
        } else {
            Err(Box::new(ScyllaDBError::MismatchedColumnTypeError(
                value.column_name(),
                value.column_type(),
            )))
        }
    }
}

impl Type<ScyllaDB> for Ipv6Addr {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Inet
    }
}

impl Encode<'_, ScyllaDB> for Ipv6Addr {
    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::IpAddr((*self).into());
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for Ipv6Addr {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: IpAddr = value.deserialize()?;
        if let IpAddr::V6(val) = val {
            Ok(val)
        } else {
            Err(Box::new(ScyllaDBError::MismatchedColumnTypeError(
                value.column_name(),
                value.column_type(),
            )))
        }
    }
}

impl Type<ScyllaDB> for IpAddr {
    fn type_info() -> ScyllaDBTypeInfo {
        ScyllaDBTypeInfo::Inet
    }
}

impl Encode<'_, ScyllaDB> for IpAddr {
    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<IsNull, BoxDynError> {
        let argument = ScyllaDBArgument::IpAddr(*self);
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for IpAddr {
    fn decode(value: ScyllaDBValueRef<'_>) -> Result<Self, BoxDynError> {
        let val: Self = value.deserialize()?;
        Ok(val)
    }
}
