use std::net::IpAddr;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(IpAddr, ScyllaDBTypeInfo::Inet, ScyllaDBArgument::IpAddr);

impl_array_type!(
    IpAddr,
    ScyllaDBTypeInfo::InetArray,
    ScyllaDBArgument::IpAddrArray
);

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, rc::Rc, str::FromStr, sync::Arc};

    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
    };

    #[test]
    fn it_can_encode_duration() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(IpAddr::from_str("192.0.2.2")?, &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            [
                IpAddr::from_str("192.0.2.2")?,
                IpAddr::from_str("2001:db8::3")?,
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            &[
                IpAddr::from_str("192.0.2.2")?,
                IpAddr::from_str("2001:db8::3")?,
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            vec![
                IpAddr::from_str("192.0.2.2")?,
                IpAddr::from_str("2001:db8::3")?,
            ],
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![
                IpAddr::from_str("192.0.2.2")?,
                IpAddr::from_str("2001:db8::3")?,
            ]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![
                IpAddr::from_str("192.0.2.2")?,
                IpAddr::from_str("2001:db8::3")?,
            ]),
            &mut buf,
        )?;

        Ok(())
    }

    #[test]
    fn it_can_decode_inet() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Inet);
        let raw_value = serialize_value(&IpAddr::from_str("192.0.2.2")?, &column_type)?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_inet"),
            ScyllaDBTypeInfo::Inet,
            &raw_value,
            &column_type,
        );
        let decoded: IpAddr = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded, IpAddr::from_str("192.0.2.2")?);

        Ok(())
    }

    #[test]
    fn it_can_decode_inet_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Inet))),
        };
        let raw_value = serialize_value(
            &vec![
                IpAddr::from_str("192.0.2.2")?,
                IpAddr::from_str("2001:db8::3")?,
            ],
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_inet"),
            ScyllaDBTypeInfo::InetArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<IpAddr> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            [
                IpAddr::from_str("192.0.2.2")?,
                IpAddr::from_str("2001:db8::3")?,
            ]
        );

        Ok(())
    }
}
