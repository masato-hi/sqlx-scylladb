use scylla::value::CqlTimeuuid;
use uuid::Uuid;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(Uuid, ScyllaDBTypeInfo::Uuid, ScyllaDBArgument::Uuid);

impl_array_type!(
    Uuid,
    ScyllaDBTypeInfo::UuidArray,
    ScyllaDBArgument::UuidArray
);

impl_type!(
    CqlTimeuuid,
    ScyllaDBTypeInfo::Timeuuid,
    ScyllaDBArgument::Timeuuid
);

impl_array_type!(
    CqlTimeuuid,
    ScyllaDBTypeInfo::TimeuuidArray,
    ScyllaDBArgument::TimeuuidArray
);

#[cfg(test)]
mod tests {
    use std::{rc::Rc, str::FromStr, sync::Arc};

    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

    use sqlx::{Decode, Encode, error::BoxDynError};
    use sqlx_core::ext::ustr::UStr;
    use uuid::Uuid;

    use crate::{
        ScyllaDB, ScyllaDBArgumentBuffer, ScyllaDBTypeInfo, ScyllaDBValueRef,
        types::serialize_value,
    };

    #[test]
    fn it_can_encode_uuid() -> Result<(), BoxDynError> {
        let mut buf = ScyllaDBArgumentBuffer::default();

        let _ = <_ as Encode<'_, ScyllaDB>>::encode(Uuid::new_v4(), &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode([Uuid::new_v4(), Uuid::new_v4()], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(&[Uuid::new_v4(), Uuid::new_v4()], &mut buf)?;
        let _ =
            <_ as Encode<'_, ScyllaDB>>::encode(vec![Uuid::new_v4(), Uuid::new_v4()], &mut buf)?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Rc::new(vec![Uuid::new_v4(), Uuid::new_v4()]),
            &mut buf,
        )?;
        let _ = <_ as Encode<'_, ScyllaDB>>::encode(
            Arc::new(vec![Uuid::new_v4(), Uuid::new_v4()]),
            &mut buf,
        )?;

        Ok(())
    }

    #[test]
    fn it_can_decode_uuid() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Native(NativeType::Uuid);
        let raw_value = serialize_value(
            &Uuid::from_str("c53954ff-13aa-412c-844a-b97faca10ef6")?,
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_uuid"),
            ScyllaDBTypeInfo::Uuid,
            &raw_value,
            &column_type,
        );
        let decoded: Uuid = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(decoded.to_string(), "c53954ff-13aa-412c-844a-b97faca10ef6");

        Ok(())
    }

    #[test]
    fn it_can_decode_uuid_array() -> Result<(), BoxDynError> {
        let column_type: ColumnType<'_> = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Uuid))),
        };
        let raw_value = serialize_value(
            &vec![
                Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
                Uuid::from_str("f8e9f4c2-3f5d-4437-920a-8644efb72676")?,
            ],
            &column_type,
        )?;

        let value = ScyllaDBValueRef::new(
            UStr::new("my_uuid"),
            ScyllaDBTypeInfo::FloatArray,
            &raw_value,
            &column_type,
        );
        let decoded: Vec<Uuid> = <_ as Decode<'_, ScyllaDB>>::decode(value)?;
        assert_eq!(
            decoded,
            [
                Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
                Uuid::from_str("f8e9f4c2-3f5d-4437-920a-8644efb72676")?,
            ]
        );

        Ok(())
    }
}
