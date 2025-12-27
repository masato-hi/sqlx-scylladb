use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use scylla::value::CqlTimestamp;
use sqlx::{Acquire, Column, Executor, FromRow, TypeInfo};
use sqlx_scylladb::{
    ScyllaDB, ScyllaDBArgument, ScyllaDBPool, ScyllaDBTypeInfo,
    ext::{
        scylla_cql::frame::response::result::{CollectionType, ColumnType, NativeType},
        sqlx::{Decode, Encode, Type, encode::IsNull},
        ustr::UStr,
    },
};
use sqlx_scylladb_core::register_any_type;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
struct AnyMap(HashMap<Uuid, CqlTimestamp>);

impl Deref for AnyMap {
    type Target = HashMap<Uuid, CqlTimestamp>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AnyMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Type<ScyllaDB> for AnyMap {
    fn type_info() -> <ScyllaDB as sqlx::Database>::TypeInfo {
        ScyllaDBTypeInfo::Any(UStr::Static("MAP<UUID, TIMESTAMP>"))
    }
}

impl Encode<'_, ScyllaDB> for AnyMap {
    fn encode_by_ref(
        &self,
        buf: &mut <ScyllaDB as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let argument = ScyllaDBArgument::Any(Arc::new(self.0.clone()));
        buf.push(argument);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ScyllaDB> for AnyMap {
    fn decode(
        value: <ScyllaDB as sqlx::Database>::ValueRef<'_>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let inner: HashMap<Uuid, CqlTimestamp> = value.deserialize()?;
        Ok(Self(inner))
    }
}

fn register_any_map_type() -> anyhow::Result<()> {
    let any_type = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Map(
            Box::new(ColumnType::Native(NativeType::Uuid)),
            Box::new(ColumnType::Native(NativeType::Timestamp)),
        ),
    };
    register_any_type(any_type, UStr::Static("MAP<UUID, TIMESTAMP>"))?;

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_any(pool: ScyllaDBPool) -> anyhow::Result<()> {
    register_any_map_type()?;

    let id = Uuid::new_v4();

    let key = Uuid::new_v4();
    let value = CqlTimestamp(1756625358255);
    let mut inner: HashMap<Uuid, CqlTimestamp> = HashMap::new();
    inner.insert(key, value);

    let any_map = AnyMap(inner);

    let _ = sqlx::query("INSERT INTO any_tests(my_id, my_any) VALUES(?, ?)")
        .bind(id)
        .bind(any_map.clone())
        .execute(&pool)
        .await?;

    let (my_id, my_any): (Uuid, AnyMap) =
        sqlx::query_as("SELECT my_id, my_any FROM any_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_id);
    assert_eq!(any_map, my_any);

    #[derive(FromRow)]
    struct AnyTest {
        my_id: Uuid,
        my_any: AnyMap,
    }

    let row: AnyTest = sqlx::query_as("SELECT my_id, my_any FROM any_tests WHERE my_id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(any_map, row.my_any);

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_any_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    register_any_map_type()?;

    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO any_tests(my_id, my_any) VALUES(?, ?)")
        .bind(id)
        .bind(None::<AnyMap>)
        .execute(&pool)
        .await?;

    let (my_id, my_any): (Uuid, Option<AnyMap>) =
        sqlx::query_as("SELECT my_id, my_any FROM any_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_id);
    assert!(my_any.is_none());

    let key = Uuid::new_v4();
    let value = CqlTimestamp(1756625358255);
    let mut inner: HashMap<Uuid, CqlTimestamp> = HashMap::new();
    inner.insert(key, value);

    let any_map = AnyMap(inner);

    let _ = sqlx::query("INSERT INTO any_tests(my_id, my_any) VALUES(?, ?)")
        .bind(id)
        .bind(Some(any_map.clone()))
        .execute(&pool)
        .await?;

    let (my_id, my_any): (Uuid, Option<AnyMap>) =
        sqlx::query_as("SELECT my_id, my_any FROM any_tests WHERE my_id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(id, my_id);
    assert_eq!(any_map, my_any.unwrap());

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_any(pool: ScyllaDBPool) -> anyhow::Result<()> {
    register_any_map_type()?;

    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn.describe("SELECT my_id, my_any FROM any_tests").await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_any", describe.columns()[1].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!(
        "MAP<UUID, TIMESTAMP>",
        describe.columns()[1].type_info().name()
    );

    Ok(())
}
