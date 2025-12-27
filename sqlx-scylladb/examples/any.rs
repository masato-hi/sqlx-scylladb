use std::{
    collections::HashMap,
    env,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use sqlx::{
    FromRow,
    migrate::{MigrateDatabase, Migrator},
};
use sqlx_scylladb::{
    ScyllaDB, ScyllaDBArgument, ScyllaDBExecutor, ScyllaDBPoolOptions, ScyllaDBTypeInfo,
    ext::{
        scylla_cql::frame::response::result::{CollectionType, ColumnType, NativeType},
        sqlx::{Decode, Encode, Type, encode::IsNull},
        ustr::UStr,
    },
};
use sqlx_scylladb_core::register_any_type;

#[derive(Debug, Clone, PartialEq)]
struct AnyMap(HashMap<i64, String>);

impl Deref for AnyMap {
    type Target = HashMap<i64, String>;

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
        ScyllaDBTypeInfo::Any(UStr::Static("MAP<BIGINT, TEXT>"))
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
        let inner: HashMap<i64, String> = value.deserialize()?;
        Ok(Self(inner))
    }
}

static MIGRATOR: Migrator = sqlx::migrate!("examples/migrations");

#[derive(FromRow)]
struct AnyData {
    id: i64,
    any_map: AnyMap,
}

fn register_any_map_type() -> anyhow::Result<()> {
    let any_type = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Map(
            Box::new(ColumnType::Native(NativeType::BigInt)),
            Box::new(ColumnType::Native(NativeType::Text)),
        ),
    };
    register_any_type(any_type, UStr::Static("MAP<BIGINT, TEXT>"))?;

    Ok(())
}

async fn create_any_data(
    conn: impl ScyllaDBExecutor<'_>,
    id: i64,
    key: i64,
    value: impl Into<String>,
) -> anyhow::Result<AnyData> {
    let mut any_inner: HashMap<i64, String> = HashMap::new();
    any_inner.insert(key, value.into());
    let any_map = AnyMap(any_inner);
    let any_data = AnyData { id, any_map };

    sqlx::query("INSERT INTO example_any(id, any_map) VALUES(?, ?)")
        .bind(id)
        .bind(any_data.any_map.clone())
        .execute(conn)
        .await?;

    Ok(any_data)
}

async fn find_any_data(conn: impl ScyllaDBExecutor<'_>, id: i64) -> anyhow::Result<AnyData> {
    let any_data = sqlx::query_as::<_, AnyData>("SELECT id, any_map FROM example_any WHERE id = ?")
        .bind(id)
        .fetch_one(conn)
        .await?;

    Ok(any_data)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = env::var("SCYLLADB_URL")?;

    ScyllaDB::create_database(&database_url).await?;

    let pool = ScyllaDBPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    MIGRATOR.run(&pool).await?;

    register_any_map_type()?;

    let _ = create_any_data(&pool, 1, 2, "Alice").await?;

    let any_data = find_any_data(&pool, 1).await?;

    let any_map_value = any_data.any_map.get(&2).unwrap();
    println!("id: {}, any_map[2]: {}", any_data.id, any_map_value);

    Ok(())
}
