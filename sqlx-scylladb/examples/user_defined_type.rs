use std::env;

use sqlx::migrate::Migrator;

use sqlx_scylladb::{
    ScyllaDBPoolOptions,
    ext::scylla::{DeserializeValue, SerializeValue},
    macros::UserDefinedType,
};

#[derive(Debug, Clone, SerializeValue, DeserializeValue, UserDefinedType)]
#[user_defined_type(name = "example_user_defined_type")]
pub(crate) struct MyUDT {
    pub id: i64,
    pub name: String,
}

static MIGRATOR: Migrator = sqlx::migrate!("examples/migrations");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = env::var("SCYLLADB_URL")?;

    let pool = ScyllaDBPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    MIGRATOR.run(&pool).await?;

    let udt = MyUDT {
        id: 1,
        name: String::from("Alice"),
    };

    let mut udt_list = Vec::new();
    udt_list.push(MyUDT {
        id: 2,
        name: String::from("Bob"),
    });
    udt_list.push(MyUDT {
        id: 3,
        name: String::from("Charlie"),
    });

    sqlx::query("INSERT INTO example_user_defined_types(id, my_udt, my_udt_list) VALUES(?, ?, ?)")
        .bind(1i64)
        .bind(udt)
        .bind(udt_list)
        .execute(&pool)
        .await?;

    #[derive(Debug, sqlx::FromRow)]
    struct ExampleUDTRow {
        id: i64,
        my_udt: MyUDT,
        my_udt_list: Vec<MyUDT>,
    }

    let row: ExampleUDTRow = sqlx::query_as(
        "SELECT id, my_udt, my_udt_list FROM example_user_defined_types WHERE id = ?",
    )
    .bind(1i64)
    .fetch_one(&pool)
    .await?;

    println!("id: {}", row.id);
    println!(
        "my_udt.id: {}, my_udt.name: {}",
        row.my_udt.id, row.my_udt.name
    );
    println!("my_udt_list: {:?}", row.my_udt_list);

    Ok(())
}
