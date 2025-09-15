use std::{collections::HashMap, net::IpAddr, str::FromStr};

use sqlx::Column;
use sqlx::{Acquire, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_text_map(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let mut ascii_ascii: HashMap<String, String> = HashMap::new();
    let mut text_text: HashMap<String, String> = HashMap::new();
    let mut text_boolean: HashMap<String, bool> = HashMap::new();
    let mut text_tinyint: HashMap<String, i8> = HashMap::new();
    let mut text_smallint: HashMap<String, i16> = HashMap::new();
    let mut text_int: HashMap<String, i32> = HashMap::new();
    let mut text_bigint: HashMap<String, i64> = HashMap::new();
    let mut text_float: HashMap<String, f32> = HashMap::new();
    let mut text_double: HashMap<String, f64> = HashMap::new();
    let mut text_uuid: HashMap<String, Uuid> = HashMap::new();
    let mut text_inet: HashMap<String, IpAddr> = HashMap::new();

    ascii_ascii.insert(String::from("my_ascii"), String::from("Hello!"));
    text_text.insert(String::from("my_text"), String::from("こんにちは"));
    text_boolean.insert(String::from("my_boolean"), true);
    text_tinyint.insert(String::from("my_tinyint"), 2);
    text_smallint.insert(String::from("my_smallint"), 3);
    text_int.insert(String::from("my_int"), 5);
    text_bigint.insert(String::from("my_bigint"), 7);
    text_float.insert(String::from("my_float"), 11.5);
    text_double.insert(String::from("my_double"), 13.5);
    text_uuid.insert(
        String::from("my_uuid"),
        Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
    );
    text_inet.insert(String::from("my_inet"), IpAddr::from_str("192.0.2.2")?);

    let _ = sqlx::query(
        r#"
        INSERT INTO text_map_tests(
            my_id, my_ascii_ascii, my_text_text, my_text_boolean,
            my_text_tinyint, my_text_smallint, my_text_int, my_text_bigint,
            my_text_float, my_text_double, my_text_uuid, my_text_inet
        )
        VALUES(
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?
        )
        "#,
    )
    .bind(id)
    .bind(&ascii_ascii)
    .bind(&text_text)
    .bind(&text_boolean)
    .bind(&text_tinyint)
    .bind(&text_smallint)
    .bind(&text_int)
    .bind(&text_bigint)
    .bind(&text_float)
    .bind(&text_double)
    .bind(&text_uuid)
    .bind(&text_inet)
    .execute(&pool)
    .await?;

    let (
        my_id,
        my_ascii_ascii,
        my_text_text,
        my_text_boolean,
        my_text_tinyint,
        my_text_smallint,
        my_text_int,
        my_text_bigint,
        my_text_float,
        my_text_double,
        my_text_uuid,
        my_text_inet,
    ): (
        Uuid,
        HashMap<String, String>,
        HashMap<String, String>,
        HashMap<String, bool>,
        HashMap<String, i8>,
        HashMap<String, i16>,
        HashMap<String, i32>,
        HashMap<String, i64>,
        HashMap<String, f32>,
        HashMap<String, f64>,
        HashMap<String, Uuid>,
        HashMap<String, IpAddr>,
    ) = sqlx::query_as(
        r#"
            SELECT
                my_id, my_ascii_ascii, my_text_text, my_text_boolean,
                my_text_tinyint, my_text_smallint, my_text_int, my_text_bigint,
                my_text_float, my_text_double, my_text_uuid, my_text_inet
            FROM text_map_tests
            WHERE my_id = ?
        "#,
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(ascii_ascii, my_ascii_ascii);
    assert_eq!(text_text, my_text_text);
    assert_eq!(text_boolean, my_text_boolean);
    assert_eq!(text_tinyint, my_text_tinyint);
    assert_eq!(text_smallint, my_text_smallint);
    assert_eq!(text_int, my_text_int);
    assert_eq!(text_bigint, my_text_bigint);
    assert_eq!(text_float, my_text_float);
    assert_eq!(text_double, my_text_double);
    assert_eq!(text_uuid, my_text_uuid);
    assert_eq!(text_inet, my_text_inet);

    #[derive(FromRow)]
    struct TextMapTest {
        my_id: Uuid,
        my_ascii_ascii: HashMap<String, String>,
        my_text_text: HashMap<String, String>,
        my_text_boolean: HashMap<String, bool>,
        my_text_tinyint: HashMap<String, i8>,
        my_text_smallint: HashMap<String, i16>,
        my_text_int: HashMap<String, i32>,
        my_text_bigint: HashMap<String, i64>,
        my_text_float: HashMap<String, f32>,
        my_text_double: HashMap<String, f64>,
        my_text_uuid: HashMap<String, Uuid>,
        my_text_inet: HashMap<String, IpAddr>,
    }

    let row: TextMapTest = sqlx::query_as(
        r#"
            SELECT
                my_id, my_ascii_ascii, my_text_text, my_text_boolean,
                my_text_tinyint, my_text_smallint, my_text_int, my_text_bigint,
                my_text_float, my_text_double, my_text_uuid, my_text_inet
            FROM text_map_tests
            WHERE my_id = ?
        "#,
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(ascii_ascii, row.my_ascii_ascii);
    assert_eq!(text_text, row.my_text_text);
    assert_eq!(text_boolean, row.my_text_boolean);
    assert_eq!(text_tinyint, row.my_text_tinyint);
    assert_eq!(text_smallint, row.my_text_smallint);
    assert_eq!(text_int, row.my_text_int);
    assert_eq!(text_bigint, row.my_text_bigint);
    assert_eq!(text_float, row.my_text_float);
    assert_eq!(text_double, row.my_text_double);
    assert_eq!(text_uuid, row.my_text_uuid);
    assert_eq!(text_inet, row.my_text_inet);

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_text_map_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        r#"
        INSERT INTO text_map_tests(
            my_id, my_ascii_ascii, my_text_text, my_text_boolean,
            my_text_tinyint, my_text_smallint, my_text_int, my_text_bigint,
            my_text_float, my_text_double, my_text_uuid, my_text_inet
        )
        VALUES(
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?
        )
        "#,
    )
    .bind(id)
    .bind(None::<HashMap<String, String>>)
    .bind(None::<HashMap<String, String>>)
    .bind(None::<HashMap<String, bool>>)
    .bind(None::<HashMap<String, i8>>)
    .bind(None::<HashMap<String, i16>>)
    .bind(None::<HashMap<String, i32>>)
    .bind(None::<HashMap<String, i64>>)
    .bind(None::<HashMap<String, f32>>)
    .bind(None::<HashMap<String, f64>>)
    .bind(None::<HashMap<String, Uuid>>)
    .bind(None::<HashMap<String, IpAddr>>)
    .execute(&pool)
    .await?;

    let (
        my_id,
        my_ascii_ascii,
        my_text_text,
        my_text_boolean,
        my_text_tinyint,
        my_text_smallint,
        my_text_int,
        my_text_bigint,
        my_text_float,
        my_text_double,
        my_text_uuid,
        my_text_inet,
    ): (
        Uuid,
        Option<HashMap<String, String>>,
        Option<HashMap<String, String>>,
        Option<HashMap<String, bool>>,
        Option<HashMap<String, i8>>,
        Option<HashMap<String, i16>>,
        Option<HashMap<String, i32>>,
        Option<HashMap<String, i64>>,
        Option<HashMap<String, f32>>,
        Option<HashMap<String, f64>>,
        Option<HashMap<String, Uuid>>,
        Option<HashMap<String, IpAddr>>,
    ) = sqlx::query_as(
        r#"
            SELECT
                my_id, my_ascii_ascii, my_text_text, my_text_boolean,
                my_text_tinyint, my_text_smallint, my_text_int, my_text_bigint,
                my_text_float, my_text_double, my_text_uuid, my_text_inet
            FROM text_map_tests
            WHERE my_id = ?
        "#,
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_ascii_ascii.is_none());
    assert!(my_text_text.is_none());
    assert!(my_text_boolean.is_none());
    assert!(my_text_tinyint.is_none());
    assert!(my_text_smallint.is_none());
    assert!(my_text_int.is_none());
    assert!(my_text_bigint.is_none());
    assert!(my_text_float.is_none());
    assert!(my_text_double.is_none());
    assert!(my_text_uuid.is_none());
    assert!(my_text_inet.is_none());

    let mut ascii_ascii: HashMap<String, String> = HashMap::new();
    let mut text_text: HashMap<String, String> = HashMap::new();
    let mut text_boolean: HashMap<String, bool> = HashMap::new();
    let mut text_tinyint: HashMap<String, i8> = HashMap::new();
    let mut text_smallint: HashMap<String, i16> = HashMap::new();
    let mut text_int: HashMap<String, i32> = HashMap::new();
    let mut text_bigint: HashMap<String, i64> = HashMap::new();
    let mut text_float: HashMap<String, f32> = HashMap::new();
    let mut text_double: HashMap<String, f64> = HashMap::new();
    let mut text_uuid: HashMap<String, Uuid> = HashMap::new();
    let mut text_inet: HashMap<String, IpAddr> = HashMap::new();

    ascii_ascii.insert(String::from("my_ascii"), String::from("Hello!"));
    text_text.insert(String::from("my_text"), String::from("こんにちは"));
    text_boolean.insert(String::from("my_boolean"), true);
    text_tinyint.insert(String::from("my_tinyint"), 2);
    text_smallint.insert(String::from("my_smallint"), 3);
    text_int.insert(String::from("my_int"), 5);
    text_bigint.insert(String::from("my_bigint"), 7);
    text_float.insert(String::from("my_float"), 11.5);
    text_double.insert(String::from("my_double"), 13.5);
    text_uuid.insert(
        String::from("my_uuid"),
        Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
    );
    text_inet.insert(String::from("my_inet"), IpAddr::from_str("192.0.2.2")?);

    let _ = sqlx::query(
        r#"
        INSERT INTO text_map_tests(
            my_id, my_ascii_ascii, my_text_text, my_text_boolean,
            my_text_tinyint, my_text_smallint, my_text_int, my_text_bigint,
            my_text_float, my_text_double, my_text_uuid, my_text_inet
        )
        VALUES(
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?
        )
        "#,
    )
    .bind(id)
    .bind(Some(ascii_ascii.clone()))
    .bind(Some(text_text.clone()))
    .bind(Some(text_boolean.clone()))
    .bind(Some(text_tinyint.clone()))
    .bind(Some(text_smallint.clone()))
    .bind(Some(text_int.clone()))
    .bind(Some(text_bigint.clone()))
    .bind(Some(text_float.clone()))
    .bind(Some(text_double.clone()))
    .bind(Some(text_uuid.clone()))
    .bind(Some(text_inet.clone()))
    .execute(&pool)
    .await?;

    let (
        my_id,
        my_ascii_ascii,
        my_text_text,
        my_text_boolean,
        my_text_tinyint,
        my_text_smallint,
        my_text_int,
        my_text_bigint,
        my_text_float,
        my_text_double,
        my_text_uuid,
        my_text_inet,
    ): (
        Uuid,
        Option<HashMap<String, String>>,
        Option<HashMap<String, String>>,
        Option<HashMap<String, bool>>,
        Option<HashMap<String, i8>>,
        Option<HashMap<String, i16>>,
        Option<HashMap<String, i32>>,
        Option<HashMap<String, i64>>,
        Option<HashMap<String, f32>>,
        Option<HashMap<String, f64>>,
        Option<HashMap<String, Uuid>>,
        Option<HashMap<String, IpAddr>>,
    ) = sqlx::query_as(
        r#"
            SELECT
                my_id, my_ascii_ascii, my_text_text, my_text_boolean,
                my_text_tinyint, my_text_smallint, my_text_int, my_text_bigint,
                my_text_float, my_text_double, my_text_uuid, my_text_inet
            FROM text_map_tests
            WHERE my_id = ?
        "#,
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(ascii_ascii, my_ascii_ascii.unwrap());
    assert_eq!(text_text, my_text_text.unwrap());
    assert_eq!(text_boolean, my_text_boolean.unwrap());
    assert_eq!(text_tinyint, my_text_tinyint.unwrap());
    assert_eq!(text_smallint, my_text_smallint.unwrap());
    assert_eq!(text_int, my_text_int.unwrap());
    assert_eq!(text_bigint, my_text_bigint.unwrap());
    assert_eq!(text_float, my_text_float.unwrap());
    assert_eq!(text_double, my_text_double.unwrap());
    assert_eq!(text_uuid, my_text_uuid.unwrap());
    assert_eq!(text_inet, my_text_inet.unwrap());

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_text_map(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe(
            r#"
            SELECT
                my_id, my_ascii_ascii, my_text_text, my_text_boolean,
                my_text_tinyint, my_text_smallint, my_text_int, my_text_bigint,
                my_text_float, my_text_double, my_text_uuid, my_text_inet
            FROM text_map_tests
            WHERE my_id = ?
        "#,
        )
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_ascii_ascii", describe.columns()[1].name());
    assert_eq!("my_text_text", describe.columns()[2].name());
    assert_eq!("my_text_boolean", describe.columns()[3].name());
    assert_eq!("my_text_tinyint", describe.columns()[4].name());
    assert_eq!("my_text_smallint", describe.columns()[5].name());
    assert_eq!("my_text_int", describe.columns()[6].name());
    assert_eq!("my_text_bigint", describe.columns()[7].name());
    assert_eq!("my_text_float", describe.columns()[8].name());
    assert_eq!("my_text_double", describe.columns()[9].name());
    assert_eq!("my_text_uuid", describe.columns()[10].name());
    assert_eq!("my_text_inet", describe.columns()[11].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!(
        "MAP<ASCII, ASCII>",
        describe.columns()[1].type_info().name()
    );
    assert_eq!("MAP<TEXT, TEXT>", describe.columns()[2].type_info().name());
    assert_eq!(
        "MAP<TEXT, BOOLEAN>",
        describe.columns()[3].type_info().name()
    );
    assert_eq!(
        "MAP<TEXT, TINYINT>",
        describe.columns()[4].type_info().name()
    );
    assert_eq!(
        "MAP<TEXT, SMALLINT>",
        describe.columns()[5].type_info().name()
    );
    assert_eq!("MAP<TEXT, INT>", describe.columns()[6].type_info().name());
    assert_eq!(
        "MAP<TEXT, BIGINT>",
        describe.columns()[7].type_info().name()
    );
    assert_eq!("MAP<TEXT, FLOAT>", describe.columns()[8].type_info().name());
    assert_eq!(
        "MAP<TEXT, DOUBLE>",
        describe.columns()[9].type_info().name()
    );
    assert_eq!("MAP<TEXT, UUID>", describe.columns()[10].type_info().name());
    assert_eq!("MAP<TEXT, INET>", describe.columns()[11].type_info().name());

    Ok(())
}
