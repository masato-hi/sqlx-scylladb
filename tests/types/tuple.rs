use std::{net::IpAddr, str::FromStr};

use scylla::{
    DeserializeValue, SerializeValue,
    value::{CqlDate, CqlTime, CqlTimestamp},
};
use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use sqlx_scylladb_macros::UserDefinedType;
use uuid::Uuid;

#[derive(
    Debug, PartialEq, Eq, Clone, FromRow, SerializeValue, DeserializeValue, UserDefinedType,
)]
struct MyUserDefinedType {
    my_bigint: i64,
    my_text: String,
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_tuple(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO tuple_tests(my_id, my_tuple) VALUES(?, ?)")
        .bind(id)
        .bind((
            true,
            2i8,
            3i16,
            5i32,
            7i64,
            11.5f32,
            13.5f64,
            Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
            CqlTimestamp(1756625358255),
            CqlDate(20330),
            CqlTime(27874),
            "Hello!",
            "こんにちは",
            IpAddr::from_str("192.0.2.2")?,
            [0x00u8, 0x61, 0x73, 0x6d],
            MyUserDefinedType {
                my_bigint: 1,
                my_text: String::from("Hello!"),
            },
        ))
        .execute(&pool)
        .await?;

    let (my_id, my_tuple): (
        Uuid,
        (
            bool,
            i8,
            i16,
            i32,
            i64,
            f32,
            f64,
            Uuid,
            CqlTimestamp,
            CqlDate,
            CqlTime,
            String,
            String,
            IpAddr,
            Vec<u8>,
            MyUserDefinedType,
        ),
    ) = sqlx::query_as("SELECT my_id, my_tuple FROM tuple_tests WHERE my_id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(true, my_tuple.0);
    assert_eq!(2, my_tuple.1);
    assert_eq!(3, my_tuple.2);
    assert_eq!(5, my_tuple.3);
    assert_eq!(7, my_tuple.4);
    assert_eq!(11.5, my_tuple.5);
    assert_eq!(13.5, my_tuple.6);
    assert_eq!(
        Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
        my_tuple.7
    );
    assert_eq!(CqlTimestamp(1756625358255), my_tuple.8);
    assert_eq!(CqlDate(20330), my_tuple.9);
    assert_eq!(CqlTime(27874), my_tuple.10);
    assert_eq!("Hello!", my_tuple.11);
    assert_eq!("こんにちは", my_tuple.12);
    assert_eq!(IpAddr::from_str("192.0.2.2")?, my_tuple.13);
    assert_eq!(vec![0x00u8, 0x61, 0x73, 0x6d], my_tuple.14);
    assert_eq!(
        MyUserDefinedType {
            my_bigint: 1,
            my_text: String::from("Hello!"),
        },
        my_tuple.15
    );

    #[derive(FromRow)]
    struct TupleTest {
        my_id: Uuid,
        my_tuple: (
            bool,
            i8,
            i16,
            i32,
            i64,
            f32,
            f64,
            Uuid,
            CqlTimestamp,
            CqlDate,
            CqlTime,
            String,
            String,
            IpAddr,
            Vec<u8>,
            MyUserDefinedType,
        ),
    }

    let row: TupleTest = sqlx::query_as("SELECT my_id, my_tuple FROM tuple_tests WHERE my_id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(true, row.my_tuple.0);
    assert_eq!(2, row.my_tuple.1);
    assert_eq!(3, row.my_tuple.2);
    assert_eq!(5, row.my_tuple.3);
    assert_eq!(7, row.my_tuple.4);
    assert_eq!(11.5, row.my_tuple.5);
    assert_eq!(13.5, row.my_tuple.6);
    assert_eq!(
        Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
        row.my_tuple.7
    );
    assert_eq!(CqlTimestamp(1756625358255), row.my_tuple.8);
    assert_eq!(CqlDate(20330), row.my_tuple.9);
    assert_eq!(CqlTime(27874), row.my_tuple.10);
    assert_eq!("Hello!", row.my_tuple.11);
    assert_eq!("こんにちは", row.my_tuple.12);
    assert_eq!(IpAddr::from_str("192.0.2.2")?, row.my_tuple.13);
    assert_eq!(vec![0x00u8, 0x61, 0x73, 0x6d], row.my_tuple.14);
    assert_eq!(
        MyUserDefinedType {
            my_bigint: 1,
            my_text: String::from("Hello!"),
        },
        row.my_tuple.15
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_tuple_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query("INSERT INTO tuple_tests(my_id, my_tuple) VALUES(?, ?)")
        .bind(id)
        .bind(
            None::<(
                bool,
                i8,
                i16,
                i32,
                i64,
                f32,
                f64,
                Uuid,
                CqlTimestamp,
                CqlDate,
                CqlTime,
                String,
                String,
                IpAddr,
                Vec<u8>,
                MyUserDefinedType,
            )>,
        )
        .execute(&pool)
        .await?;

    let (my_id, my_tuple): (
        Uuid,
        Option<(
            bool,
            i8,
            i16,
            i32,
            i64,
            f32,
            f64,
            Uuid,
            CqlTimestamp,
            CqlDate,
            CqlTime,
            String,
            String,
            IpAddr,
            Vec<u8>,
            MyUserDefinedType,
        )>,
    ) = sqlx::query_as("SELECT my_id, my_tuple FROM tuple_tests WHERE my_id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert!(my_tuple.is_none());

    let _ = sqlx::query("INSERT INTO tuple_tests(my_id, my_tuple) VALUES(?, ?)")
        .bind(id)
        .bind(Some((
            true,
            2i8,
            3i16,
            5i32,
            7i64,
            11.5f32,
            13.5f64,
            Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
            CqlTimestamp(1756625358255),
            CqlDate(20330),
            CqlTime(27874),
            "Hello!",
            "こんにちは",
            IpAddr::from_str("192.0.2.2")?,
            [0x00u8, 0x61, 0x73, 0x6d],
            MyUserDefinedType {
                my_bigint: 1,
                my_text: String::from("Hello!"),
            },
        )))
        .execute(&pool)
        .await?;

    let (my_id, my_tuple): (
        Uuid,
        Option<(
            bool,
            i8,
            i16,
            i32,
            i64,
            f32,
            f64,
            Uuid,
            CqlTimestamp,
            CqlDate,
            CqlTime,
            String,
            String,
            IpAddr,
            Vec<u8>,
            MyUserDefinedType,
        )>,
    ) = sqlx::query_as("SELECT my_id, my_tuple FROM tuple_tests WHERE my_id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    let my_tuple = my_tuple.unwrap();
    assert_eq!(true, my_tuple.0);
    assert_eq!(2, my_tuple.1);
    assert_eq!(3, my_tuple.2);
    assert_eq!(5, my_tuple.3);
    assert_eq!(7, my_tuple.4);
    assert_eq!(11.5, my_tuple.5);
    assert_eq!(13.5, my_tuple.6);
    assert_eq!(
        Uuid::from_str("7d814b8f-1894-4b97-927c-83e82cb6735b")?,
        my_tuple.7
    );
    assert_eq!(CqlTimestamp(1756625358255), my_tuple.8);
    assert_eq!(CqlDate(20330), my_tuple.9);
    assert_eq!(CqlTime(27874), my_tuple.10);
    assert_eq!("Hello!", my_tuple.11);
    assert_eq!("こんにちは", my_tuple.12);
    assert_eq!(IpAddr::from_str("192.0.2.2")?, my_tuple.13);
    assert_eq!(vec![0x00u8, 0x61, 0x73, 0x6d], my_tuple.14);
    assert_eq!(
        MyUserDefinedType {
            my_bigint: 1,
            my_text: String::from("Hello!"),
        },
        my_tuple.15
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_tuple(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_tuple FROM tuple_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_tuple", describe.columns()[1].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!(
        "TUPLE<BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, UUID, TIMESTAMP, DATE, TIME, ASCII, TEXT, INET, BLOB, my_user_defined_type>",
        describe.columns()[1].type_info().name()
    );

    Ok(())
}
