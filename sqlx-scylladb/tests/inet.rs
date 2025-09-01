use std::{net::IpAddr, str::FromStr};

use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_inet(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO inet_tests(my_id, my_inet, my_inet_list, my_inet_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(IpAddr::from_str("127.0.0.1")?)
    .bind([
        IpAddr::from_str("127.0.0.1")?,
        IpAddr::from_str("192.0.2.2")?,
        IpAddr::from_str("2001:db8::3")?,
        IpAddr::from_str("198.51.100.4")?,
    ])
    .bind([
        IpAddr::from_str("127.0.0.1")?,
        IpAddr::from_str("192.0.2.2")?,
        IpAddr::from_str("2001:db8::3")?,
        IpAddr::from_str("127.0.0.1")?,
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_inet, my_inet_list, my_inet_set): (Uuid, IpAddr, Vec<IpAddr>, Vec<IpAddr>) =
        sqlx::query_as(
            "SELECT my_id, my_inet, my_inet_list, my_inet_set FROM inet_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(IpAddr::from_str("127.0.0.1")?, my_inet);
    assert_eq!(
        vec![
            IpAddr::from_str("127.0.0.1")?,
            IpAddr::from_str("192.0.2.2")?,
            IpAddr::from_str("2001:db8::3")?,
            IpAddr::from_str("198.51.100.4")?,
        ],
        my_inet_list
    );
    assert_eq!(
        vec![
            IpAddr::from_str("2001:db8::3")?,
            IpAddr::from_str("127.0.0.1")?,
            IpAddr::from_str("192.0.2.2")?,
        ],
        my_inet_set
    );

    #[derive(FromRow)]
    struct IntTest {
        my_id: Uuid,
        my_inet: IpAddr,
        my_inet_list: Vec<IpAddr>,
        my_inet_set: Vec<IpAddr>,
    }

    let row: IntTest = sqlx::query_as(
        "SELECT my_id, my_inet, my_inet_list, my_inet_set FROM inet_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(IpAddr::from_str("127.0.0.1")?, row.my_inet);
    assert_eq!(
        vec![
            IpAddr::from_str("127.0.0.1")?,
            IpAddr::from_str("192.0.2.2")?,
            IpAddr::from_str("2001:db8::3")?,
            IpAddr::from_str("198.51.100.4")?,
        ],
        row.my_inet_list
    );
    assert_eq!(
        vec![
            IpAddr::from_str("2001:db8::3")?,
            IpAddr::from_str("127.0.0.1")?,
            IpAddr::from_str("192.0.2.2")?,
        ],
        row.my_inet_set
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_inet_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO inet_tests(my_id, my_inet, my_inet_list, my_inet_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<IpAddr>)
    .bind(None::<Vec<IpAddr>>)
    .bind(None::<Vec<IpAddr>>)
    .execute(&pool)
    .await?;

    let (my_id, my_inet, my_inet_list, my_inet_set): (
        Uuid,
        Option<IpAddr>,
        Option<Vec<IpAddr>>,
        Option<Vec<IpAddr>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_inet, my_inet_list, my_inet_set FROM inet_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_inet.is_none());
    assert!(my_inet_list.is_none());
    assert!(my_inet_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO inet_tests(my_id, my_inet, my_inet_list, my_inet_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(IpAddr::from_str("127.0.0.1")?))
    .bind(Some([
        IpAddr::from_str("127.0.0.1")?,
        IpAddr::from_str("192.0.2.2")?,
        IpAddr::from_str("2001:db8::3")?,
        IpAddr::from_str("198.51.100.4")?,
    ]))
    .bind(Some([
        IpAddr::from_str("127.0.0.1")?,
        IpAddr::from_str("192.0.2.2")?,
        IpAddr::from_str("2001:db8::3")?,
        IpAddr::from_str("127.0.0.1")?,
    ]))
    .execute(&pool)
    .await?;

    let (my_id, my_inet, my_inet_list, my_inet_set): (
        Uuid,
        Option<IpAddr>,
        Option<Vec<IpAddr>>,
        Option<Vec<IpAddr>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_inet, my_inet_list, my_inet_set FROM inet_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(IpAddr::from_str("127.0.0.1")?, my_inet.unwrap());
    assert_eq!(
        vec![
            IpAddr::from_str("127.0.0.1")?,
            IpAddr::from_str("192.0.2.2")?,
            IpAddr::from_str("2001:db8::3")?,
            IpAddr::from_str("198.51.100.4")?,
        ],
        my_inet_list.unwrap()
    );
    assert_eq!(
        vec![
            IpAddr::from_str("2001:db8::3")?,
            IpAddr::from_str("127.0.0.1")?,
            IpAddr::from_str("192.0.2.2")?,
        ],
        my_inet_set.unwrap()
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_inet(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_inet, my_inet_list, my_inet_set FROM inet_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_inet", describe.columns()[1].name());
    assert_eq!("my_inet_list", describe.columns()[2].name());
    assert_eq!("my_inet_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("INET", describe.columns()[1].type_info().name());
    assert_eq!("INET[]", describe.columns()[2].type_info().name());
    assert_eq!("INET[]", describe.columns()[3].type_info().name());

    Ok(())
}
