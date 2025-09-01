use scylla::value::CqlTimestamp;
use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_cql_timestamp(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(CqlTimestamp(1756625358255))
    .bind([
        CqlTimestamp(1756625358255),
        CqlTimestamp(1756625378304),
        CqlTimestamp(1756625399100),
        CqlTimestamp(1756625404348),
    ])
    .bind([
        CqlTimestamp(1756625358255),
        CqlTimestamp(1756625399100),
        CqlTimestamp(1756625378304),
        CqlTimestamp(1756625358255),
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (Uuid, CqlTimestamp, Vec<CqlTimestamp>, Vec<CqlTimestamp>) =
        sqlx::query_as(
            "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(CqlTimestamp(1756625358255), my_timestamp);
    assert_eq!(
        vec![
            CqlTimestamp(1756625358255),
            CqlTimestamp(1756625378304),
            CqlTimestamp(1756625399100),
            CqlTimestamp(1756625404348),
        ],
        my_timestamp_list
    );
    assert_eq!(
        vec![
            CqlTimestamp(1756625358255),
            CqlTimestamp(1756625378304),
            CqlTimestamp(1756625399100),
        ],
        my_timestamp_set
    );

    #[derive(FromRow)]
    struct CqlTimestampTest {
        my_id: Uuid,
        my_timestamp: CqlTimestamp,
        my_timestamp_list: Vec<CqlTimestamp>,
        my_timestamp_set: Vec<CqlTimestamp>,
    }

    let row: CqlTimestampTest = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(CqlTimestamp(1756625358255), row.my_timestamp);
    assert_eq!(
        vec![
            CqlTimestamp(1756625358255),
            CqlTimestamp(1756625378304),
            CqlTimestamp(1756625399100),
            CqlTimestamp(1756625404348),
        ],
        row.my_timestamp_list
    );
    assert_eq!(
        vec![
            CqlTimestamp(1756625358255),
            CqlTimestamp(1756625378304),
            CqlTimestamp(1756625399100),
        ],
        row.my_timestamp_set
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_cql_timestamp_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<CqlTimestamp>)
    .bind(None::<Vec<CqlTimestamp>>)
    .bind(None::<Vec<CqlTimestamp>>)
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (
        Uuid,
        Option<CqlTimestamp>,
        Option<Vec<CqlTimestamp>>,
        Option<Vec<CqlTimestamp>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_timestamp.is_none());
    assert!(my_timestamp_list.is_none());
    assert!(my_timestamp_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(CqlTimestamp(1756625358255)))
    .bind(Some([
        CqlTimestamp(1756625358255),
        CqlTimestamp(1756625378304),
        CqlTimestamp(1756625399100),
        CqlTimestamp(1756625404348),
    ]))
    .bind(Some([
        CqlTimestamp(1756625358255),
        CqlTimestamp(1756625378304),
        CqlTimestamp(1756625399100),
        CqlTimestamp(1756625358255),
    ]))
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (
        Uuid,
        Option<CqlTimestamp>,
        Option<Vec<CqlTimestamp>>,
        Option<Vec<CqlTimestamp>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(CqlTimestamp(1756625358255), my_timestamp.unwrap());
    assert_eq!(
        vec![
            CqlTimestamp(1756625358255),
            CqlTimestamp(1756625378304),
            CqlTimestamp(1756625399100),
            CqlTimestamp(1756625404348),
        ],
        my_timestamp_list.unwrap()
    );
    assert_eq!(
        vec![
            CqlTimestamp(1756625358255),
            CqlTimestamp(1756625378304),
            CqlTimestamp(1756625399100),
        ],
        my_timestamp_set.unwrap()
    );

    Ok(())
}

#[cfg(feature = "chrono-04")]
#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_chrono_04_datetime(pool: ScyllaDBPool) -> anyhow::Result<()> {
    use chrono_04::{DateTime, Utc};

    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc())
    .bind([
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("2059-06-12T19:54:23+00:00")?.to_utc(),
    ])
    .bind([
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (Uuid, DateTime<Utc>, Vec<DateTime<Utc>>, Vec<DateTime<Utc>>) =
        sqlx::query_as(
            "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        my_timestamp
    );
    assert_eq!(
        vec![
            DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2059-06-12T19:54:23+00:00")?.to_utc(),
        ],
        my_timestamp_list
    );
    assert_eq!(
        vec![
            DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        ],
        my_timestamp_set
    );

    #[derive(FromRow)]
    struct ChronoDateTimeTest {
        my_id: Uuid,
        my_timestamp: DateTime<Utc>,
        my_timestamp_list: Vec<DateTime<Utc>>,
        my_timestamp_set: Vec<DateTime<Utc>>,
    }

    let row: ChronoDateTimeTest = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        row.my_timestamp
    );
    assert_eq!(
        vec![
            DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2059-06-12T19:54:23+00:00")?.to_utc(),
        ],
        row.my_timestamp_list
    );
    assert_eq!(
        vec![
            DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        ],
        row.my_timestamp_set
    );

    Ok(())
}

#[cfg(feature = "chrono-04")]
#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_chrono_04_datetime_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    use chrono_04::{DateTime, Utc};

    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<DateTime<Utc>>)
    .bind(None::<Vec<DateTime<Utc>>>)
    .bind(None::<Vec<DateTime<Utc>>>)
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (
        Uuid,
        Option<DateTime<Utc>>,
        Option<Vec<DateTime<Utc>>>,
        Option<Vec<DateTime<Utc>>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_timestamp.is_none());
    assert!(my_timestamp_list.is_none());
    assert!(my_timestamp_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc()))
    .bind(Some([
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("2059-06-12T19:54:23+00:00")?.to_utc(),
    ]))
    .bind(Some([
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
    ]))
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (
        Uuid,
        Option<DateTime<Utc>>,
        Option<Vec<DateTime<Utc>>>,
        Option<Vec<DateTime<Utc>>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(
        DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        my_timestamp.unwrap()
    );
    assert_eq!(
        vec![
            DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2059-06-12T19:54:23+00:00")?.to_utc(),
        ],
        my_timestamp_list.unwrap()
    );
    assert_eq!(
        vec![
            DateTime::parse_from_rfc3339("1994-03-19T15:07:38+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05+00:00")?.to_utc(),
            DateTime::parse_from_rfc3339("2025-08-31T16:44:34+00:00")?.to_utc(),
        ],
        my_timestamp_set.unwrap()
    );

    Ok(())
}

#[cfg(feature = "time-03")]
#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_time_03_offset_date_time(pool: ScyllaDBPool) -> anyhow::Result<()> {
    use time_03::OffsetDateTime;

    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(OffsetDateTime::from_unix_timestamp(1756626921)?)
    .bind([
        OffsetDateTime::from_unix_timestamp(1756626948)?,
        OffsetDateTime::from_unix_timestamp(1756626953)?,
        OffsetDateTime::from_unix_timestamp(1756626963)?,
        OffsetDateTime::from_unix_timestamp(1756626968)?,
    ])
    .bind([
        OffsetDateTime::from_unix_timestamp(1756626948)?,
        OffsetDateTime::from_unix_timestamp(1756626953)?,
        OffsetDateTime::from_unix_timestamp(1756626963)?,
        OffsetDateTime::from_unix_timestamp(1756626948)?,
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (Uuid, OffsetDateTime, Vec<OffsetDateTime>, Vec<OffsetDateTime>) =
        sqlx::query_as(
            "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(
        OffsetDateTime::from_unix_timestamp(1756626921)?,
        my_timestamp
    );
    assert_eq!(
        vec![
            OffsetDateTime::from_unix_timestamp(1756626948)?,
            OffsetDateTime::from_unix_timestamp(1756626953)?,
            OffsetDateTime::from_unix_timestamp(1756626963)?,
            OffsetDateTime::from_unix_timestamp(1756626968)?,
        ],
        my_timestamp_list
    );
    assert_eq!(
        vec![
            OffsetDateTime::from_unix_timestamp(1756626948)?,
            OffsetDateTime::from_unix_timestamp(1756626953)?,
            OffsetDateTime::from_unix_timestamp(1756626963)?,
        ],
        my_timestamp_set
    );

    #[derive(FromRow)]
    struct ChronoDateTimeTest {
        my_id: Uuid,
        my_timestamp: OffsetDateTime,
        my_timestamp_list: Vec<OffsetDateTime>,
        my_timestamp_set: Vec<OffsetDateTime>,
    }

    let row: ChronoDateTimeTest = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(
        OffsetDateTime::from_unix_timestamp(1756626921)?,
        row.my_timestamp
    );
    assert_eq!(
        vec![
            OffsetDateTime::from_unix_timestamp(1756626948)?,
            OffsetDateTime::from_unix_timestamp(1756626953)?,
            OffsetDateTime::from_unix_timestamp(1756626963)?,
            OffsetDateTime::from_unix_timestamp(1756626968)?,
        ],
        row.my_timestamp_list
    );
    assert_eq!(
        vec![
            OffsetDateTime::from_unix_timestamp(1756626948)?,
            OffsetDateTime::from_unix_timestamp(1756626953)?,
            OffsetDateTime::from_unix_timestamp(1756626963)?,
        ],
        row.my_timestamp_set
    );

    Ok(())
}

#[cfg(feature = "time-03")]
#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_time_03_offset_date_time_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    use time_03::OffsetDateTime;

    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(None::<OffsetDateTime>)
    .bind(None::<Vec<OffsetDateTime>>)
    .bind(None::<Vec<OffsetDateTime>>)
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (
        Uuid,
        Option<OffsetDateTime>,
        Option<Vec<OffsetDateTime>>,
        Option<Vec<OffsetDateTime>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_timestamp.is_none());
    assert!(my_timestamp_list.is_none());
    assert!(my_timestamp_set.is_none());

    let _ = sqlx::query(
        "INSERT INTO timestamp_tests(my_id, my_timestamp, my_timestamp_list, my_timestamp_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Some(OffsetDateTime::from_unix_timestamp(1756626921)?))
    .bind(Some([
        OffsetDateTime::from_unix_timestamp(1756626948)?,
        OffsetDateTime::from_unix_timestamp(1756626953)?,
        OffsetDateTime::from_unix_timestamp(1756626963)?,
        OffsetDateTime::from_unix_timestamp(1756626968)?,
    ]))
    .bind(Some([
        OffsetDateTime::from_unix_timestamp(1756626948)?,
        OffsetDateTime::from_unix_timestamp(1756626953)?,
        OffsetDateTime::from_unix_timestamp(1756626963)?,
        OffsetDateTime::from_unix_timestamp(1756626948)?,
    ]))
    .execute(&pool)
    .await?;

    let (my_id, my_timestamp, my_timestamp_list, my_timestamp_set): (
        Uuid,
        Option<OffsetDateTime>,
        Option<Vec<OffsetDateTime>>,
        Option<Vec<OffsetDateTime>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(
        OffsetDateTime::from_unix_timestamp(1756626921)?,
        my_timestamp.unwrap()
    );
    assert_eq!(
        vec![
            OffsetDateTime::from_unix_timestamp(1756626948)?,
            OffsetDateTime::from_unix_timestamp(1756626953)?,
            OffsetDateTime::from_unix_timestamp(1756626963)?,
            OffsetDateTime::from_unix_timestamp(1756626968)?,
        ],
        my_timestamp_list.unwrap()
    );
    assert_eq!(
        vec![
            OffsetDateTime::from_unix_timestamp(1756626948)?,
            OffsetDateTime::from_unix_timestamp(1756626953)?,
            OffsetDateTime::from_unix_timestamp(1756626963)?,
        ],
        my_timestamp_set.unwrap()
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_timestamp(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe(
            "SELECT my_id, my_timestamp, my_timestamp_list, my_timestamp_set FROM timestamp_tests",
        )
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_timestamp", describe.columns()[1].name());
    assert_eq!("my_timestamp_list", describe.columns()[2].name());
    assert_eq!("my_timestamp_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("TIMESTAMP", describe.columns()[1].type_info().name());
    assert_eq!("TIMESTAMP[]", describe.columns()[2].type_info().name());
    assert_eq!("TIMESTAMP[]", describe.columns()[3].type_info().name());

    Ok(())
}
