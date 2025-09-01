use scylla::value::CqlTime;
use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_cql_time(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO time_tests(my_id, my_time, my_time_list, my_time_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(CqlTime(27874))
    .bind([
        CqlTime(27874),
        CqlTime(21845),
        CqlTime(22058),
        CqlTime(39263),
    ])
    .bind([
        CqlTime(27874),
        CqlTime(21845),
        CqlTime(22058),
        CqlTime(27874),
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_time, my_time_list, my_time_set): (Uuid, CqlTime, Vec<CqlTime>, Vec<CqlTime>) =
        sqlx::query_as(
            "SELECT my_id, my_time, my_time_list, my_time_set FROM time_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(CqlTime(27874), my_time);
    assert_eq!(
        vec![
            CqlTime(27874),
            CqlTime(21845),
            CqlTime(22058),
            CqlTime(39263),
        ],
        my_time_list
    );
    assert_eq!(
        vec![CqlTime(21845), CqlTime(22058), CqlTime(27874),],
        my_time_set
    );

    #[derive(FromRow)]
    struct CqlTimeTest {
        my_id: Uuid,
        my_time: CqlTime,
        my_time_list: Vec<CqlTime>,
        my_time_set: Vec<CqlTime>,
    }

    let row: CqlTimeTest = sqlx::query_as(
        "SELECT my_id, my_time, my_time_list, my_time_set FROM time_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(CqlTime(27874), row.my_time);
    assert_eq!(
        vec![
            CqlTime(27874),
            CqlTime(21845),
            CqlTime(22058),
            CqlTime(39263),
        ],
        row.my_time_list
    );
    assert_eq!(
        vec![CqlTime(21845), CqlTime(22058), CqlTime(27874),],
        row.my_time_set
    );

    Ok(())
}

#[cfg(feature = "chrono-04")]
#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_chrono_timetime(pool: ScyllaDBPool) -> anyhow::Result<()> {
    use chrono_04::NaiveTime;

    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO time_tests(my_id, my_time, my_time_list, my_time_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(NaiveTime::from_hms_opt(16, 44, 34).unwrap())
    .bind([
        NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
        NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
        NaiveTime::from_hms_opt(15, 07, 38).unwrap(),
        NaiveTime::from_hms_opt(19, 54, 23).unwrap(),
    ])
    .bind([
        NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
        NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
        NaiveTime::from_hms_opt(15, 07, 38).unwrap(),
        NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_time, my_time_list, my_time_set): (
        Uuid,
        NaiveTime,
        Vec<NaiveTime>,
        Vec<NaiveTime>,
    ) = sqlx::query_as(
        "SELECT my_id, my_time, my_time_list, my_time_set FROM time_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(NaiveTime::from_hms_opt(16, 44, 34).unwrap(), my_time);
    assert_eq!(
        vec![
            NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
            NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
            NaiveTime::from_hms_opt(15, 07, 38).unwrap(),
            NaiveTime::from_hms_opt(19, 54, 23).unwrap(),
        ],
        my_time_list
    );
    assert_eq!(
        vec![
            NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
            NaiveTime::from_hms_opt(15, 07, 38).unwrap(),
            NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
        ],
        my_time_set
    );

    #[derive(FromRow)]
    struct ChronoNaiveTimeTest {
        my_id: Uuid,
        my_time: NaiveTime,
        my_time_list: Vec<NaiveTime>,
        my_time_set: Vec<NaiveTime>,
    }

    let row: ChronoNaiveTimeTest = sqlx::query_as(
        "SELECT my_id, my_time, my_time_list, my_time_set FROM time_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(NaiveTime::from_hms_opt(16, 44, 34).unwrap(), row.my_time);
    assert_eq!(
        vec![
            NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
            NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
            NaiveTime::from_hms_opt(15, 07, 38).unwrap(),
            NaiveTime::from_hms_opt(19, 54, 23).unwrap(),
        ],
        row.my_time_list
    );
    assert_eq!(
        vec![
            NaiveTime::from_hms_opt(15, 04, 05).unwrap(),
            NaiveTime::from_hms_opt(15, 07, 38).unwrap(),
            NaiveTime::from_hms_opt(16, 44, 34).unwrap(),
        ],
        row.my_time_set
    );

    Ok(())
}

#[cfg(feature = "time-03")]
#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_time_offset_time_time(pool: ScyllaDBPool) -> anyhow::Result<()> {
    use time_03::Time;

    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO time_tests(my_id, my_time, my_time_list, my_time_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Time::from_hms(16, 44, 34)?)
    .bind([
        Time::from_hms(16, 44, 34)?,
        Time::from_hms(15, 04, 05)?,
        Time::from_hms(15, 07, 38)?,
        Time::from_hms(19, 54, 23)?,
    ])
    .bind([
        Time::from_hms(15, 04, 05)?,
        Time::from_hms(15, 07, 38)?,
        Time::from_hms(16, 44, 34)?,
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_time, my_time_list, my_time_set): (Uuid, Time, Vec<Time>, Vec<Time>) =
        sqlx::query_as(
            "SELECT my_id, my_time, my_time_list, my_time_set FROM time_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(Time::from_hms(16, 44, 34)?, my_time);
    assert_eq!(
        vec![
            Time::from_hms(16, 44, 34)?,
            Time::from_hms(15, 04, 05)?,
            Time::from_hms(15, 07, 38)?,
            Time::from_hms(19, 54, 23)?,
        ],
        my_time_list
    );
    assert_eq!(
        vec![
            Time::from_hms(15, 04, 05)?,
            Time::from_hms(15, 07, 38)?,
            Time::from_hms(16, 44, 34)?,
        ],
        my_time_set
    );

    #[derive(FromRow)]
    struct TimeTest {
        my_id: Uuid,
        my_time: Time,
        my_time_list: Vec<Time>,
        my_time_set: Vec<Time>,
    }

    let row: TimeTest = sqlx::query_as(
        "SELECT my_id, my_time, my_time_list, my_time_set FROM time_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(Time::from_hms(16, 44, 34)?, row.my_time);
    assert_eq!(
        vec![
            Time::from_hms(16, 44, 34)?,
            Time::from_hms(15, 04, 05)?,
            Time::from_hms(15, 07, 38)?,
            Time::from_hms(19, 54, 23)?,
        ],
        row.my_time_list
    );
    assert_eq!(
        vec![
            Time::from_hms(15, 04, 05)?,
            Time::from_hms(15, 07, 38)?,
            Time::from_hms(16, 44, 34)?,
        ],
        row.my_time_set
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_time(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_time, my_time_list, my_time_set FROM time_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_time", describe.columns()[1].name());
    assert_eq!("my_time_list", describe.columns()[2].name());
    assert_eq!("my_time_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("TIME", describe.columns()[1].type_info().name());
    assert_eq!("TIME[]", describe.columns()[2].type_info().name());
    assert_eq!("TIME[]", describe.columns()[3].type_info().name());

    Ok(())
}
