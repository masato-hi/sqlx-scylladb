use scylla::value::CqlDate;
use sqlx::{Acquire, Column, Executor, TypeInfo};
use sqlx_macros::FromRow;
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_cql_date(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO date_tests(my_id, my_date, my_date_list, my_date_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(CqlDate(20330))
    .bind(&[
        CqlDate(20330),
        CqlDate(13149),
        CqlDate(8842),
        CqlDate(32668),
    ])
    .bind(&[
        CqlDate(20330),
        CqlDate(13149),
        CqlDate(8842),
        CqlDate(20330),
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_date, my_date_list, my_date_set): (Uuid, CqlDate, Vec<CqlDate>, Vec<CqlDate>) =
        sqlx::query_as(
            "SELECT my_id, my_date, my_date_list, my_date_set FROM date_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(CqlDate(20330), my_date);
    assert_eq!(
        vec![
            CqlDate(20330),
            CqlDate(13149),
            CqlDate(8842),
            CqlDate(32668),
        ],
        my_date_list
    );
    assert_eq!(
        vec![CqlDate(8842), CqlDate(13149), CqlDate(20330),],
        my_date_set
    );

    #[derive(FromRow)]
    struct CqlDateTest {
        my_id: Uuid,
        my_date: CqlDate,
        my_date_list: Vec<CqlDate>,
        my_date_set: Vec<CqlDate>,
    }

    let row: CqlDateTest = sqlx::query_as(
        "SELECT my_id, my_date, my_date_list, my_date_set FROM date_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(CqlDate(20330), row.my_date);
    assert_eq!(
        vec![
            CqlDate(20330),
            CqlDate(13149),
            CqlDate(8842),
            CqlDate(32668),
        ],
        row.my_date_list
    );
    assert_eq!(
        vec![CqlDate(8842), CqlDate(13149), CqlDate(20330),],
        row.my_date_set
    );

    Ok(())
}

#[cfg(feature = "chrono-04")]
#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_chrono_datetime(pool: ScyllaDBPool) -> anyhow::Result<()> {
    use chrono_04::NaiveDate;

    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO date_tests(my_id, my_date, my_date_list, my_date_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(NaiveDate::from_ymd_opt(2025, 8, 31).unwrap())
    .bind(&[
        NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
        NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(1994, 3, 19).unwrap(),
        NaiveDate::from_ymd_opt(2059, 6, 12).unwrap(),
    ])
    .bind(&[
        NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
        NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(1994, 3, 19).unwrap(),
        NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_date, my_date_list, my_date_set): (
        Uuid,
        NaiveDate,
        Vec<NaiveDate>,
        Vec<NaiveDate>,
    ) = sqlx::query_as(
        "SELECT my_id, my_date, my_date_list, my_date_set FROM date_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(), my_date);
    assert_eq!(
        vec![
            NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
            NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(1994, 3, 19).unwrap(),
            NaiveDate::from_ymd_opt(2059, 6, 12).unwrap(),
        ],
        my_date_list
    );
    assert_eq!(
        vec![
            NaiveDate::from_ymd_opt(1994, 3, 19).unwrap(),
            NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
        ],
        my_date_set
    );

    #[derive(FromRow)]
    struct ChronoNaiveDateTest {
        my_id: Uuid,
        my_date: NaiveDate,
        my_date_list: Vec<NaiveDate>,
        my_date_set: Vec<NaiveDate>,
    }

    let row: ChronoNaiveDateTest = sqlx::query_as(
        "SELECT my_id, my_date, my_date_list, my_date_set FROM date_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(), row.my_date);
    assert_eq!(
        vec![
            NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
            NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(1994, 3, 19).unwrap(),
            NaiveDate::from_ymd_opt(2059, 6, 12).unwrap(),
        ],
        row.my_date_list
    );
    assert_eq!(
        vec![
            NaiveDate::from_ymd_opt(1994, 3, 19).unwrap(),
            NaiveDate::from_ymd_opt(2006, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2025, 8, 31).unwrap(),
        ],
        row.my_date_set
    );

    Ok(())
}

#[cfg(feature = "time-03")]
#[sqlx::test(migrations = "tests/types_migrations")]
async fn it_can_select_time_offset_date_time(pool: ScyllaDBPool) -> anyhow::Result<()> {
    use time_03::{
        Date,
        Month::{August, January, June, March},
    };

    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO date_tests(my_id, my_date, my_date_list, my_date_set) VALUES(?, ?, ?, ?)",
    )
    .bind(id)
    .bind(Date::from_calendar_date(2025, August, 31)?)
    .bind(&[
        Date::from_calendar_date(2025, August, 31)?,
        Date::from_calendar_date(2006, January, 2)?,
        Date::from_calendar_date(1994, March, 19)?,
        Date::from_calendar_date(2059, June, 12)?,
    ])
    .bind(&[
        Date::from_calendar_date(2025, August, 31)?,
        Date::from_calendar_date(2006, January, 2)?,
        Date::from_calendar_date(1994, March, 19)?,
        Date::from_calendar_date(2025, August, 31)?,
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_date, my_date_list, my_date_set): (Uuid, Date, Vec<Date>, Vec<Date>) =
        sqlx::query_as(
            "SELECT my_id, my_date, my_date_list, my_date_set FROM date_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(Date::from_calendar_date(2025, August, 31)?, my_date);
    assert_eq!(
        vec![
            Date::from_calendar_date(2025, August, 31)?,
            Date::from_calendar_date(2006, January, 2)?,
            Date::from_calendar_date(1994, March, 19)?,
            Date::from_calendar_date(2059, June, 12)?,
        ],
        my_date_list
    );
    assert_eq!(
        vec![
            Date::from_calendar_date(1994, March, 19)?,
            Date::from_calendar_date(2006, January, 2)?,
            Date::from_calendar_date(2025, August, 31)?,
        ],
        my_date_set
    );

    #[derive(FromRow)]
    struct DateTest {
        my_id: Uuid,
        my_date: Date,
        my_date_list: Vec<Date>,
        my_date_set: Vec<Date>,
    }

    let row: DateTest = sqlx::query_as(
        "SELECT my_id, my_date, my_date_list, my_date_set FROM date_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(Date::from_calendar_date(2025, August, 31)?, row.my_date);
    assert_eq!(
        vec![
            Date::from_calendar_date(2025, August, 31)?,
            Date::from_calendar_date(2006, January, 2)?,
            Date::from_calendar_date(1994, March, 19)?,
            Date::from_calendar_date(2059, June, 12)?,
        ],
        row.my_date_list
    );
    assert_eq!(
        vec![
            Date::from_calendar_date(1994, March, 19)?,
            Date::from_calendar_date(2006, January, 2)?,
            Date::from_calendar_date(2025, August, 31)?,
        ],
        row.my_date_set
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types_migrations")]
async fn describe_date(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_date, my_date_list, my_date_set FROM date_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_date", describe.columns()[1].name());
    assert_eq!("my_date_list", describe.columns()[2].name());
    assert_eq!("my_date_set", describe.columns()[3].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("DATE", describe.columns()[1].type_info().name());
    assert_eq!("DATE[]", describe.columns()[2].type_info().name());
    assert_eq!("DATE[]", describe.columns()[3].type_info().name());

    Ok(())
}
