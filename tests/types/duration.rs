use scylla::value::CqlDuration;
use sqlx::{Acquire, Column, Executor, FromRow, TypeInfo};
use sqlx_scylladb::ScyllaDBPool;
use uuid::Uuid;

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_duration(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO duration_tests(my_id, my_duration, my_duration_list) VALUES(?, ?, ?)",
    )
    .bind(id)
    .bind(CqlDuration {
        months: 1,
        days: 15,
        nanoseconds: 300000000,
    })
    .bind([
        CqlDuration {
            months: 1,
            days: 15,
            nanoseconds: 300000000,
        },
        CqlDuration {
            months: 2,
            days: 16,
            nanoseconds: 400000000,
        },
        CqlDuration {
            months: 3,
            days: 17,
            nanoseconds: 500000000,
        },
        CqlDuration {
            months: 4,
            days: 18,
            nanoseconds: 600000000,
        },
    ])
    .execute(&pool)
    .await?;

    let (my_id, my_duration, my_duration_list): (Uuid, CqlDuration, Vec<CqlDuration>) =
        sqlx::query_as(
            "SELECT my_id, my_duration, my_duration_list FROM duration_tests WHERE my_id = ?",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(id, my_id);
    assert_eq!(
        CqlDuration {
            months: 1,
            days: 15,
            nanoseconds: 300000000
        },
        my_duration
    );
    assert_eq!(
        vec![
            CqlDuration {
                months: 1,
                days: 15,
                nanoseconds: 300000000
            },
            CqlDuration {
                months: 2,
                days: 16,
                nanoseconds: 400000000
            },
            CqlDuration {
                months: 3,
                days: 17,
                nanoseconds: 500000000
            },
            CqlDuration {
                months: 4,
                days: 18,
                nanoseconds: 600000000
            }
        ],
        my_duration_list
    );

    #[derive(FromRow)]
    struct DurationTest {
        my_id: Uuid,
        my_duration: CqlDuration,
        my_duration_list: Vec<CqlDuration>,
    }

    let row: DurationTest = sqlx::query_as(
        "SELECT my_id, my_duration, my_duration_list FROM duration_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, row.my_id);
    assert_eq!(
        CqlDuration {
            months: 1,
            days: 15,
            nanoseconds: 300000000
        },
        row.my_duration
    );
    assert_eq!(
        vec![
            CqlDuration {
                months: 1,
                days: 15,
                nanoseconds: 300000000
            },
            CqlDuration {
                months: 2,
                days: 16,
                nanoseconds: 400000000
            },
            CqlDuration {
                months: 3,
                days: 17,
                nanoseconds: 500000000
            },
            CqlDuration {
                months: 4,
                days: 18,
                nanoseconds: 600000000
            }
        ],
        row.my_duration_list
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn it_can_select_duration_optional(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    let _ = sqlx::query(
        "INSERT INTO duration_tests(my_id, my_duration, my_duration_list) VALUES(?, ?, ?)",
    )
    .bind(id)
    .bind(None::<CqlDuration>)
    .bind(None::<Vec<CqlDuration>>)
    .execute(&pool)
    .await?;

    let (my_id, my_duration, my_duration_list): (
        Uuid,
        Option<CqlDuration>,
        Option<Vec<CqlDuration>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_duration, my_duration_list FROM duration_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert!(my_duration.is_none());
    assert!(my_duration_list.is_none());

    let _ = sqlx::query(
        "INSERT INTO duration_tests(my_id, my_duration, my_duration_list) VALUES(?, ?, ?)",
    )
    .bind(id)
    .bind(Some(CqlDuration {
        months: 1,
        days: 15,
        nanoseconds: 300000000,
    }))
    .bind(Some([
        CqlDuration {
            months: 1,
            days: 15,
            nanoseconds: 300000000,
        },
        CqlDuration {
            months: 2,
            days: 16,
            nanoseconds: 400000000,
        },
        CqlDuration {
            months: 3,
            days: 17,
            nanoseconds: 500000000,
        },
        CqlDuration {
            months: 4,
            days: 18,
            nanoseconds: 600000000,
        },
    ]))
    .execute(&pool)
    .await?;

    let (my_id, my_duration, my_duration_list): (
        Uuid,
        Option<CqlDuration>,
        Option<Vec<CqlDuration>>,
    ) = sqlx::query_as(
        "SELECT my_id, my_duration, my_duration_list FROM duration_tests WHERE my_id = ?",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(id, my_id);
    assert_eq!(
        CqlDuration {
            months: 1,
            days: 15,
            nanoseconds: 300000000
        },
        my_duration.unwrap()
    );
    assert_eq!(
        vec![
            CqlDuration {
                months: 1,
                days: 15,
                nanoseconds: 300000000
            },
            CqlDuration {
                months: 2,
                days: 16,
                nanoseconds: 400000000
            },
            CqlDuration {
                months: 3,
                days: 17,
                nanoseconds: 500000000
            },
            CqlDuration {
                months: 4,
                days: 18,
                nanoseconds: 600000000
            }
        ],
        my_duration_list.unwrap()
    );

    Ok(())
}

#[sqlx::test(migrations = "tests/types/migrations")]
async fn describe_duration(pool: ScyllaDBPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let conn = conn.acquire().await?;

    let describe = conn
        .describe("SELECT my_id, my_duration, my_duration_list FROM duration_tests")
        .await?;

    assert_eq!("my_id", describe.columns()[0].name());
    assert_eq!("my_duration", describe.columns()[1].name());
    assert_eq!("my_duration_list", describe.columns()[2].name());

    assert_eq!("UUID", describe.columns()[0].type_info().name());
    assert_eq!("DURATION", describe.columns()[1].type_info().name());
    assert_eq!("DURATION[]", describe.columns()[2].type_info().name());

    Ok(())
}
