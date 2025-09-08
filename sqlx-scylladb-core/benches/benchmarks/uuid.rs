use std::time::Instant;

use criterion::{Criterion, criterion_group};
use scylla::client::caching_session::CachingSession;
use sqlx_scylladb_core::ScyllaDBPool;
use uuid::Uuid;

use crate::benchmarks::{setup_scylla_session, setup_sqlx_scylladb_pool};

const COUNT: i64 = 10000;

async fn setup_table() -> anyhow::Result<()> {
    let session = setup_scylla_session().await?;

    session
        .execute_unpaged("DROP TABLE IF EXISTS bench_uuid", ())
        .await?;
    session
        .execute_unpaged(
            "CREATE TABLE IF NOT EXISTS bench_uuid(id BIGINT PRIMARY KEY, my_uuid UUID)",
            (),
        )
        .await?;

    Ok(())
}

async fn run_insert_uuid_with_scylla(session: &CachingSession) -> anyhow::Result<()> {
    for i in 0..COUNT {
        session
            .execute_unpaged(
                "INSERT INTO bench_uuid(id, my_uuid) VALUES(?, ?)",
                (i, Uuid::new_v4()),
            )
            .await?;
    }

    Ok(())
}

async fn run_insert_uuid_with_sqlx_scylladb(pool: &ScyllaDBPool) -> anyhow::Result<()> {
    for i in 0..COUNT {
        sqlx::query("INSERT INTO bench_uuid(id, my_uuid) VALUES(?, ?)")
            .bind(i)
            .bind(Uuid::new_v4())
            .execute(pool)
            .await?;
    }

    Ok(())
}

async fn run_select_uuid_with_scylla(session: &CachingSession) -> anyhow::Result<()> {
    for i in 0..COUNT {
        let _: (Uuid,) = session
            .execute_unpaged("SELECT my_uuid FROM bench_uuid WHERE id = ?", (i,))
            .await?
            .into_rows_result()?
            .first_row()?;
    }

    Ok(())
}

async fn run_select_uuid_with_sqlx_scylladb(pool: &ScyllaDBPool) -> anyhow::Result<()> {
    for i in 0..COUNT {
        let _: (Uuid,) = sqlx::query_as("SELECT my_uuid FROM bench_uuid WHERE id = ?")
            .bind(i)
            .fetch_one(pool)
            .await?;
    }

    Ok(())
}

pub fn insert_uuid_with_scylla(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("insert_uuid_with_scylla", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let session = setup_scylla_session().await.unwrap();

            let start = Instant::now();
            for _i in 0..iters {
                std::hint::black_box(run_insert_uuid_with_scylla(&session).await).unwrap();
            }
            start.elapsed()
        })
    });
}

pub fn insert_uuid_with_sqlx_scylladb(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("insert_uuid_with_sqlx_scylladb", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let pool = setup_sqlx_scylladb_pool().await.unwrap();

            let start = Instant::now();
            for _i in 0..iters {
                std::hint::black_box(run_insert_uuid_with_sqlx_scylladb(&pool).await).unwrap();
            }
            start.elapsed()
        })
    });
}

pub fn select_uuid_with_scylla(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("select_uuid_with_scylla", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let session = setup_scylla_session().await.unwrap();

            run_insert_uuid_with_scylla(&session).await.unwrap();

            let start = Instant::now();
            for _i in 0..iters {
                std::hint::black_box(run_select_uuid_with_scylla(&session).await).unwrap();
            }
            start.elapsed()
        })
    });
}

pub fn select_uuid_with_sqlx_scylladb(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("select_uuid_with_sqlx_scylladb", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let pool = setup_sqlx_scylladb_pool().await.unwrap();

            run_insert_uuid_with_sqlx_scylladb(&pool).await.unwrap();

            let start = Instant::now();
            for _i in 0..iters {
                std::hint::black_box(run_select_uuid_with_sqlx_scylladb(&pool).await).unwrap();
            }
            start.elapsed()
        })
    });
}

criterion_group!(
    benches,
    insert_uuid_with_scylla,
    insert_uuid_with_sqlx_scylladb,
    select_uuid_with_scylla,
    select_uuid_with_sqlx_scylladb,
);
