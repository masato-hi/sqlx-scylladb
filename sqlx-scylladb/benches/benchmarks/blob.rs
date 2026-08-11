use std::time::Instant;

use criterion::{Criterion, criterion_group};
use rand::random;
use scylla::client::caching_session::CachingSession;
use sqlx_scylladb::ScyllaDBPool;

use crate::benchmarks::{setup_scylla_session, setup_sqlx_scylladb_pool};

const COUNT: i64 = 10000;
const BLOB_SIZE: usize = 1024;

fn random_blob() -> [u8; BLOB_SIZE] {
    random()
}

async fn setup_table() -> anyhow::Result<()> {
    let session = setup_scylla_session().await?;

    session
        .execute_unpaged("DROP TABLE IF EXISTS bench_blob", ())
        .await?;
    session
        .execute_unpaged(
            "CREATE TABLE IF NOT EXISTS bench_blob(id BIGINT PRIMARY KEY, my_blob BLOB)",
            (),
        )
        .await?;

    Ok(())
}

async fn run_insert_blob_with_scylla(session: &CachingSession) -> anyhow::Result<()> {
    for i in 0..COUNT {
        session
            .execute_unpaged(
                "INSERT INTO bench_blob(id, my_blob) VALUES(?, ?)",
                (i, random_blob()),
            )
            .await?;
    }

    Ok(())
}

async fn run_insert_blob_with_sqlx_scylladb(pool: &ScyllaDBPool) -> anyhow::Result<()> {
    for i in 0..COUNT {
        sqlx::query("INSERT INTO bench_blob(id, my_blob) VALUES(?, ?)")
            .bind(i)
            .bind(random_blob())
            .execute(pool)
            .await?;
    }

    Ok(())
}

async fn run_select_blob_with_scylla(session: &CachingSession) -> anyhow::Result<()> {
    for i in 0..COUNT {
        let _: (Vec<u8>,) = session
            .execute_unpaged("SELECT my_blob FROM bench_blob WHERE id = ?", (i,))
            .await?
            .into_rows_result()?
            .first_row()?;
    }

    Ok(())
}

async fn run_select_blob_with_sqlx_scylladb(pool: &ScyllaDBPool) -> anyhow::Result<()> {
    for i in 0..COUNT {
        let _: (Vec<u8>,) = sqlx::query_as("SELECT my_blob FROM bench_blob WHERE id = ?")
            .bind(i)
            .fetch_one(pool)
            .await?;
    }

    Ok(())
}

pub fn insert_blob_with_scylla(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("insert_blob_with_scylla", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let session = setup_scylla_session().await.unwrap();

            let start = Instant::now();
            for _ in 0..iters {
                std::hint::black_box(run_insert_blob_with_scylla(&session).await).unwrap();
            }
            start.elapsed()
        })
    });
}

pub fn insert_blob_with_sqlx_scylladb(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("insert_blob_with_sqlx_scylladb", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let pool = setup_sqlx_scylladb_pool().await.unwrap();

            let start = Instant::now();
            for _ in 0..iters {
                std::hint::black_box(run_insert_blob_with_sqlx_scylladb(&pool).await).unwrap();
            }
            start.elapsed()
        })
    });
}

pub fn select_blob_with_scylla(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("select_blob_with_scylla", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let session = setup_scylla_session().await.unwrap();

            run_insert_blob_with_scylla(&session).await.unwrap();

            let start = Instant::now();
            for _ in 0..iters {
                std::hint::black_box(run_select_blob_with_scylla(&session).await).unwrap();
            }
            start.elapsed()
        })
    });
}

pub fn select_blob_with_sqlx_scylladb(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("select_blob_with_sqlx_scylladb", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let pool = setup_sqlx_scylladb_pool().await.unwrap();

            run_insert_blob_with_sqlx_scylladb(&pool).await.unwrap();

            let start = Instant::now();
            for _ in 0..iters {
                std::hint::black_box(run_select_blob_with_sqlx_scylladb(&pool).await).unwrap();
            }
            start.elapsed()
        })
    });
}

criterion_group!(
    benches,
    insert_blob_with_scylla,
    insert_blob_with_sqlx_scylladb,
    select_blob_with_scylla,
    select_blob_with_sqlx_scylladb,
);
