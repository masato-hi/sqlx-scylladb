use pprof::protos::Message;
use std::{fs::File, time::Instant};

use criterion::{Criterion, criterion_group};

use crate::benchmarks::{
    setup_sqlx_scylladb_pool,
    text::{run_insert_text_with_sqlx_scylladb, setup_table},
};

pub fn profile_insert_text_with_sqlx_scylladb(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("profile_insert_text_with_sqlx_scylladb", move |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            setup_table().await.unwrap();
            let pool = setup_sqlx_scylladb_pool().await.unwrap();

            let guard = pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .unwrap();

            let start = Instant::now();
            for _i in 0..iters {
                std::hint::black_box(run_insert_text_with_sqlx_scylladb(&pool).await).unwrap();
            }
            let elapsed = start.elapsed();

            if let Ok(report) = guard.report().build() {
                let mut file = File::create("profile.pb").unwrap();
                let profile = report.pprof().unwrap();
                profile.write_to_writer(&mut file).unwrap();
            };

            elapsed
        })
    });
}

criterion_group!(benches, profile_insert_text_with_sqlx_scylladb,);
