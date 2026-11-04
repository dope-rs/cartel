use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use common::{assert_reclaimed, assert_retirement, wal::*};
use dope_core::driver::life::recovery::Policy;
use dope_runtime::topology;
use dope_test::fibers;
use dope_test::harness::support::file::Directory;
use std::{env, num::NonZero, path::Path, sync::OnceLock};
use std::{sync::mpsc, thread, time::Duration};

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn checkpoint_pages_fit_native_range() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let dir = Directory::try_with(
        Path::new(env!("CARGO_TARGET_TMPDIR")),
        "cartel-sqlite-checkpoint-pages",
        |removed| removed.expect("temporary directory cleanup"),
    )
    .expect("temporary directory");
    let path = dir.path().join("database.db");
    let maximum = i32::MAX as u32;
    for value in [maximum, maximum + 1] {
        let mut wal = cartel_sqlite::Wal::new();
        wal.checkpoint(cartel_sqlite::Checkpoint::Pages(
            NonZero::new(value).expect("positive checkpoint threshold"),
        ));
        let target = cartel_sqlite::OpenOptions::new()
            .journal(cartel_sqlite::Journal::Wal(wal))
            .open(runtime, &path)
            .map_err(plan::Error::Client)?;
        let result =
            plan::Plan::<common::catalog::Commands>::try_new(target, try_limits()?, try_memory()?)?
                .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                    result.expect("SQLite scope")
                });
        if value == maximum {
            result.expect("maximum native checkpoint threshold");
        } else {
            assert!(matches!(
                result,
                Err(plan::Failure::Install(topology::InstallError::Install(
                    cartel_sqlite::open::Error::Step(cartel_sqlite::Error::CheckpointPages {
                        value: actual,
                    })
                ))) if actual == value
            ));
        }
    }
    Ok(())
}

#[test]
fn reader_sees_wal_commit() -> Result<(), plan::Error> {
    let base = env::temp_dir();
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let dir = Directory::try_with(&base, "cartel-sqlite-wal", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("database.db");
    let read_db = database(runtime, &path)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)?;
    let write_db = database(runtime, &path)?;
    let result = plan::Plan::<WalDatabase>::try_new(read_db, try_limits()?, try_memory()?)?
        .try_run(
            |driver, mut reader| {
                let (lock_tx, lock_rx) = mpsc::channel();
                let (free_tx, free_rx) = mpsc::channel();
                let writer = thread::spawn(move || {
                    let result = (|| -> Result<(), plan::Error> {
                        let result = plan::Plan::<WalDatabase>::try_new(
                            write_db,
                            try_limits()?,
                            try_memory()?,
                        )?
                        .try_run(
                            |driver, mut writer| {
                                let mut tx = fibers::TEST
                                    .try_run(driver, writer.transaction())
                                    .map_err(plan::Error::Run)?
                                    .map_err(plan::Error::Call)?;
                                let written = fibers::TEST.try_run(
                                    driver,
                                    tx.query()
                                        .run_no_rows(WalDatabase::INSERT_ITEM, (1_i64, ())),
                                );
                                assert!(
                                    matches!(written, Ok(Ok(()))),
                                    "uncommitted WAL write failed"
                                );
                                assert!(lock_tx.send(()).is_ok(), "WAL writer signal failed");
                                assert!(
                                    free_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
                                    "WAL writer release failed"
                                );
                                let saved = fibers::TEST.try_run(driver, tx.commit());
                                assert!(matches!(saved, Ok(Ok(()))), "WAL writer commit failed");
                                Ok(())
                            },
                            Policy::new(assert_reclaimed, assert_retirement),
                            |result| result.expect("SQLite scope"),
                        );
                        result.expect("WAL writer scope failed")
                    })();
                    assert!(result.is_ok(), "WAL writer failed");
                });

                assert!(
                    lock_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
                    "WAL writer wait failed"
                );
                let before = fibers::TEST
                    .try_run(
                        driver,
                        reader
                            .query()
                            .run_one::<i64, _, _>(WalDatabase::COUNT_ITEMS, ()),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                assert_eq!(before, 1);

                assert!(free_tx.send(()).is_ok(), "WAL writer release failed");
                assert!(writer.join().is_ok(), "WAL writer join failed");
                let after = fibers::TEST
                    .try_run(
                        driver,
                        reader
                            .query()
                            .run_one::<i64, _, _>(WalDatabase::COUNT_ITEMS, ()),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)?;
                assert_eq!(after, 2);
                Ok(())
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
    result.expect("WAL reader scope failed")
}
