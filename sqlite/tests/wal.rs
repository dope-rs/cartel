use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use common::{assert_reclaimed, assert_retirement, wal::*};
use dope_core::driver::life::recovery::Policy;
use dope_runtime::{process::lifecycle, topology};
use dope_test::fibers;
use dope_test::harness::support::file::Directory;
use std::{env, num::NonZero, path::Path};

pub mod common;

#[test]
fn wal_contracts() -> Result<(), plan::Error> {
    // SAFETY: this binary has one test; installation precedes every SQLite call
    // and worker start. Only Cartel uses installed SQLite; Core handles and
    // allocations remain exclusively owned by their originating worker until release.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    checkpoint_pages_fit_native_range(runtime)?;
    reader_sees_wal_commit(runtime)
}

fn checkpoint_pages_fit_native_range(runtime: Runtime) -> Result<(), plan::Error> {
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

fn reader_sees_wal_commit(runtime: Runtime) -> Result<(), plan::Error> {
    use lifecycle::Task as _;

    let base = env::temp_dir();
    let dir = Directory::try_with(&base, "cartel-sqlite-wal", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary path");
    let path = dir.path().join("database.db");
    let read_db = database(runtime, &path)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)?;
    let peer_read = database(runtime, &path)?;
    let peer_write = database(runtime, &path)?;
    let limits = try_limits()?;
    let memory = try_memory()?;
    let result = plan::Plan::<WalDatabase>::try_new(read_db, limits, memory)?.try_run(
        |driver, mut reader| {
            let mut tx = fibers::TEST
                .try_run(driver, reader.transaction())
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            fibers::TEST
                .try_run(
                    driver,
                    tx.query()
                        .run_no_rows(WalDatabase::INSERT_ITEM, (1_i64, ())),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            let ((), uncommitted) = lifecycle::Entry(move || {
                let result = plan::Plan::<WalDatabase>::try_new(peer_read, limits, memory)
                    .expect("WAL peer reader plan")
                    .try_count(
                        WalDatabase::COUNT_ITEMS,
                        Policy::new(assert_reclaimed, assert_retirement),
                        |result| result.expect("SQLite scope"),
                    );
                result
                    .expect("WAL peer reader scope failed")
                    .expect("WAL peer read")
            })
            .with(|| ())
            .expect("WAL peer reader start and join");
            assert_eq!(uncommitted, 1);
            fibers::TEST
                .try_run(driver, tx.rollback())
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
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

            lifecycle::Entry(move || {
                let result = plan::Plan::<WalDatabase>::try_new(peer_write, limits, memory)
                    .expect("WAL peer writer plan")
                    .try_run(
                        |driver, mut writer| {
                            fibers::TEST
                                .try_run(
                                    driver,
                                    writer
                                        .query()
                                        .run_no_rows(WalDatabase::INSERT_ITEM, (1_i64, ())),
                                )
                                .map_err(plan::Error::Run)?
                                .map_err(plan::Error::Call)
                        },
                        Policy::new(assert_reclaimed, assert_retirement),
                        |result| result.expect("SQLite scope"),
                    );
                result
                    .expect("WAL peer writer scope failed")
                    .expect("WAL peer write");
            })
            .with(|| ())
            .expect("WAL peer writer start and join");
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
