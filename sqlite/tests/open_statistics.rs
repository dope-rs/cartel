use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Error, Progress, open};
use common::catalog::{Stable, open, try_limits, try_memory};
use common::{assert_reclaimed, assert_retirement, database, plan};
use dope_core::driver::life::recovery::Policy;
use dope_runtime::topology;
use dope_test::harness::support::file::Directory;
use std::{num::NonZero, path::Path};

pub mod common;

#[test]
fn statistics_exhaust_open() -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-open-statistics", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary directory");
    let path = dir.path().join("catalog.db");
    // SAFETY: this binary has one test, and the fixture closes and shuts down SQLite.
    unsafe { database::statistics(&path) }.expect("statistics fixture");
    // SAFETY: the fixture completed shutdown and no other thread can call SQLite.
    // Only Cartel uses installed SQLite; Core handles and allocations remain
    // exclusively owned by their originating worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    let mut limits = try_limits()?;
    limits.work.open.progress = Progress::new(NonZero::<u64>::MIN);
    let result = plan::Plan::<Stable>::try_new(open(runtime, &path)?, limits, try_memory()?)?
        .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
            result.expect("SQLite scope")
        });
    assert!(matches!(
        result,
        Err(plan::Failure::Install(topology::InstallError::Install(
            open::Error::Step(Error::WorkExhausted)
        )))
    ));
    let count = plan::Plan::try_new(open(runtime, &path)?, try_limits()?, try_memory()?)?
        .try_count(
            Stable::COUNT,
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
        .expect("statistics database reopens")?;
    assert_eq!(count, 1);
    Ok(())
}
