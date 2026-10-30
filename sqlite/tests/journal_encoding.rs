use cartel_sqlite::runtime::{self, Runtime};
use common::catalog::{Stable, open, try_limits, try_memory};
use common::{assert_reclaimed, assert_retirement, database, plan};
use dope_core::driver::life::recovery::Policy;
use dope_test::harness::support::file::Directory;
use std::path::Path;

pub mod common;

#[test]
fn reopen_utf16_journal() -> Result<(), plan::Error> {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = Directory::try_with(base, "cartel-sqlite-utf16-journal", |removed| {
        removed.expect("temporary directory cleanup")
    })
    .expect("temporary directory");
    let path = dir.path().join("catalog.db");
    // SAFETY: this binary has one test, and the fixture closes and shuts down SQLite.
    unsafe { database::utf16(&path) }.expect("UTF-16 fixture");
    // SAFETY: the fixture completed shutdown and no other thread can call SQLite.
    // Only Cartel uses installed SQLite; Core handles and allocations remain
    // exclusively owned by their originating worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    let count = plan::Plan::try_new(open(runtime, &path)?, try_limits()?, try_memory()?)?
        .try_count(
            Stable::COUNT,
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        )
        .expect("UTF-16 database opens")?;
    assert_eq!(count, 1);
    Ok(())
}
