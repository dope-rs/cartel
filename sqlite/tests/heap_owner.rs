use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use common::{assert_reclaimed, assert_retirement, engine::heap::*};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::sync::OnceLock;

pub mod common;

/// Only Cartel and same-worker allocator checks use installed SQLite; Core handles
/// and allocations stay exclusively with their original worker while hooks can run.
static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn roundup_keeps_owner() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: initialization excludes other SQLite calls; this binary preserves native custody.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings()?;
    let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
        |driver, mut database| {
            // SAFETY: this installed database retains the current thread's allocator scope.
            // Each nonnull native allocation remains live until its one release below.
            unsafe {
                for (bytes, rounded) in [(1, 16), (16, 16), (17, 48), (48, 48), (49, 112)] {
                    let pointer = libsqlite3_sys::sqlite3_malloc64(bytes);
                    assert!(!pointer.is_null(), "rounding boundary: {bytes}");
                    assert_eq!(libsqlite3_sys::sqlite3_msize(pointer), rounded);
                    libsqlite3_sys::sqlite3_free(pointer);
                }
                let original = libsqlite3_sys::sqlite3_malloc64(16);
                assert!(!original.is_null(), "original allocation");
                original.cast::<u8>().write_bytes(0xa5, 16);
                let grown = libsqlite3_sys::sqlite3_realloc64(original, 80);
                assert!(!grown.is_null(), "successful allocation growth");
                let size = libsqlite3_sys::sqlite3_msize(grown);
                assert!(size >= 80);
                let original = libsqlite3_sys::sqlite3_realloc64(grown, 16);
                assert_eq!(original, grown, "shrinking keeps the existing block");
                for bytes in [(1_u64 << 30) - 15, SQLITE_BYTES as u64] {
                    let failed = libsqlite3_sys::sqlite3_realloc64(original, bytes);
                    assert!(failed.is_null(), "rejected allocation: {bytes}");
                    let next = libsqlite3_sys::sqlite3_malloc64(16);
                    assert!(!next.is_null(), "second allocation");
                    assert_ne!(next, original, "failed realloc must retain its owner");
                    assert_eq!(libsqlite3_sys::sqlite3_msize(original), size);
                    assert_eq!(
                        std::slice::from_raw_parts(original.cast::<u8>(), 16),
                        &[0xa5; 16]
                    );
                    libsqlite3_sys::sqlite3_free(next);
                }
                assert!(libsqlite3_sys::sqlite3_realloc64(original, 0).is_null());
            }
            let value = fibers::TEST
                .try_run(
                    driver,
                    database
                        .query()
                        .run_one::<i64, _, _>(HeapDatabase::CHECK, ()),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            assert_eq!(value, 1);
            // SAFETY: these blocks remain on this worker; their original arena
            // retains its backing until both native allocations are released.
            let retained = [0xa5_u8, 0x5a].map(|byte| unsafe {
                let pointer = libsqlite3_sys::sqlite3_malloc64(16);
                assert!(!pointer.is_null(), "retained allocation");
                pointer.cast::<u8>().write_bytes(byte, 16);
                pointer
            });
            Ok(retained)
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    let [absent, other] = result.expect("rounded allocation cleanup scope")?;
    // SAFETY: installation remains active, the first Scope has ended, and this
    // worker still exclusively owns both original native allocations.
    unsafe {
        assert!(libsqlite3_sys::sqlite3_malloc64(16).is_null());
        let grown = libsqlite3_sys::sqlite3_realloc64(absent, 80);
        assert!(!grown.is_null(), "growth without an allocator scope");
        let size = libsqlite3_sys::sqlite3_msize(grown);
        assert!(size >= 80);
        assert!(libsqlite3_sys::sqlite3_realloc64(grown, SQLITE_BYTES as u64).is_null());
        assert_eq!(libsqlite3_sys::sqlite3_msize(grown), size);
        assert_eq!(
            std::slice::from_raw_parts(grown.cast::<u8>(), 16),
            &[0xa5; 16]
        );
        libsqlite3_sys::sqlite3_free(grown);
    }
    let (limits, memory) = try_settings()?;
    let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
        |driver, mut database| {
            // SAFETY: the live block retains its original arena on this worker,
            // while this database installs a different allocator scope.
            unsafe {
                let grown = libsqlite3_sys::sqlite3_realloc64(other, 80);
                assert!(!grown.is_null(), "growth under a different allocator scope");
                assert_eq!(
                    std::slice::from_raw_parts(grown.cast::<u8>(), 16),
                    &[0x5a; 16]
                );
                libsqlite3_sys::sqlite3_free(grown);
            }
            let value = fibers::TEST
                .try_run(
                    driver,
                    database
                        .query()
                        .run_one::<i64, _, _>(HeapDatabase::CHECK, ()),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)?;
            assert_eq!(value, 1);
            Ok(())
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("SQLite scope"),
    );
    result.expect("retained allocation cleanup scope")
}
