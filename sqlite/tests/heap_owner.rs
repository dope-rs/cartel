use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use common::{assert_reclaimed, assert_retirement, engine::heap::*};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;

pub mod common;

#[test]
fn roundup_keeps_owner() -> Result<(), plan::Error> {
    // SAFETY: this binary has one test; only Cartel and same-worker allocator
    // checks use SQLite, retaining exclusive native custody until release.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    let (limits, memory) = try_settings()?;
    let result = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_run(
        |driver, mut database| {
            // SAFETY: this installed database retains the current thread's allocator scope.
            // Each nonnull native allocation remains live until its one release below.
            let moved =
                unsafe {
                    for (bytes, rounded) in [(1, 16), (16, 16), (17, 48), (48, 48), (49, 112)] {
                        let pointer = libsqlite3_sys::sqlite3_malloc64(bytes);
                        assert!(!pointer.is_null(), "rounding boundary: {bytes}");
                        assert_eq!(libsqlite3_sys::sqlite3_msize(pointer), rounded);
                        libsqlite3_sys::sqlite3_free(pointer);
                    }
                    let reserved = libsqlite3_sys::sqlite3_malloc64(112);
                    assert!(!reserved.is_null(), "order-two allocation");
                    let mut blocks = Vec::new();
                    loop {
                        let pointer = libsqlite3_sys::sqlite3_malloc64(16);
                        if pointer.is_null() {
                            break;
                        }
                        blocks.push(pointer);
                    }
                    // Only this admitted 128-byte block can supply the four units.
                    libsqlite3_sys::sqlite3_free(reserved);
                    let left = blocks.len();
                    blocks.extend([(); 4].map(|_| libsqlite3_sys::sqlite3_malloc64(16)));
                    assert!(blocks[left..].iter().enumerate().all(|(index, pointer)| {
                        pointer.addr() == reserved.addr() + index * 32
                    }));
                    let original = blocks[left];
                    original.cast::<u8>().write_bytes(0xa5, 16);
                    assert!(libsqlite3_sys::sqlite3_realloc64(original, 48).is_null());
                    libsqlite3_sys::sqlite3_free(blocks.remove(left + 1));
                    assert!(libsqlite3_sys::sqlite3_realloc64(original, 80).is_null());
                    assert_eq!(libsqlite3_sys::sqlite3_msize(original), 16);
                    assert_eq!(
                        std::slice::from_raw_parts(original.cast::<u8>(), 16),
                        &[0xa5; 16]
                    );
                    let grown = libsqlite3_sys::sqlite3_realloc64(original, 48);
                    assert_eq!(grown, original, "free right buddy grows in place");
                    assert_eq!(libsqlite3_sys::sqlite3_msize(grown), 48);
                    assert_eq!(
                        std::slice::from_raw_parts(grown.cast::<u8>(), 16),
                        &[0xa5; 16]
                    );
                    let source = blocks.remove(left + 1);
                    libsqlite3_sys::sqlite3_free(blocks.remove(left + 1));
                    libsqlite3_sys::sqlite3_free(blocks.remove(left));
                    source.cast::<u8>().write_bytes(0x5a, 16);
                    let moved = libsqlite3_sys::sqlite3_realloc64(source, 80);
                    assert_eq!(moved, original, "right then left buddies merge");
                    assert_eq!(libsqlite3_sys::sqlite3_msize(moved), 112);
                    assert_eq!(
                        std::slice::from_raw_parts(moved.cast::<u8>(), 16),
                        &[0x5a; 16]
                    );
                    for pointer in blocks {
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
                    moved
                };
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
            let absent = unsafe {
                let pointer = libsqlite3_sys::sqlite3_malloc64(16);
                assert!(!pointer.is_null(), "retained allocation");
                pointer.cast::<u8>().write_bytes(0xa5, 16);
                pointer
            };
            Ok([absent, moved])
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
                let grown = libsqlite3_sys::sqlite3_realloc64(other, 176);
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
