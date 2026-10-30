use cartel_sqlite::{Action, Error, Lookaside, OpenOptions, database, open, runtime};
use common::{
    assert_reclaimed, assert_retirement,
    catalog::{self, Commands},
    engine::memory::settings,
    plan,
};
use dope_core::driver::life::recovery::Policy;
use dope_runtime::topology;
use std::path;

pub mod common;

#[test]
fn validates_heap_layout() {
    assert!(
        settings((64 << 10) - 64)
            .reservation::<Commands>()
            .is_none()
    );
    assert!(settings((64 << 10) + 1).reservation::<Commands>().is_none());
    let plan = settings(64 << 10).reservation::<Commands>();
    assert_eq!(plan.map(|plan| plan.sqlite_capacity), Some(63_520));
}

#[test]
fn validates_lookaside_layout() {
    assert!(Lookaside::try_new(size_of::<usize>() as u16, 1).is_none());
    assert_eq!(Lookaside::try_new(16, 1).map(Lookaside::bytes), Some(16));
    assert!(Lookaside::try_new(65_528, 32_771).is_some());
    assert!(Lookaside::try_new(65_528, 32_772).is_none());
    assert_eq!(Lookaside::DISABLED.bytes(), 0);
}

#[test]
fn native_profile() -> Result<(), plan::Error> {
    let cache =
        unsafe { libsqlite3_sys::sqlite3_compileoption_used(c"ENABLE_MEMORY_MANAGEMENT".as_ptr()) };
    let stats = unsafe {
        libsqlite3_sys::sqlite3_compileoption_used(c"DISABLE_PAGECACHE_OVERFLOW_STATS".as_ptr())
    };
    let vtab = unsafe { libsqlite3_sys::sqlite3_compileoption_used(c"ENABLE_STMTVTAB".as_ptr()) };
    let wsd = unsafe { libsqlite3_sys::sqlite3_compileoption_used(c"OMIT_WSD".as_ptr()) };
    let log = unsafe { libsqlite3_sys::sqlite3_compileoption_used(c"ENABLE_SQLLOG".as_ptr()) };
    assert_eq!(
        unsafe { libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_URI, 1) },
        libsqlite3_sys::SQLITE_OK
    );
    let mut pages = [0; 64];
    assert_eq!(
        unsafe { common::engine::memory::custom_cache(&mut pages) },
        libsqlite3_sys::SQLITE_OK
    );
    // SAFETY: this binary initializes alone; prior cache overrides are replaced
    // without execution. Profile rejections return before hooks are installed.
    // Only Cartel uses installed SQLite; Core handles and allocations remain
    // exclusively owned by their originating worker.
    let result = runtime::Runtime::install(unsafe { runtime::raw::Permit::exclusive() });
    let expected = if cache != 0 {
        Some(runtime::Error::GlobalCache)
    } else if stats == 0 {
        Some(runtime::Error::CacheStats)
    } else if vtab != 0 {
        Some(runtime::Error::StatementVtab)
    } else if wsd != 0 {
        Some(runtime::Error::WritableStatic)
    } else if log != 0 {
        Some(runtime::Error::StatementLog)
    } else {
        None
    };
    if let Some(error) = expected {
        assert_eq!(result.err(), Some(error));
        assert_eq!(
            unsafe { libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_URI, 0) },
            libsqlite3_sys::SQLITE_OK
        );
        return Ok(());
    }
    let runtime = result.map_err(plan::Error::Runtime)?;
    assert_eq!(pages[0], u64::MAX);
    let mut memory = catalog::try_memory()?;
    for (bytes, expected) in [
        ((64 << 10) - 64, runtime::Error::SqliteBytes),
        ((64 << 10) + 1, runtime::Error::SqliteBytes),
        (usize::MAX & !63, runtime::Error::CapacityOverflow),
    ] {
        memory.sqlite_bytes = bytes;
        assert!(matches!(
            OpenOptions::new()
                .open_memory(runtime)
                .map_err(plan::Error::Client)?
                .plan::<Commands>(catalog::try_limits()?, memory),
            Err(database::Error::Runtime(actual)) if actual == expected
        ));
    }
    memory.sqlite_bytes = 2_214_592_448;
    assert!(
        OpenOptions::new()
            .open_memory(runtime)
            .map_err(plan::Error::Client)?
            .plan::<Commands>(catalog::try_limits()?, memory)
            .is_ok()
    );
    memory.sqlite_bytes += 64;
    assert!(matches!(
        OpenOptions::new()
            .open_memory(runtime)
            .map_err(plan::Error::Client)?
            .plan::<Commands>(catalog::try_limits()?, memory),
        Err(database::Error::Runtime(runtime::Error::SqliteMemory { actual }))
            if actual == i32::MAX as usize + 1
    ));
    let path = path::Path::new("file::memory:?vfs=unix&cache=shared");
    assert!(!path.exists());
    let target = catalog::readonly(runtime, path)?;
    let result =
        plan::Plan::<Commands>::try_new(target, catalog::try_limits()?, catalog::try_memory()?)?
            .try_install(Policy::new(assert_reclaimed, assert_retirement), |result| {
                result.expect("SQLite scope")
            });
    assert!(matches!(result,
        Err(plan::Failure::Install(
            topology::InstallError::Install(open::Error::Step(Error::Sqlite {
                action: Action::Open, code
            }))
        )) if code.primary() == libsqlite3_sys::SQLITE_CANTOPEN
    ));
    Ok(())
}
