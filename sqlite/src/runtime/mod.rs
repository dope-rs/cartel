use dope_core::driver::schedule::clock::timer;
use o3::alloc;
use std::mem;

pub(crate) mod heap;
mod layout;
pub mod local;
pub mod raw;
pub(crate) mod slot;

pub use layout::Layout;

/// Proof that process-wide SQLite configuration is installed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Runtime(());

impl Runtime {
    /// Installs private native caches with a 20-page initial batch before workers start.
    /// Requires writable static data and no statement logging or statement virtual table.
    /// Build with `LIBSQLITE3_FLAGS=-USQLITE_ENABLE_MEMORY_MANAGEMENT -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS`.
    pub fn install(permit: raw::Permit) -> Result<Self, Error> {
        let raw::Permit(()) = permit.initialize()?;
        Ok(Self(()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// The native build shares page-cache allocations between connection arenas.
    GlobalCache,
    /// The native build retains unused global page-cache allocation statistics.
    CacheStats,
    /// The native statement virtual table measures memory after execution.
    StatementVtab,
    /// The native build emulates writable static data with fallible initialization.
    WritableStatic,
    /// The native SQL log can expand borrowed bindings during statement cleanup.
    StatementLog,
    Threading(crate::Code),
    Initialization(crate::Code),
    VfsMissing,
    VfsOpen,
    VfsFileSize {
        actual: std::ffi::c_int,
    },
    VfsRegistration(crate::Code),
    SqliteBytes,
    SqliteCapacity,
    /// Usable heap bytes exceed SQLite's signed statement memory counter.
    SqliteMemory {
        actual: usize,
    },
    StatementSqlBytes {
        actual: usize,
    },
    CapacityOverflow,
    Allocation(alloc::Error),
    Layout,
    TimerCapacity(timer::Exhausted),
}

const _: () = assert!(mem::size_of::<Runtime>() == 0);
