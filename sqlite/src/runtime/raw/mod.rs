mod vfs;

use crate::{
    runtime::{self, Error, heap, local},
    sys,
};
use heap::layout::Layout;
use o3::alloc;
use std::{mem, ptr};

#[repr(C, align(64))]
struct Unit {
    bytes: [mem::MaybeUninit<u8>; Span::ALIGNMENT],
}

pub(crate) struct Memory {
    memory: alloc::array::Block<mem::MaybeUninit<Unit>, usize>,
}

pub(crate) struct Span {
    base: ptr::NonNull<u8>,
    bytes: usize,
}

/// Exclusive native custody lasts for the process once allocator hooks are installed, even if installation fails; copying or dropping `Runtime` does not release it.
///
/// Arena-backed native handles and allocations must remain exclusively owned by their originating worker until release; raw calls and other wrappers must preserve this custody and Cartel's admitted execution. Foreign initialization and VFS allocations retain their std allocation family and audited native synchronization.
///
/// After allocator hook installation, callers must not reconfigure or replace the allocator, page cache, VFS or callbacks, enable URI interpretation, shared caches or native workers, install unaudited extensions, or publish arena-backed storage into global native state.
#[must_use]
pub struct Permit(pub(super) ());

/// Exclusive installation authority for a build with infallible native shutdown.
struct Setup(Permit);

impl Permit {
    /// # Safety
    /// SQLite must be uninitialized, and no other thread may call SQLite until this permit is consumed by `Runtime::install` and that call returns; the caller must uphold [`Permit`]'s process-lifetime custody contract.
    /// The initial default VFS, mutex methods, logging callbacks and auto-extension registry must belong to the audited native implementation without unaudited callbacks or extensions; allocator and page-cache tables replaced without execution may already be installed.
    pub unsafe fn exclusive() -> Self {
        Self(())
    }

    pub(super) fn initialize(self) -> Result<Self, Error> {
        if unsafe {
            libsqlite3_sys::sqlite3_compileoption_used(c"ENABLE_MEMORY_MANAGEMENT".as_ptr())
        } != 0
        {
            return Err(Error::GlobalCache);
        }
        if unsafe {
            libsqlite3_sys::sqlite3_compileoption_used(c"DISABLE_PAGECACHE_OVERFLOW_STATS".as_ptr())
        } == 0
        {
            return Err(Error::CacheStats);
        }
        if unsafe { libsqlite3_sys::sqlite3_compileoption_used(c"ENABLE_STMTVTAB".as_ptr()) } != 0 {
            return Err(Error::StatementVtab);
        }
        if unsafe { libsqlite3_sys::sqlite3_compileoption_used(c"OMIT_WSD".as_ptr()) } != 0 {
            return Err(Error::WritableStatic);
        }
        if unsafe { libsqlite3_sys::sqlite3_compileoption_used(c"ENABLE_SQLLOG".as_ptr()) } != 0 {
            return Err(Error::StatementLog);
        }
        Setup(self).initialize()
    }
}

impl Setup {
    fn initialize(self) -> Result<Permit, Error> {
        use heap::raw;
        use vfs::Vfs;

        let code =
            unsafe { libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_MULTITHREAD) };
        if code != libsqlite3_sys::SQLITE_OK {
            return Err(Error::Threading(sys::Code::new(code)));
        }
        // SAFETY: MULTITHREAD admitted the uninitialized phase, and Permit excludes
        // concurrent calls. These native cases only copy values, invoke no callbacks,
        // and return SQLITE_OK without initializing SQLite.
        unsafe {
            libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_URI, 0);
            libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_MEMSTATUS, 0);
            // Each connection receives its owned lookaside before its first SQL.
            libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_LOOKASIDE, 0, 0);
            libsqlite3_sys::sqlite3_config(
                libsqlite3_sys::SQLITE_CONFIG_PAGECACHE,
                ptr::null_mut::<std::ffi::c_void>(),
                0,
                20,
            );
        }
        let cache = libsqlite3_sys::sqlite3_pcache_methods2 {
            iVersion: 1,
            pArg: ptr::null_mut(),
            xInit: None,
            xShutdown: None,
            xCreate: None,
            xCachesize: None,
            xPagecount: None,
            xFetch: None,
            xUnpin: None,
            xRekey: None,
            xTruncate: None,
            xDestroy: None,
            xShrink: None,
        };
        // SAFETY: the admitted phase permits this callback-free table copy.
        // A null xInit selects the audited native cache, replacing any prior custom cache.
        unsafe {
            libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_PCACHE2, &raw const cache);
        }
        let hooks = raw::methods();
        // SAFETY: the same phase is retained; MALLOC copies this complete table by
        // value and returns SQLITE_OK without invoking its callbacks.
        unsafe {
            libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_MALLOC, &raw const hooks);
        }
        let scope = raw::Foreign::enter();
        let code = unsafe { libsqlite3_sys::sqlite3_initialize() };
        let result = if code == libsqlite3_sys::SQLITE_OK {
            unsafe { Vfs::install() }
        } else {
            Err(Error::Initialization(sys::Code::new(code)))
        };
        if result.is_err() {
            // SAFETY: Setup retains exclusive startup authority and rejects OMIT_WSD,
            // the only native build path where shutdown can return a non-OK code.
            unsafe { libsqlite3_sys::sqlite3_shutdown() };
        }
        drop(scope);
        result?;
        Ok(self.0)
    }
}

impl Memory {
    /// Layout retains statement bytes already aligned by stmt::bytes.
    pub(in crate::runtime) fn new(
        sqlite: Layout,
        collect: usize,
        stmts: usize,
        aside: usize,
        phase: std::alloc::Layout,
    ) -> Result<(Self, heap::raw::Span, Span, Span, Span, Span), runtime::Error> {
        if phase.align() > Span::ALIGNMENT {
            return Err(runtime::Error::Layout);
        }
        let phase_bytes = Self::round(phase.size())?;
        let rounded = Self::round(collect)?;
        let aside = Self::round(aside)?;
        let arena = sqlite.backing().ok_or(runtime::Error::CapacityOverflow)?;
        let control = local::State::<crate::catalogs::Empty>::RETENTION_BYTES;
        let start = control
            .checked_add(phase_bytes)
            .ok_or(runtime::Error::CapacityOverflow)?;
        let retain = start
            .checked_add(arena)
            .ok_or(runtime::Error::CapacityOverflow)?;
        let bytes = retain
            .checked_add(rounded)
            .and_then(|total| total.checked_add(stmts))
            .and_then(|total| total.checked_add(aside))
            .ok_or(runtime::Error::CapacityOverflow)?;
        let units = bytes.div_ceil(Span::ALIGNMENT);
        let mut memory =
            alloc::array::Block::try_new(units, ()).map_err(runtime::Error::Allocation)?;
        // SAFETY: a nonempty checked byte total produces at least one allocated Unit.
        let base = unsafe { ptr::NonNull::new_unchecked(memory.as_mut_ptr().cast()) };
        let collect = Span::new(base, retain, collect);
        let stmts = Span::new(base, retain + rounded, stmts);
        let aside = Span::new(base, retain + rounded + stmts.len(), aside);
        let phase = Span::new(base, control, phase.size());
        let start = Span::new(base, start, arena).base();
        // SAFETY: start and Layout describe the checked SQLite partition in this allocation.
        let sqlite = unsafe { heap::raw::Span::new(start, sqlite) };
        Ok((Self { memory }, sqlite, collect, stmts, aside, phase))
    }

    fn round(bytes: usize) -> Result<usize, runtime::Error> {
        bytes
            .checked_add(Span::ALIGNMENT - 1)
            .map(|rounded| rounded / Span::ALIGNMENT * Span::ALIGNMENT)
            .ok_or(runtime::Error::CapacityOverflow)
    }

    pub(in crate::runtime) fn base(&mut self) -> ptr::NonNull<u8> {
        // SAFETY: Memory always owns at least one aligned Unit.
        unsafe { ptr::NonNull::new_unchecked(self.memory.as_mut_ptr().cast()) }
    }
}

impl Span {
    pub(crate) const ALIGNMENT: usize = 64;

    fn new(base: ptr::NonNull<u8>, start: usize, bytes: usize) -> Self {
        // SAFETY: Memory bounds every partition start by its checked total allocation size.
        let base = unsafe { ptr::NonNull::new_unchecked(base.as_ptr().add(start)) };
        Self { base, bytes }
    }

    pub(crate) const fn into_parts(self) -> (ptr::NonNull<u8>, usize) {
        (self.base, self.bytes)
    }

    pub(crate) const fn as_mut_ptr(&mut self) -> *mut u8 {
        self.base.as_ptr()
    }

    pub(crate) const fn len(&self) -> usize {
        self.bytes
    }

    pub(crate) const fn base(&self) -> ptr::NonNull<u8> {
        self.base
    }
}

const _: () = {
    assert!(mem::size_of::<Unit>() == Span::ALIGNMENT);
    assert!(mem::align_of::<Unit>() == Span::ALIGNMENT);
};
