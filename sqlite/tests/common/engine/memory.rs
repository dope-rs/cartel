use cartel_sqlite::{Cache, Lookaside, Memory, Results};
use std::mem;

pub fn settings(bytes: usize) -> Memory {
    Memory {
        sqlite_bytes: bytes,
        results: Results { rows: 1, bytes: 1 },
        statement_bytes: 1,
        cache: Cache {
            main_bytes: 1,
            temporary_bytes: 1,
        },
        lookaside: Lookaside::DISABLED,
    }
}

/// # Safety
/// SQLite must be uninitialized and exclusively configured. `pages` must stay live,
/// immobile, and exclusively available while native code can use the installed cache.
pub unsafe fn custom_cache(pages: &mut [u64; 64]) -> std::ffi::c_int {
    unsafe extern "C" fn initialize(_: *mut std::ffi::c_void) -> std::ffi::c_int {
        libsqlite3_sys::SQLITE_ERROR
    }

    let mut cache = mem::MaybeUninit::<libsqlite3_sys::sqlite3_pcache_methods2>::uninit();
    let code = unsafe {
        libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_GETPCACHE2, cache.as_mut_ptr())
    };
    if code != libsqlite3_sys::SQLITE_OK {
        return code;
    }
    let mut cache = unsafe { cache.assume_init() };
    cache.xInit = Some(initialize);
    let code = unsafe {
        libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_PCACHE2, &raw const cache)
    };
    if code != libsqlite3_sys::SQLITE_OK {
        return code;
    }
    pages.fill(u64::MAX);
    unsafe {
        libsqlite3_sys::sqlite3_config(
            libsqlite3_sys::SQLITE_CONFIG_PAGECACHE,
            pages.as_mut_ptr().cast::<std::ffi::c_void>(),
            size_of_val(pages) as std::ffi::c_int,
            1,
        )
    }
}
