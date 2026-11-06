use crate::common::plan;
use cartel_sqlite::{
    Action, Budget, Busy, Cache, Catalog, Code, Connection, Error, Limits, Lookaside, Memory,
    Parameters, Program, Progress, Results, Scan, Work,
};
use o3::base::marker::ThreadBound;
use std::{ffi, marker, num::NonZero, path::Path, ptr, time::Duration};

cartel_sqlite::catalog! {
pub ContentionDatabase;

pub const COMMANDS: &[&str] = &["CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY)"];

statements! {
    INSERT_ITEM(1, 0) = "INSERT INTO items (id) VALUES (?1)",
    COUNT_ITEMS(0, 1) = "SELECT count(*) FROM items",
}

pub fn try_settings() -> Result<(Limits, Memory), plan::Error> {
    Ok((
        Limits {
            parameters: Parameters {
                items: 1,
                bytes: 64,
            },
            scan: Scan {
                rows: 8,
                row_bytes: 4 << 10,
            },
            savepoints: 16,
            program: Program::new(NonZero::new(100_000).ok_or(plan::Error::Program)?),
            work: Work::uniform(Budget {
                progress: Progress::new(NonZero::new(100).ok_or(plan::Error::Progress)?),
                busy: Busy {
                    window: Duration::from_secs(2),
                    interval: Duration::from_millis(1),
                    retries: NonZero::<u32>::MIN.saturating_add(4_095),
                },
            }),
        },
        Memory {
            sqlite_bytes: 4 << 20,
            results: Results {
                rows: 8,
                bytes: 4 << 10,
            },
            statement_bytes: 128 << 10,
            cache: Cache {
                main_bytes: 1 << 20,
                temporary_bytes: 1 << 20,
            },
            lookaside: Lookaside::try_new(128, 64).ok_or(plan::Error::Lookaside)?,
        },
    ))
}

}

pub struct Blocker<'app> {
    raw: *mut libsqlite3_sys::sqlite3,
    _scope: marker::PhantomData<(&'app (), ThreadBound)>,
}

impl<'app> Blocker<'app> {
    pub fn try_new<I: Catalog>(
        _: &Connection<'app, '_, '_, I>,
        path: &Path,
    ) -> Result<Self, Error> {
        let path = ffi::CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|error| Error::PathNul(error.nul_position()))?;
        let mut raw = ptr::null_mut();
        // SAFETY: the client's application lifetime retains the current arena and
        // installed default Cartel VFS. Only Released can retire its native owner.
        // URI interpretation is disabled; this peer owns a private cache.
        let code = unsafe {
            libsqlite3_sys::sqlite3_open_v2(
                path.as_ptr(),
                &mut raw,
                libsqlite3_sys::SQLITE_OPEN_READWRITE
                    | libsqlite3_sys::SQLITE_OPEN_CREATE
                    | libsqlite3_sys::SQLITE_OPEN_NOMUTEX
                    | libsqlite3_sys::SQLITE_OPEN_PRIVATECACHE
                    | libsqlite3_sys::SQLITE_OPEN_NOFOLLOW
                    | libsqlite3_sys::SQLITE_OPEN_EXRESCODE,
                ptr::null(),
            )
        };
        let mut peer = Self {
            raw,
            _scope: marker::PhantomData,
        };
        Self::check(code, Action::Open)?;
        peer.exec(c"BEGIN; INSERT INTO items (id) VALUES (1)")?;
        Ok(peer)
    }

    pub fn lock_again(&mut self) -> Result<(), Error> {
        self.exec(c"BEGIN; INSERT INTO items (id) VALUES (3)")
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        self.exec(c"COMMIT")
    }

    fn exec(&mut self, sql: &ffi::CStr) -> Result<(), Error> {
        // SAFETY: this exclusive native handle remains in its admitted worker;
        // fixed fixture SQL publishes no callbacks and exec finalizes its statements.
        let code = unsafe {
            libsqlite3_sys::sqlite3_exec(
                self.raw,
                sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        Self::check(code, Action::Step)
    }

    fn check(code: i32, action: Action) -> Result<(), Error> {
        if code == libsqlite3_sys::SQLITE_OK {
            Ok(())
        } else {
            Err(Error::Sqlite {
                action,
                code: Code::new(code),
            })
        }
    }
}

impl Drop for Blocker<'_> {
    fn drop(&mut self) {
        // SAFETY: native exec retains no statement. Close also accepts a null or
        // partially opened handle and rolls back any transaction on early return.
        let code = unsafe { libsqlite3_sys::sqlite3_close(self.raw) };
        assert_eq!(code, libsqlite3_sys::SQLITE_OK, "blocking SQLite close");
    }
}
