use std::{mem, path, ptr};

pub use action::Action;
pub use code::Code;
pub(crate) use fault::Fault;
use o3::{alloc::VecExt as _, base::marker::ThreadBound};
pub(crate) use sql::{Commands, Sql};
pub(crate) use step::Step;
pub(crate) use tx::Tx;

use crate::{open::journal, row, sys};

mod action;
mod code;
mod fault;
mod pragma;
mod sql;
mod step;
mod tx;

pub(crate) struct Connection {
    raw: ptr::NonNull<libsqlite3_sys::sqlite3>,
    _thread: ThreadBound,
}

pub(crate) struct Filename(pub(in crate::engine) std::ffi::CString);

struct Statement<'connection> {
    raw: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>,
    _connection: &'connection Connection,
}

#[must_use]
pub(crate) struct Completion<T> {
    pub(crate) result: Result<T, crate::Error>,
    pub(crate) cleanup: Option<Finalized>,
}

/// The exact error returned while consuming one native statement.
#[must_use]
#[repr(transparent)]
pub(crate) struct Finalized(Fault);

impl Finalized {
    pub(crate) fn into_error(self) -> crate::Error {
        self.0.error()
    }
}
enum Policy {
    ForeignKeys,
    Defensive,
    TrustedSchema,
}
impl Connection {
    const SAFETY_FLAGS: i32 = libsqlite3_sys::SQLITE_OPEN_NOMUTEX
        | libsqlite3_sys::SQLITE_OPEN_PRIVATECACHE
        | libsqlite3_sys::SQLITE_OPEN_NOFOLLOW
        | libsqlite3_sys::SQLITE_OPEN_EXRESCODE;

    pub(crate) fn open(path: &std::ffi::CStr, flags: i32) -> Result<Self, crate::Error> {
        let mut raw = ptr::null_mut();
        let code = unsafe {
            libsqlite3_sys::sqlite3_open_v2(
                path.as_ptr(),
                &mut raw,
                flags | Self::SAFETY_FLAGS,
                ptr::null(),
            )
        };
        let Some(raw) = ptr::NonNull::new(raw) else {
            return Err(sys::Fault::new(sys::Action::Open, sys::Code::new(code)).error());
        };
        let db = Self {
            raw,
            _thread: ThreadBound::NEW,
        };
        db.result(sys::Action::Open, code)?;
        db.configure(Policy::ForeignKeys)?;
        db.configure(Policy::Defensive)?;
        db.configure(Policy::TrustedSchema)?;
        Ok(db)
    }

    pub(crate) fn as_ptr(&self) -> *mut libsqlite3_sys::sqlite3 {
        self.raw.as_ptr()
    }

    pub(crate) fn transaction(&self) -> sys::Tx {
        transaction(self.raw.as_ptr())
    }

    pub(crate) fn limit(&self, id: i32, value: i32) -> i32 {
        unsafe { libsqlite3_sys::sqlite3_limit(self.raw.as_ptr(), id, value) }
    }

    pub(crate) fn autocheckpoint(&self, pages: i32) {
        // SAFETY: the live connection admits this infallible setter; it does not run the hook.
        unsafe { libsqlite3_sys::sqlite3_wal_autocheckpoint(self.raw.as_ptr(), pages) };
    }

    pub(crate) fn exec(&self, sql: &str) -> Completion<()> {
        Statement::run(self, Sql::new(sql), Statement::drain)
    }

    pub(crate) fn execute(&self, sql: Sql<'_>) -> Completion<()> {
        Statement::run(self, Ok(sql), Statement::done)
    }

    pub(crate) fn pragma(&self, name: &str, value: i64) -> Completion<()> {
        match pragma::Pragma::try_new(name, value) {
            Ok(pragma) => self.exec(pragma.as_str()),
            Err(error) => Completion {
                result: Err(error),
                cleanup: None,
            },
        }
    }

    pub(crate) fn query_i64(&self, sql: &str) -> Completion<i64> {
        Statement::run(self, Sql::new(sql), |stmt| {
            stmt.columns(1)?;
            match stmt.step()? {
                sys::Step::Row => {}
                sys::Step::Done => return Err(crate::Error::QueryReturnedNoRows),
            }
            let value = stmt.integer(0)?;
            stmt.end()?;
            Ok(value)
        })
    }

    pub(crate) fn journal(&self, sql: &str) -> Completion<journal::Mode> {
        Statement::run(self, Sql::new(sql), |stmt| {
            stmt.columns(1)?;
            match stmt.step()? {
                sys::Step::Row => {}
                sys::Step::Done => return Err(crate::Error::QueryReturnedNoRows),
            }
            stmt.kind(0, row::Class::Text)?;
            let bytes = stmt.text(0)?;
            let mode = if bytes.eq_ignore_ascii_case(b"delete") {
                journal::Mode::Delete
            } else if bytes.eq_ignore_ascii_case(b"truncate") {
                journal::Mode::Truncate
            } else if bytes.eq_ignore_ascii_case(b"persist") {
                journal::Mode::Persist
            } else if bytes.eq_ignore_ascii_case(b"memory") {
                journal::Mode::Memory
            } else if bytes.eq_ignore_ascii_case(b"wal") {
                journal::Mode::Wal
            } else if bytes.eq_ignore_ascii_case(b"off") {
                journal::Mode::Off
            } else {
                return Err(crate::Error::JournalMode);
            };
            stmt.end()?;
            Ok(mode)
        })
    }

    pub(crate) fn result(&self, action: sys::Action, code: i32) -> Result<(), crate::Error> {
        if code == libsqlite3_sys::SQLITE_OK {
            Ok(())
        } else {
            Err(self.failure(action, code).error())
        }
    }

    pub(crate) fn failure(&self, action: sys::Action, code: i32) -> sys::Fault {
        failure(self.raw.as_ptr(), action, code)
    }

    pub(crate) fn lookaside(
        &self,
        memory: *mut u8,
        layout: crate::memory::Lookaside,
    ) -> Result<(), crate::Error> {
        let (width, slots) = layout.sqlite_parts();
        let memory = if slots == 0 { ptr::null_mut() } else { memory };
        let code = unsafe {
            libsqlite3_sys::sqlite3_db_config(
                self.raw.as_ptr(),
                libsqlite3_sys::SQLITE_DBCONFIG_LOOKASIDE,
                memory.cast::<std::ffi::c_void>(),
                width,
                slots,
            )
        };
        self.result(sys::Action::Lookaside, code)
    }

    fn configure(&self, policy: Policy) -> Result<(), crate::Error> {
        let (option, enabled, action) = match policy {
            Policy::ForeignKeys => (
                libsqlite3_sys::SQLITE_DBCONFIG_ENABLE_FKEY,
                1,
                sys::Action::ForeignKeys,
            ),
            Policy::Defensive => (
                libsqlite3_sys::SQLITE_DBCONFIG_DEFENSIVE,
                1,
                sys::Action::Defensive,
            ),
            Policy::TrustedSchema => (
                libsqlite3_sys::SQLITE_DBCONFIG_TRUSTED_SCHEMA,
                0,
                sys::Action::TrustedSchema,
            ),
        };
        // These native flag cases apply enabled exactly; no readback is needed.
        let code = unsafe {
            libsqlite3_sys::sqlite3_db_config(
                self.raw.as_ptr(),
                option,
                enabled,
                ptr::null_mut::<std::ffi::c_int>(),
            )
        };
        self.result(action, code)
    }
}

/// # Safety
/// The statement and its original connection must both remain live for this call.
pub(crate) unsafe fn statement_result(
    stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>,
    action: Action,
    code: i32,
) -> Result<(), crate::Error> {
    if code == libsqlite3_sys::SQLITE_OK {
        return Ok(());
    }
    let db = unsafe { libsqlite3_sys::sqlite3_db_handle(stmt.as_ptr()) };
    Err(failure(db, action, code).error())
}

/// # Safety
/// The statement and its original connection must both remain live for this call.
pub(crate) unsafe fn statement_tx(stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>) -> Tx {
    let db = unsafe { libsqlite3_sys::sqlite3_db_handle(stmt.as_ptr()) };
    transaction(db)
}

fn transaction(db: *mut libsqlite3_sys::sqlite3) -> Tx {
    match unsafe { libsqlite3_sys::sqlite3_get_autocommit(db) } {
        0 => Tx::Active,
        _ => Tx::Idle,
    }
}

fn failure(db: *mut libsqlite3_sys::sqlite3, action: Action, code: i32) -> Fault {
    let extended = unsafe { libsqlite3_sys::sqlite3_extended_errcode(db) };
    let code = if extended == libsqlite3_sys::SQLITE_OK {
        code
    } else {
        extended
    };
    Fault::new(action, Code::new(code))
}

impl Filename {
    pub(crate) fn new(path: &path::Path, capacity: usize) -> Result<Self, crate::Error> {
        let bytes = path.as_os_str().as_encoded_bytes();
        if bytes.len() > capacity {
            return Err(crate::Error::PathBytes {
                actual: bytes.len(),
                capacity,
            });
        }
        if let Some(position) = bytes.iter().position(|byte| *byte == 0) {
            return Err(crate::Error::PathNul(position));
        }
        let total = bytes.len() + 1;
        let mut owned = Vec::new();
        owned.try_grow(total).map_err(crate::Error::Allocation)?;
        owned.extend_from_slice(bytes);
        owned.push(0);
        let path = unsafe { std::ffi::CString::from_vec_with_nul_unchecked(owned) };
        Ok(Self(path))
    }
}

/// # Safety
/// The statement and its original connection must remain live for this call.
pub(crate) unsafe fn statement_parameters(
    stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>,
    expected: usize,
) -> Result<u32, crate::Error> {
    // SAFETY: the retained native statement owns its nonnegative signed nVar.
    let actual = unsafe { libsqlite3_sys::sqlite3_bind_parameter_count(stmt.as_ptr()) } as u32;
    if expected != usize::MAX && actual as usize != expected {
        return Err(crate::Error::ParameterCount {
            expected,
            actual: actual as usize,
        });
    }
    Ok(actual)
}

/// # Safety
/// The statement and its original connection must remain live for this call.
pub(crate) unsafe fn statement_columns(stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>) -> u32 {
    // SAFETY: the live native statement returns its current u16 nResColumn.
    unsafe { libsqlite3_sys::sqlite3_column_count(stmt.as_ptr()) as u32 }
}

impl<'connection> Statement<'connection> {
    /// The borrowed operation cannot retain this statement, which is finalized exactly once.
    fn run<T>(
        db: &'connection Connection,
        sql: Result<Sql<'_>, crate::Error>,
        run: impl FnOnce(&mut Self) -> Result<T, crate::Error>,
    ) -> Completion<T> {
        let sql = match sql {
            Ok(sql) => sql,
            Err(error) => {
                return Completion {
                    result: Err(error),
                    cleanup: None,
                };
            }
        };
        sql.prepare(
            db,
            0,
            |error| Completion {
                result: Err(error),
                cleanup: None,
            },
            |raw, tail| {
                let mut stmt = Self {
                    raw,
                    _connection: db,
                };
                let result = tail.check().and_then(|()| run(&mut stmt));
                stmt.finish(result)
            },
        )
    }

    fn finish<T>(self, result: Result<T, crate::Error>) -> Completion<T> {
        let cleanup = finalize_fault(self.raw)
            .filter(|fault| !fault.is_repeated(&result))
            .map(Finalized);
        Completion { result, cleanup }
    }

    fn step(&mut self) -> Result<sys::Step, crate::Error> {
        let code = unsafe { libsqlite3_sys::sqlite3_step(self.raw.as_ptr()) };
        match code {
            libsqlite3_sys::SQLITE_ROW => Ok(sys::Step::Row),
            libsqlite3_sys::SQLITE_DONE => Ok(sys::Step::Done),
            code => Err(sys::Fault::new(sys::Action::Step, sys::Code::new(code)).error()),
        }
    }

    fn done(&mut self) -> Result<(), crate::Error> {
        // SAFETY: this owner retains its live statement and original connection.
        unsafe { statement_parameters(self.raw, 0) }?;
        self.columns(0)?;
        match self.step()? {
            sys::Step::Row => Err(crate::Error::UnexpectedRows {
                expected: 0,
                minimum: 1,
            }),
            sys::Step::Done => Ok(()),
        }
    }

    fn end(&mut self) -> Result<(), crate::Error> {
        match self.step()? {
            sys::Step::Row => Err(crate::Error::UnexpectedRows {
                expected: 1,
                minimum: 2,
            }),
            sys::Step::Done => Ok(()),
        }
    }

    fn drain(&mut self) -> Result<(), crate::Error> {
        loop {
            match self.step()? {
                sys::Step::Row => {}
                sys::Step::Done => return Ok(()),
            }
        }
    }

    fn columns(&self, expected: usize) -> Result<(), crate::Error> {
        // SAFETY: this owner retains its live statement and original connection.
        let actual = unsafe { statement_columns(self.raw) } as usize;
        if actual != expected {
            return Err(crate::Error::ColumnCount { expected, actual });
        }
        Ok(())
    }

    fn integer(&self, column: i32) -> Result<i64, crate::Error> {
        self.kind(column, row::Class::Integer)?;
        Ok(unsafe { libsqlite3_sys::sqlite3_column_int64(self.raw.as_ptr(), column) })
    }

    fn kind(&self, column: i32, expected: row::Class) -> Result<(), crate::Error> {
        let actual = unsafe { row::raw::kind(self.raw, column) };
        if !expected.is_match(actual) {
            return Err(crate::Error::ColumnType {
                column,
                expected,
                actual: actual.into_public(),
            });
        }
        Ok(())
    }

    fn text(&self, column: i32) -> Result<&[u8], crate::Error> {
        // SAFETY: the current row and checked column remain live while self is
        // borrowed; this internal caller reads no other representation of the column.
        unsafe { row::raw::text(self.raw, column) }
    }
}

pub(crate) fn finalize(stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>) -> Option<Finalized> {
    finalize_fault(stmt).map(Finalized)
}

fn finalize_fault(stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>) -> Option<Fault> {
    let code = unsafe { libsqlite3_sys::sqlite3_finalize(stmt.as_ptr()) };
    if code == libsqlite3_sys::SQLITE_OK {
        None
    } else {
        Some(Fault::new(Action::Finalize, Code::new(code)))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: self owns the live connection, with every statement retired
        // inside its allocator scope and no escaping backup or blob handle.
        // Native close can only reject an invalid handle or unretired objects.
        unsafe { libsqlite3_sys::sqlite3_close(self.raw.as_ptr()) };
    }
}

const _: () = {
    assert!(mem::size_of::<usize>() <= mem::size_of::<u64>());
    assert!(mem::size_of::<Finalized>() == mem::size_of::<Fault>());
    assert!(mem::size_of::<Option<Finalized>>() == mem::size_of::<Option<Fault>>());
};
