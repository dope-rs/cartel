use crate::api::stmt::Prepared;
use crate::sys;
use std::{marker, mem, ptr, result};

pub struct Binder<'statement, 'params> {
    statement: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>,
    remaining: usize,
    marker: marker::PhantomData<(&'statement mut libsqlite3_sys::sqlite3_stmt, &'params ())>,
}

impl<'statement, 'params> Binder<'statement, 'params> {
    pub(super) fn new(stmt: &'statement mut Prepared<'_>, bytes: usize) -> Self {
        Self {
            statement: stmt.statement,
            remaining: bytes,
            marker: marker::PhantomData,
        }
    }

    pub(crate) fn bind_null(&mut self, index: i32) -> result::Result<(), crate::Error> {
        let code = unsafe { libsqlite3_sys::sqlite3_bind_null(self.statement.as_ptr(), index) };
        self.result(sys::Action::BindNull, code)
    }

    pub(crate) fn bind_integer(
        &mut self,
        index: i32,
        value: i64,
    ) -> result::Result<(), crate::Error> {
        self.consume(mem::size_of::<i64>())?;
        let code =
            unsafe { libsqlite3_sys::sqlite3_bind_int64(self.statement.as_ptr(), index, value) };
        self.result(sys::Action::BindInteger, code)
    }

    pub(crate) fn bind_real(&mut self, index: i32, value: f64) -> result::Result<(), crate::Error> {
        self.consume(mem::size_of::<f64>())?;
        let code =
            unsafe { libsqlite3_sys::sqlite3_bind_double(self.statement.as_ptr(), index, value) };
        self.result(sys::Action::BindReal, code)
    }

    pub(crate) fn bind_text(
        &mut self,
        index: i32,
        value: &'params str,
    ) -> result::Result<(), crate::Error> {
        self.consume(value.len())?;
        let code = unsafe {
            libsqlite3_sys::sqlite3_bind_text64(
                self.statement.as_ptr(),
                index,
                value.as_ptr().cast(),
                value.len() as u64,
                libsqlite3_sys::SQLITE_STATIC(),
                libsqlite3_sys::SQLITE_UTF8 as u8,
            )
        };
        self.result(sys::Action::BindText, code)
    }

    pub(crate) fn bind_blob(
        &mut self,
        index: i32,
        value: &'params [u8],
    ) -> result::Result<(), crate::Error> {
        self.consume(value.len())?;
        let code = unsafe {
            libsqlite3_sys::sqlite3_bind_blob64(
                self.statement.as_ptr(),
                index,
                value.as_ptr().cast(),
                value.len() as u64,
                libsqlite3_sys::SQLITE_STATIC(),
            )
        };
        self.result(sys::Action::BindBlob, code)
    }

    fn consume(&mut self, bytes: usize) -> result::Result<(), crate::Error> {
        let left = self.remaining;
        let Some(next) = left.checked_sub(bytes) else {
            return Err(crate::Error::ParameterBytes {
                required: bytes,
                available: left,
            });
        };
        self.remaining = next;
        Ok(())
    }

    fn result(&self, action: sys::Action, code: i32) -> result::Result<(), crate::Error> {
        // SAFETY: this binder retains the exclusive prepared statement and database borrow.
        unsafe { sys::statement_result(self.statement, action, code) }
    }
}
