mod decode;
mod reader;
mod retainer;

pub use decode::Decode;
#[doc(hidden)]
pub use reader::Reader;
pub(crate) use retainer::Retainer;

use crate::sys;
use o3::base::num::bounded::NonZeroU32;
use std::{ptr, slice};

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub(crate) enum Kind {
    Integer = 1,
    Real = 2,
    Text = 3,
    Blob = 4,
    Null = 5,
}

impl Kind {
    pub(crate) const fn into_public(self) -> super::Kind {
        match self {
            Self::Integer => super::Kind::Integer,
            Self::Real => super::Kind::Real,
            Self::Text => super::Kind::Text,
            Self::Blob => super::Kind::Blob,
            Self::Null => super::Kind::Null,
        }
    }
}

unsafe extern "C" {
    fn sqlite3_column_type(
        stmt: *mut libsqlite3_sys::sqlite3_stmt,
        column: i32,
    ) -> NonZeroU32<1, 5>;
}

/// # Safety
/// The statement retains its current row and the admitted column.
/// The audited libsqlite3-sys 0.38.1 producer returns only 1..=5 through its native i32 ABI.
pub(crate) unsafe fn kind(stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>, column: i32) -> Kind {
    let raw = unsafe { sqlite3_column_type(stmt.as_ptr(), column) };
    match raw.get() {
        1 => Kind::Integer,
        2 => Kind::Real,
        3 => Kind::Text,
        4 => Kind::Blob,
        _ => Kind::Null,
    }
}

/// # Safety
/// The live row's column is admitted text; no operation may invalidate its UTF-8 representation during `'row`.
/// Materialization can fail only through OOM; preserve the exact native diagnostic.
pub(crate) unsafe fn text<'row>(
    stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>,
    column: i32,
) -> Result<&'row [u8], crate::Error> {
    let pointer = unsafe { libsqlite3_sys::sqlite3_column_text(stmt.as_ptr(), column) };
    let Some(pointer) = ptr::NonNull::new(pointer.cast_mut()) else {
        let code = unsafe { error(stmt) };
        return Err(crate::Error::Sqlite {
            action: sys::Action::ColumnText,
            code,
        });
    };
    let length = unsafe { length(stmt, column) };
    Ok(unsafe { slice::from_raw_parts(pointer.as_ptr(), length) })
}

/// # Safety
/// The live row's column is admitted blob; a non-OOM null pointer means zero bytes.
/// No operation may invalidate its byte representation during `'row`.
pub(super) unsafe fn blob<'row>(
    stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>,
    column: i32,
) -> Result<&'row [u8], crate::Error> {
    let pointer = unsafe { libsqlite3_sys::sqlite3_column_blob(stmt.as_ptr(), column) }
        .cast_mut()
        .cast::<u8>();
    let Some(pointer) = ptr::NonNull::new(pointer) else {
        let code = unsafe { error(stmt) };
        if code.primary() == libsqlite3_sys::SQLITE_NOMEM {
            return Err(sys::Fault::new(sys::Action::ColumnBlob, code).error());
        }
        return Ok(&[]);
    };
    let length = unsafe { length(stmt, column) };
    Ok(unsafe { slice::from_raw_parts(pointer.as_ptr(), length) })
}

unsafe fn error(stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>) -> sys::Code {
    let database = unsafe { libsqlite3_sys::sqlite3_db_handle(stmt.as_ptr()) };
    sys::Code::new(unsafe { libsqlite3_sys::sqlite3_extended_errcode(database) })
}

/// # Safety
/// The live row's admitted UTF-8 text or blob column has returned a nonnull materialized pointer and remains unchanged.
/// libsqlite3-sys 0.38.1 returns nonnegative `Mem.n`: UTF-8 needs no conversion, `MEM_Zero` is expanded, and native length/allocation limits keep `n <= i32::MAX`.
unsafe fn length(stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>, column: i32) -> usize {
    let actual = unsafe { libsqlite3_sys::sqlite3_column_bytes(stmt.as_ptr(), column) };
    actual as usize
}
