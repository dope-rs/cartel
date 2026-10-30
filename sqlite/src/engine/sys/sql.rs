use crate::sys;
use std::{ptr, result};

pub(crate) struct Sql<'a> {
    text: &'a str,
}

/// The remaining bytes of the exact SQL input consumed by native preparation.
pub(crate) struct Tail<'sql> {
    suffix: &'sql [u8],
    offset: usize,
}

pub(crate) struct Commands<'sql> {
    sql: &'sql [&'sql str],
    max_bytes: usize,
}

impl<'sql> Commands<'sql> {
    pub(crate) const EMPTY: Self = Self {
        sql: &[],
        max_bytes: 0,
    };

    pub(crate) fn try_new(sql: &'sql [&'sql str]) -> result::Result<Self, crate::Error> {
        let mut bytes = 0;
        for text in sql {
            bytes = bytes.max(Sql::new(text)?.len());
        }
        Ok(Self {
            sql,
            max_bytes: bytes,
        })
    }

    pub(crate) const fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Preserves the immutable list's SQL validation when splitting its head.
    pub(crate) fn split_first(&self) -> Option<(Sql<'sql>, Self)> {
        let (&text, sql) = self.sql.split_first()?;
        let first = Sql { text };
        Some((
            first,
            Self {
                sql,
                max_bytes: self.max_bytes,
            },
        ))
    }
}

impl<'a> Sql<'a> {
    pub(crate) fn new(text: &'a str) -> result::Result<Self, crate::Error> {
        if let Some(offset) = text.as_bytes().iter().position(|byte| *byte == 0) {
            return Err(crate::Error::StatementNul { offset });
        }
        if text.len() > i32::MAX as usize {
            return Err(crate::Error::PrepareSqlBytes { actual: text.len() });
        }
        Ok(Self { text })
    }

    pub(crate) const fn len(&self) -> usize {
        self.text.len()
    }

    /// Keeps native preparation and its returned tail attached to this input.
    /// Successful preparation transfers the statement to its existing owner.
    pub(crate) fn prepare<R>(
        self,
        db: &sys::Connection,
        flags: u32,
        failed: impl FnOnce(crate::Error) -> R,
        ready: impl FnOnce(ptr::NonNull<libsqlite3_sys::sqlite3_stmt>, Tail<'a>) -> R,
    ) -> R {
        let mut stmt = ptr::null_mut();
        let mut tail = ptr::null();
        let code = unsafe {
            libsqlite3_sys::sqlite3_prepare_v3(
                db.as_ptr(),
                self.text.as_ptr().cast(),
                self.text.len() as i32,
                flags,
                &mut stmt,
                &mut tail,
            )
        };
        if code != libsqlite3_sys::SQLITE_OK {
            // Native preparation destroys any partial statement before returning an error.
            return failed(db.failure(sys::Action::Prepare, code).error());
        }
        let Some(stmt) = ptr::NonNull::new(stmt) else {
            return failed(crate::Error::StatementMissing { bytes: self.len() });
        };
        let bytes = self.text.as_bytes();
        // SAFETY: this successful native call maps its parser tail back into
        // the same NUL-free, length-admitted input, including its one-past end.
        let offset = unsafe { tail.cast::<u8>().offset_from_unsigned(bytes.as_ptr()) };
        // SAFETY: the same-call native tail identifies this exact input suffix.
        let suffix = unsafe { bytes.get_unchecked(offset..) };
        ready(stmt, Tail { suffix, offset })
    }
}

impl Tail<'_> {
    pub(crate) fn check(self) -> result::Result<(), crate::Error> {
        let Some(extra) = self
            .suffix
            .iter()
            .position(|byte| !byte.is_ascii_whitespace())
        else {
            return Ok(());
        };
        Err(crate::Error::StatementTail {
            offset: self.offset + extra,
        })
    }
}
