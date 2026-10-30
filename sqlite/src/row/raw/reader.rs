use std::{marker, mem, num, ptr, result};

use crate::{api::stmt::Prepared, row};

pub struct Reader<'row> {
    stmt: ptr::NonNull<libsqlite3_sys::sqlite3_stmt>,
    left: usize,
    column: i32,
    kind: Option<row::raw::Kind>,
    /// Positive, zero-based future column, found strictly after the current column.
    known: Option<num::NonZero<u16>>,
    future: row::raw::Kind,
    marker: marker::PhantomData<&'row mut libsqlite3_sys::sqlite3_stmt>,
}

pub(super) enum Presence {
    Null,
    Value,
}

pub(super) enum Value<'row> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'row [u8]),
    Blob(&'row [u8]),
}

impl Value<'_> {
    pub(super) const fn kind(&self) -> row::Kind {
        match self {
            Self::Null => row::Kind::Null,
            Self::Integer(_) => row::Kind::Integer,
            Self::Real(_) => row::Kind::Real,
            Self::Text(_) => row::Kind::Text,
            Self::Blob(_) => row::Kind::Blob,
        }
    }
}

impl<'row> Reader<'row> {
    const LAYOUT: () =
        assert!(mem::size_of::<Self>() == 2 * mem::size_of::<usize>() + 2 * mem::size_of::<i32>());

    pub(crate) fn new(
        stmt: &'row mut Prepared<'_>,
        capacity: num::NonZero<u32>,
        layout: usize,
    ) -> result::Result<Self, crate::Error> {
        let capacity = capacity.get() as usize;
        let Some(left) = capacity.checked_sub(layout) else {
            return Err(crate::Error::RowBytes {
                required: layout,
                available: capacity,
            });
        };
        Ok(Self {
            stmt: stmt.as_non_null(),
            left,
            column: 0,
            kind: None,
            known: None,
            future: row::raw::Kind::Null,
            marker: marker::PhantomData,
        })
    }

    pub(super) fn column(&self) -> i32 {
        self.column
    }

    /// Rows admits Decode's span against the native u16 result count.
    /// A retained NULL kind carries its prefix to the next non-NULL column.
    /// An endpoint before the span end proves presence; ending at it promotes its kind.
    pub(super) fn take_nulls(&mut self, columns: usize) -> Presence {
        if columns == 0 {
            return Presence::Null;
        }
        let cached = self.kind;
        if self.kind() != row::raw::Kind::Null {
            return Presence::Value;
        }
        let end = self.column + columns as i32;
        if cached.is_some()
            && let Some(known) = self.known
        {
            if i32::from(known.get()) < end {
                return Presence::Value;
            }
            self.skip_nulls(end);
            return Presence::Null;
        }
        for column in self.column + 1..end {
            let kind = unsafe { row::raw::kind(self.stmt, column) };
            if kind != row::raw::Kind::Null {
                self.future = kind;
                self.known = num::NonZero::new(column as u16);
                return Presence::Value;
            }
        }
        self.column = end;
        self.kind = None;
        Presence::Null
    }

    pub(super) fn value(&mut self) -> result::Result<Value<'row>, crate::Error> {
        let stmt = self.stmt.as_ptr();
        let column = self.column;
        let (value, bytes) = match self.kind() {
            row::raw::Kind::Null => {
                self.skip_nulls(column + 1);
                return Ok(Value::Null);
            }
            row::raw::Kind::Integer => {
                let value = unsafe { libsqlite3_sys::sqlite3_column_int64(stmt, column) };
                (Value::Integer(value), mem::size_of::<i64>())
            }
            row::raw::Kind::Real => {
                let value = unsafe { libsqlite3_sys::sqlite3_column_double(stmt, column) };
                (Value::Real(value), mem::size_of::<f64>())
            }
            row::raw::Kind::Text => {
                // SAFETY: Decode visits each admitted column once while the
                // exclusive row borrow prevents stepping or resetting the statement.
                let value = unsafe { row::raw::text(self.stmt, column) }?;
                (Value::Text(value), value.len())
            }
            row::raw::Kind::Blob => {
                // SAFETY: this admitted blob column remains unchanged for the
                // exclusive row borrow carried by Reader.
                let value = unsafe { row::raw::blob(self.stmt, column) }?;
                (Value::Blob(value), value.len())
            }
        };
        let left = self.left;
        let Some(next) = left.checked_sub(bytes) else {
            return Err(crate::Error::RowBytes {
                required: bytes,
                available: left,
            });
        };
        self.left = next;
        self.column += 1;
        self.kind = None;
        Ok(value)
    }

    fn kind(&mut self) -> row::raw::Kind {
        if let Some(kind) = self.kind {
            return kind;
        }
        let kind = unsafe { row::raw::kind(self.stmt, self.column) };
        self.kind = Some(kind);
        kind
    }

    fn skip_nulls(&mut self, column: i32) {
        self.column = column;
        if let Some(known) = self.known {
            let known = i32::from(known.get());
            if column < known {
                self.kind = Some(row::raw::Kind::Null);
                return;
            }
            self.known = None;
            if column == known {
                self.kind = Some(self.future);
                return;
            }
        }
        self.kind = None;
    }
}

const _: () = Reader::LAYOUT;
