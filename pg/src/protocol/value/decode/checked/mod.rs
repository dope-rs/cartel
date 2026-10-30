use crate::{
    protocol::{Wire, value::decode},
    schema,
};
use std::{marker, str};

mod cursor;
mod sealed;
pub use cursor::Cursor;
pub(crate) use sealed::Sealed;

/// The closed field descriptions supported by retained, validated projection.
pub trait Fields: schema::Row + Sealed {
    fn read<'row>(cursor: &mut Cursor<'row>) -> Self::View<'row>;
}

/// A complete immutable row validated against this exact field description.
pub(crate) struct Row<'row, T: Fields> {
    bytes: &'row [u8],
    fields: marker::PhantomData<T>,
}

impl<'row, T: Fields> Row<'row, T> {
    pub(crate) fn validate(bytes: Wire<'row>, columns: u16) -> Result<Self, crate::Error> {
        let mut reader = decode::RowReader::new(bytes, columns)?;
        T::decode(&mut reader)?;
        reader.finish()?;
        Ok(Self {
            bytes: bytes.as_slice(),
            fields: marker::PhantomData,
        })
    }

    /// # Safety
    /// The immutable retained owner validated these same bytes as T.
    pub(crate) unsafe fn new(bytes: &'row [u8]) -> Self {
        Self {
            bytes,
            fields: marker::PhantomData,
        }
    }

    pub(crate) fn read(self) -> T::View<'row> {
        // SAFETY: validation consumed the complete two-byte row header.
        let bytes = unsafe { self.bytes.get_unchecked(2..) };
        T::read(&mut Cursor { bytes })
    }
}

const _: () = {
    assert!(size_of::<Row<'static, (i64, (String, ()))>>() == size_of::<&[u8]>());
    assert!(size_of::<Cursor<'static>>() == size_of::<&[u8]>());
};

impl<A: Fields> Fields for (A, ()) {
    fn read<'row>(cursor: &mut Cursor<'row>) -> Self::View<'row> {
        (A::read(cursor), ())
    }
}

impl<A: Fields, B: Fields> Fields for (A, B) {
    fn read<'row>(cursor: &mut Cursor<'row>) -> Self::View<'row> {
        (A::read(cursor), B::read(cursor))
    }
}

impl Fields for i32 {
    fn read<'row>(cursor: &mut Cursor<'row>) -> i32 {
        // SAFETY: the closed field sequence validated this nonnull four-byte scalar.
        let bytes = unsafe {
            cursor
                .bytes
                .as_ptr()
                .add(4)
                .cast::<[u8; 4]>()
                .read_unaligned()
        };
        cursor.bytes = unsafe { cursor.bytes.get_unchecked(8..) };
        i32::from_be_bytes(bytes)
    }
}

impl Fields for i64 {
    fn read<'row>(cursor: &mut Cursor<'row>) -> i64 {
        // SAFETY: the closed field sequence validated this nonnull eight-byte scalar.
        let bytes = unsafe {
            cursor
                .bytes
                .as_ptr()
                .add(4)
                .cast::<[u8; 8]>()
                .read_unaligned()
        };
        cursor.bytes = unsafe { cursor.bytes.get_unchecked(12..) };
        i64::from_be_bytes(bytes)
    }
}

impl Fields for String {
    fn read<'row>(cursor: &mut Cursor<'row>) -> &'row str {
        // SAFETY: validation proved the length prefix, complete nonnull payload and UTF-8.
        let length = unsafe { cursor.bytes.as_ptr().cast::<[u8; 4]>().read_unaligned() };
        let length = i32::from_be_bytes(length) as usize;
        let text = unsafe { cursor.bytes.get_unchecked(4..4 + length) };
        cursor.bytes = unsafe { cursor.bytes.get_unchecked(4 + length..) };
        unsafe { str::from_utf8_unchecked(text) }
    }
}
