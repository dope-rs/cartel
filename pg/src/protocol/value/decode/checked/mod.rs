use crate::protocol::{Wire, value::decode};

mod cursor;
mod sealed;
pub use cursor::Cursor;
pub(crate) use sealed::Sealed;

/// The closed field descriptions supported by retained, validated projection.
pub trait Fields: Sealed {
    fn read<'row>(cursor: &mut Cursor<'row>) -> Self::View<'row>;
}

pub(crate) fn validate<T: Fields>(bytes: Wire<'_>, columns: u16) -> Result<(), crate::Error> {
    let mut reader = decode::RowReader::new(bytes, columns)?;
    <T as Sealed>::validate(&mut reader)?;
    reader.finish()
}

/// # Safety
/// The immutable retained owner validated these same bytes as T.
pub(crate) unsafe fn read<T: Fields>(bytes: &[u8]) -> T::View<'_> {
    // SAFETY: validation consumed the complete two-byte row header.
    let bytes = unsafe { bytes.get_unchecked(2..) };
    T::read(&mut Cursor { bytes })
}

const _: () = {
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

impl Fields for bool {
    fn read<'row>(cursor: &mut Cursor<'row>) -> bool {
        // SAFETY: the closed field sequence validated a nonnull byte equal to zero or one.
        let [value] = unsafe { cursor.fixed() };
        value != 0
    }
}

impl Fields for i32 {
    fn read<'row>(cursor: &mut Cursor<'row>) -> i32 {
        // SAFETY: the closed field sequence validated this nonnull four-byte scalar.
        i32::from_be_bytes(unsafe { cursor.fixed() })
    }
}

impl Fields for i64 {
    fn read<'row>(cursor: &mut Cursor<'row>) -> i64 {
        // SAFETY: the closed field sequence validated this nonnull eight-byte scalar.
        i64::from_be_bytes(unsafe { cursor.fixed() })
    }
}

impl Fields for String {
    fn read<'row>(cursor: &mut Cursor<'row>) -> &'row str {
        // SAFETY: validation proved the length prefix, complete nonnull payload and UTF-8.
        unsafe { cursor.text::<0>() }
    }
}

impl Fields for crate::Jsonb {
    fn read<'row>(cursor: &mut Cursor<'row>) -> &'row str {
        // SAFETY: validation also proved the version byte and UTF-8 after that byte.
        unsafe { cursor.text::<1>() }
    }
}
