/// The remaining fields of a validated row, available only to its field readers.
pub struct Cursor<'row> {
    pub(super) bytes: &'row [u8],
}

impl<'row> Cursor<'row> {
    /// # Safety
    /// The next field has a complete nonnull payload of exactly N bytes.
    pub(super) unsafe fn fixed<const N: usize>(&mut self) -> [u8; N] {
        let value = unsafe {
            self.bytes
                .as_ptr()
                .add(4)
                .cast::<[u8; N]>()
                .read_unaligned()
        };
        self.bytes = unsafe { self.bytes.get_unchecked(4 + N..) };
        value
    }

    /// # Safety
    /// The next field has a complete nonnull payload of at least PREFIX bytes,
    /// and the payload after PREFIX is valid UTF-8.
    pub(super) unsafe fn text<const PREFIX: usize>(&mut self) -> &'row str {
        let length = unsafe { self.bytes.as_ptr().cast::<[u8; 4]>().read_unaligned() };
        let length = i32::from_be_bytes(length) as usize;
        let text = unsafe { self.bytes.get_unchecked(4 + PREFIX..4 + length) };
        self.bytes = unsafe { self.bytes.get_unchecked(4 + length..) };
        unsafe { std::str::from_utf8_unchecked(text) }
    }
}
