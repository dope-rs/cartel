/// The remaining fields of a validated row, available only to its field readers.
pub struct Cursor<'row> {
    pub(super) bytes: &'row [u8],
}
