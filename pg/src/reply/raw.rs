use crate::reply::{group, server};
use o3::buffer::{self, hold::scope::Deferred, resident, resident::copy};
use std::{array, num::NonZero, ops::Range, str};

/// The two validated notification fields and their original paying source.
#[must_use]
pub(crate) struct Fields<'input, 'd, R: buffer::Source + 'input> {
    source: Deferred<'input, 'd, R>,
    fields: [[u32; 2]; 2],
    len: u32,
}

impl<'input, 'd, R: buffer::Source + 'input> Fields<'input, 'd, R> {
    /// # Safety
    /// The ranges must be ordered, in bounds, valid UTF-8, and fit u32 in
    /// endpoints and total length within this unchanged source.
    pub(crate) unsafe fn new(source: Deferred<'input, 'd, R>, ranges: [Range<usize>; 2]) -> Self {
        let mut len = 0u32;
        let fields = ranges.map(|range| {
            let start = range.start as u32;
            let end = range.end as u32;
            len = len.wrapping_add(end.wrapping_sub(start));
            [start, end]
        });
        Self {
            source,
            fields,
            len,
        }
    }

    pub(crate) fn get<const I: usize>(&self) -> &'input str {
        const { assert!(I < 2) };
        self.field(I)
    }

    fn field(&self, index: usize) -> &'input str {
        let [start, end] = self.fields[index];
        // SAFETY: construction binds these validated spans to this exact source.
        let bytes = unsafe {
            self.source
                .as_slice()
                .get_unchecked(start as usize..end as usize)
        };
        // SAFETY: notification parsing proved UTF-8 for this complete field.
        unsafe { str::from_utf8_unchecked(bytes) }
    }

    pub(crate) fn retained_len(&self) -> usize {
        self.len as usize
    }

    pub(super) fn try_compact(self) -> Result<buffer::raw::Text<'d>, copy::Error> {
        let owner = match NonZero::new(self.len) {
            None => resident::Exact::new(),
            Some(capacity) => {
                let slices: [&[u8]; 2] = array::from_fn(|index| self.field(index).as_bytes());
                // SAFETY: the stored capacity is the exact sum of these validated spans.
                let slices = unsafe { buffer::raw::Slices::new(capacity, slices) };
                slices.try_copy(self.source.budget())?
            }
        };
        // SAFETY: concatenating the complete admitted UTF-8 fields preserves UTF-8.
        Ok(unsafe { buffer::raw::utf8(owner) })
    }
}

const _: () = assert!(
    size_of::<Fields<'static, 'static, buffer::storage::Shared>>() == 7 * size_of::<usize>()
);

pub(super) fn group_fields(payload: &[u8], record: group::Record) -> server::Fields<'_> {
    // SAFETY: group admission reserves the aggregate owner; record publication
    // follows the complete copy of one parsed diagnostic into this exact range.
    let payload = unsafe { payload.get_unchecked(record.start as usize..record.end as usize) };
    // SAFETY: the immutable ledger retains the complete validated source bytes.
    unsafe { server::Fields::from_valid(payload) }
}

/// Replays unchanged numeric bytes retained by Number, with every digit and
/// multiply/add already checked by parsing.
pub(super) fn number(value: server::Number<'_>) -> NonZero<u32> {
    let mut number = 0u32;
    for byte in value.bytes() {
        number = number * 10 + u32::from(byte - b'0');
    }
    // SAFETY: numeric admission rejects empty fields and a zero result.
    unsafe { NonZero::new_unchecked(number) }
}
