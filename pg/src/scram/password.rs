use o3::alloc::VecExt as _;
use std::{mem, ptr};
use stringprep::tables;
use unicode_normalization::char::{canonical_combining_class, compose, decompose_compatible};
use zeroize::Zeroizing;

mod bidi;

pub(crate) struct Password(Zeroizing<String>);

struct Decomposed(Zeroizing<Vec<char>>);

struct Composed(Zeroizing<Vec<char>>);

impl Password {
    pub(crate) fn new(password: String) -> Self {
        Self(Zeroizing::new(password))
    }

    pub(crate) fn try_prepare(mut self) -> Result<Self, crate::ConfigError> {
        if self
            .0
            .chars()
            .all(|ch| ch.is_ascii() && !tables::ascii_control_character(ch))
        {
            return Ok(self);
        }
        prepare(&mut self.0)?;
        Ok(self)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

fn prepare(value: &mut Zeroizing<String>) -> Result<(), crate::ConfigError> {
    let mut chars = mapped(value);
    let Some(first) = chars.next() else {
        return Ok(());
    };
    let mut last_right = false;
    let mut right = false;
    let mut left = false;
    let mut length = Some(0usize);
    let mut start = 0;
    let mut last = 0;
    let mut disordered = false;
    let mut scratch = 0;
    for ch in std::iter::once(first).chain(chars) {
        if is_prohibited(ch) || tables::unassigned_code_point(ch) {
            return Ok(());
        }
        last_right = bidi::right(ch);
        right |= last_right;
        left |= bidi::left(ch);
        decompose_compatible(ch, |ch| {
            length = length.and_then(|len| {
                let class = canonical_combining_class(ch);
                if class == 0 {
                    if disordered {
                        scratch = scratch.max(len - start);
                    }
                    start = len;
                    disordered = false;
                } else {
                    disordered |= last > class;
                }
                last = class;
                len.checked_add(1)
            });
        });
    }
    if right && (left || !bidi::right(first) || !last_right) {
        return Ok(());
    }
    let length = length.ok_or(crate::ConfigError::PasswordCapacity)?;
    if disordered {
        scratch = scratch.max(length - start);
    }
    let mut source = Decomposed(Zeroizing::new(
        Vec::try_new(length.saturating_add(scratch)).map_err(crate::ConfigError::Allocation)?,
    ));
    for mapped in mapped(value) {
        decompose_compatible(mapped, |ch| {
            let output = &mut source.0;
            let len = output.len();
            // SAFETY: replaying this unchanged source emits exactly the admitted
            // number of characters, preserving the admitted scratch suffix.
            unsafe {
                output.as_mut_ptr().add(len).write(ch);
                output.set_len(len + 1);
            }
        });
    }
    encode(&compose_all(source), value)
}

fn mapped(value: &str) -> impl Iterator<Item = char> + '_ {
    value
        .chars()
        .map(|ch| {
            if tables::non_ascii_space_character(ch) {
                ' '
            } else {
                ch
            }
        })
        .filter(|ch| !tables::commonly_mapped_to_nothing(*ch))
}

fn compose_all(mut input: Decomposed) -> Composed {
    let len = input.0.len();
    let ordered_all = input.0.capacity() == len;
    let values = input.0.as_mut_ptr();
    // SAFETY: decomposition initialized len characters and retained enough spare
    // slots for every disordered segment. Exact capacity == len proves none exist.
    let scratch = unsafe { values.add(len) };
    let mut written = 0;
    let mut starter: Option<*mut char> = None;
    let mut previous = 0;
    let mut start = 0;
    while start < len {
        let mut end = if ordered_all { len } else { start };
        let mut last = 0;
        let mut ordered = true;
        while end < len {
            // SAFETY: compaction never writes beyond the already consumed prefix.
            let class = canonical_combining_class(unsafe { values.add(end).read() });
            if class == 0 && end != start {
                break;
            }
            ordered &= last <= class;
            last = class;
            end += 1;
        }
        let source = if ordered {
            // SAFETY: this complete canonical segment remains initialized in place.
            unsafe { values.add(start) }
        } else {
            let mut positions = Zeroizing::new([0usize; 256]);
            for index in start..end {
                // SAFETY: the complete unconsumed segment lies in the initialized prefix.
                let ch = unsafe { values.add(index).read() };
                positions[canonical_combining_class(ch) as usize] += 1;
            }
            let mut offset = 0;
            for count in positions.iter_mut() {
                let next = offset + *count;
                *count = offset;
                offset = next;
            }
            for index in start..end {
                // SAFETY: the source and scratch ranges are disjoint; source-order
                // scatter fills each counted position exactly once, preserving ties.
                let ch = unsafe { values.add(index).read() };
                let at = &mut positions[canonical_combining_class(ch) as usize];
                unsafe { scratch.add(*at).write(ch) };
                *at += 1;
            }
            scratch
        };
        for index in 0..end - start {
            // SAFETY: the selected segment is fully initialized. Each read is copied
            // before writing at most one earlier slot; future source stays untouched.
            let ch = unsafe { source.add(index).read() };
            let class = canonical_combining_class(ch);
            if let Some(current) = starter
                && (previous == 0 || previous < class)
                && let Some(joined) = compose(unsafe { current.read() }, ch)
            {
                unsafe { current.write(joined) };
                continue;
            }
            let current = unsafe { values.add(written) };
            unsafe { current.write(ch) };
            if class == 0 {
                starter = Some(current);
            }
            previous = class;
            written += 1;
        }
        start = end;
    }
    // Zeroizing clears the full capacity, including scratch and the truncated tail.
    input.0.truncate(written);
    Composed(input.0)
}

fn is_prohibited(ch: char) -> bool {
    tables::ascii_control_character(ch)
        || tables::non_ascii_control_character(ch)
        || tables::private_use(ch)
        || tables::non_character_code_point(ch)
        || tables::surrogate_code(ch)
        || tables::inappropriate_for_plain_text(ch)
        || tables::inappropriate_for_canonical_representation(ch)
        || tables::change_display_properties_or_deprecated(ch)
        || tables::tagging_character(ch)
}

fn encode(value: &Composed, original: &mut Zeroizing<String>) -> Result<(), crate::ConfigError> {
    // The admitted char allocation bounds this unchanged output by four bytes per char.
    let bytes = value.0.iter().map(|ch| ch.len_utf8()).sum();
    let mut output = if original.capacity() == bytes {
        // Every original and spare byte is overwritten before this allocation is returned.
        mem::take(&mut **original).into_bytes()
    } else {
        Vec::try_new(bytes).map_err(crate::ConfigError::Allocation)?
    };
    let mut cursor = output.as_mut_ptr();
    let mut encoded = Zeroizing::new([0; 4]);
    for ch in value.0.iter() {
        let encoded = ch.encode_utf8(&mut encoded[..]);
        // SAFETY: this unchanged source was measured above; every encoded
        // fragment fits the exact output allocation and is disjoint from it.
        unsafe {
            ptr::copy_nonoverlapping(encoded.as_ptr(), cursor, encoded.len());
            cursor = cursor.add(encoded.len());
        }
    }
    // SAFETY: the complete measured prefix was initialized solely by encode_utf8.
    unsafe {
        output.set_len(bytes);
        *original = Zeroizing::new(String::from_utf8_unchecked(output));
    }
    Ok(())
}
