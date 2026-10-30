use o3::alloc::{self, VecExt as _, array};
use std::ptr;
use stringprep::tables;
use unicode_normalization::char::{canonical_combining_class, compose, decompose_compatible};
use zeroize::Zeroizing;

pub(crate) struct Password(Zeroizing<String>);

struct Decomposed(Zeroizing<Vec<(u8, char)>>);

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
        if let Some(value) = prepare(&self.0)? {
            self.0 = Zeroizing::new(value);
        }
        Ok(self)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

fn prepare(value: &str) -> Result<Option<String>, crate::ConfigError> {
    let mut chars = mapped(value);
    let Some(first) = chars.next() else {
        return Ok(None);
    };
    let first_right = tables::bidi_r_or_al(first);
    let mut last_right = first_right;
    let mut right = false;
    let mut left = false;
    let mut length = Some(0usize);
    for ch in std::iter::once(first).chain(chars) {
        if is_prohibited(ch) || tables::unassigned_code_point(ch) {
            return Ok(None);
        }
        last_right = tables::bidi_r_or_al(ch);
        right |= last_right;
        left |= tables::bidi_l(ch);
        decompose_compatible(ch, |_| length = length.and_then(|len| len.checked_add(1)));
    }
    if right && (left || !first_right || !last_right) {
        return Ok(None);
    }
    let length = length.ok_or(crate::ConfigError::PasswordCapacity)?;
    let mut source = Decomposed(Zeroizing::new(
        Vec::try_new(length).map_err(crate::ConfigError::Allocation)?,
    ));
    for mapped in mapped(value) {
        decompose_compatible(mapped, |ch| {
            let class = canonical_combining_class(ch);
            let output = &mut source.0;
            let len = output.len();
            // SAFETY: replaying this unchanged source emits exactly the admitted
            // number of characters. The insertion position and shifted suffix
            // stay within that exclusive allocation, which never reallocates.
            unsafe {
                let at = if class == 0 {
                    output.as_mut_ptr().add(len)
                } else {
                    let offset = output
                        .iter()
                        .rposition(|value| value.0 <= class)
                        .map_or(0, |index| index + 1);
                    let at = output.as_mut_ptr().add(offset);
                    if offset < len {
                        ptr::copy(at, at.add(1), len - offset);
                    }
                    at
                };
                at.write((class, ch));
                output.set_len(len + 1);
            }
        });
    }
    encode(&compose_all(&source)?).map(Some)
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

fn compose_all(input: &Decomposed) -> Result<Composed, crate::ConfigError> {
    let mut output = Composed(Zeroizing::new(Vec::new()));
    output
        .0
        .try_grow(input.0.len())
        .map_err(crate::ConfigError::Allocation)?;
    let mut written = 0;
    {
        let mut source = input.0.iter();
        let mut starter: Option<&mut char> = None;
        let mut previous = 0;
        'slots: for slot in output.0.spare_capacity_mut() {
            let (class, ch) = loop {
                let Some(&(class, ch)) = source.next() else {
                    break 'slots;
                };
                if let Some(current) = starter.as_deref_mut()
                    && (previous == 0 || previous < class)
                    && let Some(joined) = compose(*current, ch)
                {
                    *current = joined;
                    continue;
                }
                break (class, ch);
            };
            let current = slot.write(ch);
            if class == 0 {
                starter = Some(current);
            }
            previous = class;
            written += 1;
        }
    }
    // SAFETY: disjoint spare slots initialize exactly this prefix. Capacity is
    // at least the input length, so every source character is consumed before
    // slots run out; composition can only reduce the number of written slots.
    unsafe { output.0.set_len(written) };
    Ok(output)
}

fn is_prohibited(ch: char) -> bool {
    tables::non_ascii_space_character(ch)
        || tables::ascii_control_character(ch)
        || tables::non_ascii_control_character(ch)
        || tables::private_use(ch)
        || tables::non_character_code_point(ch)
        || tables::surrogate_code(ch)
        || tables::inappropriate_for_plain_text(ch)
        || tables::inappropriate_for_canonical_representation(ch)
        || tables::change_display_properties_or_deprecated(ch)
        || tables::tagging_character(ch)
}

fn encode(value: &Composed) -> Result<String, crate::ConfigError> {
    let bytes = value.0.iter().try_fold(0usize, |total, ch| {
        total
            .checked_add(ch.len_utf8())
            .ok_or(crate::ConfigError::PasswordCapacity)
    })?;
    let mut output = array::Plan::try_new(bytes)
        .map_err(alloc::Error::from)
        .and_then(|plan| plan.try_vec().map_err(alloc::Error::from))
        .map_err(crate::ConfigError::Allocation)?;
    let mut cursor = output.as_mut_ptr();
    for ch in value.0.iter() {
        let mut encoded = Zeroizing::new([0; 4]);
        let encoded = ch.encode_utf8(&mut encoded[..]);
        // SAFETY: this unchanged source was measured above; every encoded
        // fragment fits the fresh exact allocation and is disjoint from it.
        unsafe {
            ptr::copy_nonoverlapping(encoded.as_ptr(), cursor, encoded.len());
            cursor = cursor.add(encoded.len());
        }
    }
    // SAFETY: the complete measured prefix was initialized solely by encode_utf8.
    unsafe {
        output.set_len(bytes);
        Ok(String::from_utf8_unchecked(output))
    }
}
