use std::str;

use o3::buffer::bytes::{Prefix, bounded};

use crate::protocol::{Length, Reason, Text, TextError, Utf8};

/// Borrowed wire bytes whose original admission bounds every projection by u32.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Wire<'a>(Prefix<'a, { u32::MAX as usize }>);

impl<'a> Wire<'a> {
    pub(crate) const fn from_raw(
        value: bounded::raw::Input<&'a [u8], { u32::MAX as usize }>,
    ) -> Self {
        Self(Prefix::from_raw(value))
    }

    pub fn try_new(bytes: &'a [u8]) -> Result<Self, crate::Error> {
        Prefix::try_new(bytes).map(Self).map_err(|_| {
            crate::Error::Protocol(Reason::WireLength {
                actual: bytes.len(),
            })
        })
    }

    pub const fn as_slice(&self) -> &'a [u8] {
        self.0.as_slice()
    }

    pub const fn len(self) -> usize {
        self.0.as_slice().len()
    }

    pub const fn is_empty(self) -> bool {
        self.0.as_slice().is_empty()
    }

    pub const fn length(self) -> Length {
        Length::from_u32(self.0.len::<{ u32::MAX }>().get())
    }

    pub fn split_first(self) -> Option<(u8, Self)> {
        self.0.split_first().map(|(head, tail)| (*head, Self(tail)))
    }

    pub fn split_array<const N: usize>(self) -> Option<(&'a [u8; N], Self)> {
        self.0.split_array().map(|(head, tail)| (head, Self(tail)))
    }

    #[inline]
    pub fn split_at(self, mid: usize) -> Option<(Self, Self)> {
        self.0
            .split_at(mid)
            .map(|(head, tail)| (Self(head), Self(tail)))
    }

    pub fn split_byte(self, byte: u8) -> Option<(Self, Self)> {
        let at = self.as_slice().iter().position(|value| *value == byte)?;
        // SAFETY: the search selected this delimiter within the admitted source.
        let (head, tail) = unsafe {
            (
                self.as_slice().get_unchecked(..at),
                self.as_slice().get_unchecked(at + 1..),
            )
        };
        // SAFETY: both immutable projections retain the original upper bound.
        Some((
            Self::from_raw(unsafe { bounded::raw::Input::new(head) }),
            Self::from_raw(unsafe { bounded::raw::Input::new(tail) }),
        ))
    }

    pub fn try_text(self) -> Result<Text<'a>, TextError> {
        match str::from_utf8(self.as_slice()) {
            Ok(text) => Ok(Text(text)),
            Err(error) => {
                // UTF-8 validation reports a prefix within this u32-bounded source.
                let valid = Length::from_u32(error.valid_up_to() as u32);
                Err(TextError {
                    valid,
                    error: Utf8::from_error(error),
                })
            }
        }
    }
}

const _: () = assert!(size_of::<Wire<'static>>() == size_of::<&[u8]>());
