use crate::protocol::{Offset, Reason, Text};

#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum Class {
    Permanent,
    Transient,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Code<'a>(&'a str);

impl<'a> Code<'a> {
    pub(super) const fn from_valid(value: &'a str) -> Self {
        Self(value)
    }

    pub(crate) fn try_new(value: Text<'a>) -> Result<Self, Reason> {
        let mut bytes = value.bytes();
        let actual = bytes.length();
        while let Some((byte, tail)) = bytes.split_first() {
            if !byte.is_ascii_uppercase() && !byte.is_ascii_digit() {
                return Err(Reason::DiagnosticSqlstateByte {
                    actual,
                    offset: Offset::from_u32(actual.get() - bytes.length().get()),
                    byte,
                });
            }
            bytes = tail;
        }
        if actual.get() != 5 {
            return Err(Reason::DiagnosticSqlstateLength { actual });
        }
        Ok(Self(value.as_str()))
    }

    pub const fn as_str(&self) -> &'a str {
        self.0
    }

    pub(crate) fn class(self) -> Class {
        match self.0.as_bytes() {
            [b'0', b'8', ..] | [b'5', b'3' | b'7' | b'8', ..] => Class::Transient,
            _ => Class::Permanent,
        }
    }
}

const _: () = assert!(std::mem::size_of::<Class>() == std::mem::size_of::<bool>());
const _: () = assert!(std::mem::align_of::<Class>() == std::mem::align_of::<bool>());
