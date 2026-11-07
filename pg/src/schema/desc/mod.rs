use crate::protocol::{Reason, encode};
use crate::schema::{self, result};
use std::{mem, num};
mod formats;
mod oids;
mod registry;

#[doc(hidden)]
pub use formats::Formats;
use oids::*;
#[doc(hidden)]
pub use registry::Registry;
pub(crate) const DYNAMIC_LTREE: u8 = 1;

#[doc(hidden)]
pub struct Input<'a> {
    pub payload: &'a [u8],
    pub registry: Registry,
}

const _: () = assert!(mem::size_of::<Input<'_>>() <= 5 * mem::size_of::<usize>());

enum Target {
    Exact(u32),
    String,
    StringArray,
    Ltree,
}

#[doc(hidden)]
pub struct Types<'a> {
    bytes: &'a [u8],
    expected: u16,
    index: u16,
    registry: Registry,
}

impl<'a> Types<'a> {
    pub(crate) fn new(
        payload: &'a [u8],
        expected: u16,
        registry: Registry,
    ) -> Result<Self, crate::Error> {
        let [count0, count1, bytes @ ..] = payload else {
            return Err(crate::Error::DescriptionRowCountTruncated {
                actual: payload.len(),
            });
        };
        let signed = i16::from_be_bytes([*count0, *count1]);
        if signed < 0 {
            return Err(crate::Error::DescriptionRowCountNegative { actual: signed });
        }
        let count = signed as u16;
        if count != expected {
            return Err(crate::Error::DescriptionColumnCount {
                actual: count,
                expected,
            });
        }
        Ok(Self {
            bytes,
            expected: count,
            index: 0,
            registry,
        })
    }

    fn next_oid(&mut self) -> Result<u32, crate::Error> {
        if self.index == self.expected {
            return Err(crate::Error::DescriptionDecoderOverflow {
                actual: usize::from(self.index) + 1,
                expected: self.expected,
            });
        }
        let Some(name_end) = self.bytes.iter().position(|byte| *byte == 0) else {
            return Err(crate::Error::DescriptionFieldName {
                actual: self.bytes.len(),
            });
        };
        let metadata = &self.bytes[name_end + 1..];
        let [
            _,
            _,
            _,
            _,
            _,
            _,
            oid0,
            oid1,
            oid2,
            oid3,
            _,
            _,
            _,
            _,
            _,
            _,
            format0,
            format1,
            tail @ ..,
        ] = metadata
        else {
            return Err(crate::Error::DescriptionFieldMetadata {
                actual: metadata.len(),
            });
        };
        let oid = u32::from_be_bytes([*oid0, *oid1, *oid2, *oid3]);
        let format = i16::from_be_bytes([*format0, *format1]);
        if format != 0 {
            return Err(crate::Error::DescriptionFieldFormat { actual: format });
        }
        self.bytes = tail;
        self.index += 1;
        Ok(oid)
    }

    fn match_type(&mut self, expected: Target) -> Result<(), crate::Error> {
        let actual = self.next_oid()?;
        match expected {
            Target::Exact(expected) if actual == expected => Ok(()),
            Target::Exact(expected) => Err(crate::Error::DescriptionOid { actual, expected }),
            Target::String if matches!(actual, NAME | TEXT | BPCHAR | VARCHAR) => Ok(()),
            Target::String => Err(crate::Error::DescriptionStringOid { actual }),
            Target::StringArray
                if matches!(
                    actual,
                    NAME_ARRAY | TEXT_ARRAY | BPCHAR_ARRAY | VARCHAR_ARRAY
                ) =>
            {
                Ok(())
            }
            Target::StringArray => Err(crate::Error::DescriptionStringArrayOid { actual }),
            Target::Ltree => match self.registry.ltree() {
                Some(expected) if expected.get() == actual => Ok(()),
                Some(expected) => Err(crate::Error::DescriptionLtreeOid {
                    actual,
                    expected: expected.get(),
                }),
                None => Err(crate::Error::Protocol(Reason::LtreeMissing)),
            },
        }
    }

    pub(crate) fn finish(self) -> Result<(), crate::Error> {
        if self.index != self.expected {
            return Err(crate::Error::DescriptionUnvalidated {
                actual: self.index,
                expected: self.expected,
            });
        }
        if !self.bytes.is_empty() {
            return Err(crate::Error::DescriptionRowTrailing {
                actual: self.bytes.len(),
            });
        }
        Ok(())
    }
}

const _: () = assert!(mem::size_of::<Types<'_>>() == mem::size_of::<(&[u8], [u16; 2], Registry)>());

#[doc(hidden)]
pub trait Validate {
    const DYNAMIC_TYPES: u8 = 0;
    const FORMAT: u16 = 1;

    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error>;

    #[doc(hidden)]
    fn formats(values: &mut Formats<'_>) -> Result<(), encode::Error> {
        values.take(Self::FORMAT)
    }
}

impl Validate for () {
    fn validate(_: &mut Types<'_>) -> Result<(), crate::Error> {
        Ok(())
    }

    fn formats(_: &mut Formats<'_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl<A: Validate, B: Validate> Validate for (A, B) {
    const DYNAMIC_TYPES: u8 = A::DYNAMIC_TYPES | B::DYNAMIC_TYPES;

    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        A::validate(types)?;
        B::validate(types)
    }

    fn formats(values: &mut Formats<'_>) -> Result<(), encode::Error> {
        A::formats(values)?;
        B::formats(values)
    }
}

impl<A: Validate, B: Validate, C: Validate> Validate for (A, B, C) {
    const DYNAMIC_TYPES: u8 = A::DYNAMIC_TYPES | B::DYNAMIC_TYPES | C::DYNAMIC_TYPES;

    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        A::validate(types)?;
        B::validate(types)?;
        C::validate(types)
    }

    fn formats(values: &mut Formats<'_>) -> Result<(), encode::Error> {
        A::formats(values)?;
        B::formats(values)?;
        C::formats(values)
    }
}

impl<A: Validate, B: Validate, C: Validate, D: Validate> Validate for (A, B, C, D) {
    const DYNAMIC_TYPES: u8 =
        A::DYNAMIC_TYPES | B::DYNAMIC_TYPES | C::DYNAMIC_TYPES | D::DYNAMIC_TYPES;

    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        A::validate(types)?;
        B::validate(types)?;
        C::validate(types)?;
        D::validate(types)
    }

    fn formats(values: &mut Formats<'_>) -> Result<(), encode::Error> {
        A::formats(values)?;
        B::formats(values)?;
        C::formats(values)?;
        D::formats(values)
    }
}

impl Validate for bool {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(BOOL))
    }
}

impl Validate for i16 {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(INT2))
    }
}

impl Validate for i32 {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(INT4))
    }
}

impl Validate for i64 {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(INT8))
    }
}

impl Validate for f32 {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(FLOAT4))
    }
}

impl Validate for f64 {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(FLOAT8))
    }
}

impl Validate for Vec<u8> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(BYTEA))
    }
}

impl Validate for crate::Uuid {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(UUID))
    }
}

impl Validate for crate::Timestamp {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(TIMESTAMP))
    }
}

impl Validate for crate::Date {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(DATE))
    }
}

impl Validate for crate::Jsonb {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(JSONB))
    }
}

impl Validate for Vec<i32> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(INT4_ARRAY))
    }
}

impl Validate for Vec<i64> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(INT8_ARRAY))
    }
}

impl Validate for String {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::String)
    }
}

impl Validate for crate::Text {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::String)
    }
}

impl Validate for Vec<String> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::StringArray)
    }
}

impl Validate for crate::Ltree {
    const DYNAMIC_TYPES: u8 = DYNAMIC_LTREE;
    const FORMAT: u16 = 0;

    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Ltree)
    }
}

impl Validate for Option<bool> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(BOOL))
    }
}

impl Validate for Option<i16> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(INT2))
    }
}

impl Validate for Option<i32> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(INT4))
    }
}

impl Validate for Option<i64> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(INT8))
    }
}

impl Validate for Option<f32> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(FLOAT4))
    }
}

impl Validate for Option<f64> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(FLOAT8))
    }
}

impl Validate for Option<String> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::String)
    }
}

impl Validate for Option<crate::Text> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::String)
    }
}

impl Validate for Option<crate::Jsonb> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(JSONB))
    }
}

impl Validate for Option<crate::Ltree> {
    const DYNAMIC_TYPES: u8 = DYNAMIC_LTREE;
    const FORMAT: u16 = 0;

    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Ltree)
    }
}

impl Validate for Option<Vec<u8>> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(BYTEA))
    }
}

impl Validate for Option<crate::Uuid> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(UUID))
    }
}

impl Validate for Option<crate::Timestamp> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(TIMESTAMP))
    }
}

impl Validate for Option<crate::Date> {
    fn validate(types: &mut Types<'_>) -> Result<(), crate::Error> {
        types.match_type(Target::Exact(DATE))
    }
}

impl Types<'_> {
    pub(crate) fn parameters(payload: &[u8], expected: &[u32]) -> Result<(), crate::Error> {
        let [count0, count1, tail @ ..] = payload else {
            return Err(crate::Error::DescriptionParameterCountTruncated {
                actual: payload.len(),
            });
        };
        let signed = i16::from_be_bytes([*count0, *count1]);
        if signed < 0 {
            return Err(crate::Error::DescriptionParameterCountNegative { actual: signed });
        }
        let count = signed as u16;
        if usize::from(count) != expected.len() {
            return Err(crate::Error::DescriptionParameterCount {
                actual: count,
                expected: expected.len(),
            });
        }
        let mut bytes = tail;
        for (index, want) in expected.iter().enumerate() {
            let [oid0, oid1, oid2, oid3, tail @ ..] = bytes else {
                return Err(crate::Error::DescriptionParameterOidTruncated {
                    index,
                    actual: bytes.len(),
                });
            };
            let actual = u32::from_be_bytes([*oid0, *oid1, *oid2, *oid3]);
            if actual != *want {
                return Err(crate::Error::DescriptionParameterOid {
                    index,
                    actual,
                    expected: *want,
                });
            }
            bytes = tail;
        }
        if !bytes.is_empty() {
            return Err(crate::Error::DescriptionParameterTrailing {
                actual: bytes.len(),
            });
        }
        Ok(())
    }

    pub(crate) fn ltree_oid(payload: &[u8]) -> Result<num::NonZero<u32>, crate::Error> {
        let mut types = Types::new(payload, 1, Registry::default())?;
        let oid = types.next_oid()?;
        types.finish()?;
        num::NonZero::new(oid).ok_or(crate::Error::DescriptionLtreeOidZero)
    }

    #[doc(hidden)]
    pub fn query<Q: schema::TypedQuery>(
        payload: &[u8],
        registry: Registry,
    ) -> Result<(), crate::Error> {
        if !matches!(<Q::Result as result::Shape>::KIND, result::Kind::Rows) {
            return Err(crate::Error::DescriptionUnexpectedRows);
        }
        let mut types = Types::new(payload, Q::N_RESULT_COLS, registry)?;
        <Q::Row as Validate>::validate(&mut types)?;
        types.finish()
    }
}
