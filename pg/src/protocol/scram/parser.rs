use crate::protocol::{Length, Number, Offset, Reason, Wire};
use crate::scram;
use o3::buffer::{
    bytes::{bounded, scope},
    storage,
};
use std::{marker::PhantomData, num, ops};

struct Attribute<'a> {
    name: u8,
    value: Wire<'a>,
    start: u32,
}

impl Attribute<'_> {
    fn offset(&self) -> Offset {
        Offset::from_u32(self.start - 2)
    }
}

struct Attributes<'a> {
    tail: Option<Wire<'a>>,
    length: Length,
}

pub(super) struct Parser<'a> {
    bytes: Wire<'a>,
}

impl<'a> Parser<'a> {
    pub(super) const fn new(bytes: Wire<'a>) -> Self {
        Self { bytes }
    }

    pub(super) fn server_first(self) -> Result<ServerFirst<'a>, Reason> {
        let mut attrs = Attributes::new(self.bytes);
        let nonce = attrs.required(b'r')?;
        if let Some((relative, byte)) = (0u32..)
            .zip(nonce.value.as_slice().iter().copied())
            .find(|(_, byte)| !is_nonce_char(*byte))
        {
            return Err(Reason::NonceChar {
                offset: wire_offset(nonce.start, relative),
                byte,
            });
        }
        let salt = attrs.required(b's')?;
        let count = attrs.required(b'i')?;
        let count =
            positive_u32(count.value, count.start).map_err(|error| Reason::Iteration { error })?;
        attrs.extensions()?;
        Ok(ServerFirst {
            input: self.bytes,
            nonce: nonce.start..nonce.start + nonce.value.length().get(),
            salt: salt.start..salt.start + salt.value.length().get(),
            iterations: count,
        })
    }

    pub(super) fn server_final(self) -> Result<ServerFinal<'a>, Reason> {
        let mut attrs = Attributes::new(self.bytes);
        let first = attrs.next()?.ok_or(Reason::MissingResult)?;
        if first.name == b'm' {
            return Err(Reason::Extension {
                offset: first.offset(),
            });
        }
        let value = first.value;
        let result = match first.name {
            b'e' => {
                if let Some((relative, byte)) = (0u32..)
                    .zip(value.as_slice().iter().copied())
                    .find(|(_, byte)| !is_value_char(*byte))
                {
                    return Err(Reason::FinalValue {
                        offset: wire_offset(first.start, relative),
                        byte,
                    });
                }
                ServerFinal::Error(parse_server_error(value.as_slice()))
            }
            b'v' => ServerFinal::Verifier(value),
            actual => return Err(Reason::FinalAttribute { actual }),
        };
        attrs.extensions()?;
        Ok(result)
    }
}

impl<'a> Attributes<'a> {
    const fn new(bytes: Wire<'a>) -> Self {
        Self {
            tail: Some(bytes),
            length: bytes.length(),
        }
    }

    fn next(&mut self) -> Result<Option<Attribute<'a>>, Reason> {
        let Some(bytes) = self.tail.take() else {
            return Ok(None);
        };
        if bytes.is_empty() {
            return Ok(None);
        }
        let start = self.length.get() - bytes.length().get();
        let (head, body) = match bytes.split_array::<2>() {
            Some(parts) if !parts.1.is_empty() => parts,
            _ => {
                return Err(Reason::AttributeTruncated {
                    offset: Offset::from_u32(start),
                    actual: bytes.length(),
                });
            }
        };
        let [name, equals] = *head;
        if !name.is_ascii_alphabetic() {
            return Err(Reason::AttributeName {
                offset: Offset::from_u32(start),
                actual: name,
            });
        }
        if equals != b'=' {
            return Err(Reason::AttributeEquals {
                offset: Offset::from_u32(start + 1),
                actual: equals,
            });
        }
        let begin = self.length.get() - body.length().get();
        let (value, tail) = match body.split_byte(b',') {
            Some((value, tail)) => (value, Some(tail)),
            None => (body, None),
        };
        if value.is_empty() {
            return Err(Reason::EmptyAttribute {
                name,
                offset: Offset::from_u32(begin),
            });
        }
        if tail.is_some_and(Wire::is_empty) {
            return Err(Reason::TrailingComma {
                offset: Offset::from_u32(self.length.get() - 1),
            });
        }
        self.tail = tail;
        Ok(Some(Attribute {
            name,
            value,
            start: begin,
        }))
    }

    fn required(&mut self, name: u8) -> Result<Attribute<'a>, Reason> {
        let attr = self
            .next()?
            .ok_or(Reason::MissingAttribute { expected: name })?;
        if attr.name == b'm' {
            return Err(Reason::Extension {
                offset: attr.offset(),
            });
        }
        if attr.name != name {
            return Err(Reason::AttributeOrder {
                actual: attr.name,
                expected: name,
            });
        }
        Ok(attr)
    }

    fn extensions(&mut self) -> Result<(), Reason> {
        let mut names = Names::default();
        while let Some(attr) = self.next()? {
            if attr.name == b'm' {
                return Err(Reason::Extension {
                    offset: attr.offset(),
                });
            }
            if is_scram_attribute(attr.name) {
                return Err(Reason::StandardExtension {
                    name: attr.name,
                    offset: attr.offset(),
                });
            }
            if names.contains(attr.name) {
                return Err(Reason::DuplicateAttribute {
                    name: attr.name,
                    offset: attr.offset(),
                });
            }
            names.insert(attr.name);
            if let Some((relative, byte)) = (0u32..)
                .zip(attr.value.as_slice().iter().copied())
                .find(|(_, byte)| !is_value_char(*byte))
            {
                return Err(Reason::ExtensionValue {
                    name: attr.name,
                    offset: wire_offset(attr.start, relative),
                    byte,
                });
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Names(u64);

impl Names {
    fn contains(&self, name: u8) -> bool {
        self.0 & Self::bit(name) != 0
    }

    fn insert(&mut self, name: u8) {
        self.0 |= Self::bit(name);
    }

    fn bit(name: u8) -> u64 {
        let index = if name.is_ascii_lowercase() {
            name - b'a'
        } else {
            name - b'A' + 26
        };
        1u64 << index
    }
}

pub(super) struct ServerFirst<'a> {
    input: Wire<'a>,
    nonce: ops::Range<u32>,
    salt: ops::Range<u32>,
    iterations: num::NonZero<u32>,
}

pub(super) struct Retained {
    bytes: storage::Shared,
    nonce: ops::Range<u32>,
}

struct Selection<'id> {
    bounds: ops::Range<u32>,
    brand: PhantomData<fn(&'id mut ()) -> &'id mut ()>,
}

impl Selection<'_> {
    fn new(bounds: ops::Range<u32>) -> Self {
        Self {
            bounds,
            brand: PhantomData,
        }
    }
}

// SAFETY: only the methods below mint selections, inside the scope of their
// parsed or retained owner. Attribute parsing derives each ordered range from
// that exact Wire's immutable suffixes; its u32 extent bounds every endpoint.
// Retention copies the complete original Wire before transferring its nonce
// range. Neither owner exposes mutation or accepts an independently supplied
// range, and the invariant brand confines each selection to that owner's scope.
unsafe impl<'id> scope::raw::Select<'id> for Selection<'id> {
    fn bounds(self) -> ops::Range<usize> {
        self.bounds.start as usize..self.bounds.end as usize
    }
}

impl<'a> ServerFirst<'a> {
    pub(super) fn with_parts<R>(
        self,
        run: impl FnOnce(Self, Wire<'a>, Wire<'a>, num::NonZero<u32>) -> R,
    ) -> R {
        scope::Owner::with(self.input.as_slice(), |owner| {
            let view = owner.borrow();
            let nonce = view.get(Selection::new(self.nonce.clone())).0;
            let salt = view.get(Selection::new(self.salt.clone())).0;
            // SAFETY: both projections retain the same parsed Wire's u32 bound.
            let nonce = Wire::from_raw(unsafe { bounded::raw::Input::new(nonce) });
            // SAFETY: the salt is another projection of that exact bounded Wire.
            let salt = Wire::from_raw(unsafe { bounded::raw::Input::new(salt) });
            let count = self.iterations;
            run(self, nonce, salt, count)
        })
    }

    pub(super) fn try_retain(self) -> Result<Retained, crate::Error> {
        let bytes = storage::Shared::try_copy(self.input.as_slice()).map_err(crate::Error::from)?;
        Ok(Retained {
            bytes,
            nonce: self.nonce,
        })
    }
}

impl Retained {
    pub(super) fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub(super) fn nonce(&self) -> &[u8] {
        scope::Owner::with(&self.bytes, |owner| {
            owner.borrow().get(Selection::new(self.nonce.clone())).0
        })
    }
}

pub(super) enum ServerFinal<'a> {
    Error(scram::Error),
    Verifier(Wire<'a>),
}

fn parse_server_error(value: &[u8]) -> scram::Error {
    match value {
        b"invalid-encoding" => scram::Error::InvalidEncoding,
        b"extensions-not-supported" => scram::Error::ExtensionsNotSupported,
        b"invalid-proof" => scram::Error::InvalidProof,
        b"channel-bindings-dont-match" => scram::Error::ChannelBindingsDontMatch,
        b"server-does-support-channel-binding" => scram::Error::ServerDoesSupportChannelBinding,
        b"channel-binding-not-supported" => scram::Error::ChannelBindingNotSupported,
        b"unsupported-channel-binding-type" => scram::Error::UnsupportedChannelBindingType,
        b"unknown-user" => scram::Error::UnknownUser,
        b"invalid-username-encoding" => scram::Error::InvalidUsernameEncoding,
        b"no-resources" => scram::Error::NoResources,
        _ => scram::Error::Other,
    }
}

const fn is_nonce_char(byte: u8) -> bool {
    matches!(byte, 0x21..=0x2b | 0x2d..=0x7e)
}

const fn is_value_char(byte: u8) -> bool {
    matches!(byte, 0x01..=0x2b | 0x2d..=0x3c | 0x3e..=0x7f)
}

const fn is_scram_attribute(name: u8) -> bool {
    matches!(
        name,
        b'a' | b'c' | b'e' | b'i' | b'm' | b'n' | b'p' | b'r' | b's' | b'v'
    )
}

fn positive_u32(bytes: Wire<'_>, start: u32) -> Result<num::NonZero<u32>, Number> {
    let Some((first, _)) = bytes.split_first() else {
        return Err(Number::Empty);
    };
    if first == b'0' {
        return if bytes.len() == 1 {
            Err(Number::Zero)
        } else {
            Err(Number::LeadingZero {
                offset: Offset::from_u32(start),
            })
        };
    }
    let mut value = 0u32;
    for (relative, byte) in (0u32..).zip(bytes.as_slice().iter().copied()) {
        if !byte.is_ascii_digit() {
            return Err(Number::Invalid {
                offset: wire_offset(start, relative),
                byte,
            });
        }
        let Some(next) = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u32::from(byte - b'0')))
        else {
            return Err(Number::Overflow {
                offset: wire_offset(start, relative),
                byte,
            });
        };
        value = next;
    }
    num::NonZero::<u32>::new(value).ok_or(Number::Zero)
}

fn wire_offset(position: u32, relative: u32) -> Offset {
    Offset::from_u32(position + relative)
}

const _: () = {
    assert!(size_of::<Attributes<'static>>() == 3 * size_of::<usize>());
    assert!(size_of::<Attribute<'static>>() == 3 * size_of::<usize>());
    assert!(size_of::<ServerFirst<'static>>() == 5 * size_of::<usize>());
    assert!(size_of::<Retained>() == 4 * size_of::<usize>());
};
