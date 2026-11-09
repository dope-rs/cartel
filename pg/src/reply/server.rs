use crate::{
    protocol,
    reply::{self, owned},
};
use o3::{
    buffer::{resident, storage},
    mem::rc,
};
use protocol::{Length, Offset, Wire};
use std::{
    mem::{self, MaybeUninit},
    str,
};

pub struct Diagnostic<'d> {
    pub(crate) payload: resident::Shared<'d>,
}

impl<'d> Diagnostic<'d> {
    pub(crate) fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self {
            payload: self.payload.try_clone()?,
        })
    }

    pub fn fields(&self) -> Fields<'_> {
        // SAFETY: Diagnostic is constructed only from the parsed original bytes.
        unsafe { Fields::from_valid(self.payload.as_slice()) }
    }

    pub fn try_own(self) -> Result<owned::Server, (Self, storage::AllocError)> {
        let Self { payload } = self;
        match payload.try_owned() {
            Ok(payload) => Ok(owned::Server::new(payload)),
            Err(payload) => match storage::Shared::try_copy(payload.as_slice()) {
                Ok(payload) => Ok(owned::Server::new(payload)),
                Err(error) => Err((Self { payload }, error)),
            },
        }
    }
}

const _: () =
    assert!(mem::size_of::<Diagnostic<'static>>() == mem::size_of::<resident::Shared<'static>>());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fields<'a> {
    severity: &'a str,
    code: reply::Code<'a>,
    message: &'a str,
    context: reply::Context<'a>,
}

/// A numeric field selected from an unchanged, admitted diagnostic.
pub(super) struct Number<'a>(&'a str);

impl<'a> Number<'a> {
    pub(super) fn bytes(self) -> str::Bytes<'a> {
        self.0.bytes()
    }
}

impl<'a> Fields<'a> {
    pub fn from_owned(server: &'a owned::Server) -> Self {
        // SAFETY: ownership transfer preserves the original parsed diagnostic.
        unsafe { Self::from_valid(server.payload.as_slice()) }
    }

    /// The complete source must retain a successfully parsed diagnostic.
    pub(super) unsafe fn from_valid(payload: &'a [u8]) -> Self {
        let mut build = Build {
            severity: MaybeUninit::uninit(),
            severity_nonlocalized: None,
            code: MaybeUninit::uninit(),
            message: MaybeUninit::uninit(),
            context: reply::Context::default(),
        };
        let mut cursor = payload;
        loop {
            let kind = unsafe { *cursor.as_ptr() };
            if kind == 0 {
                break;
            }
            let tail = unsafe { cursor.get_unchecked(1..) };
            let mut end = 0;
            while unsafe { *tail.as_ptr().add(end) } != 0 {
                end += 1;
            }
            let value = unsafe { str::from_utf8_unchecked(tail.get_unchecked(..end)) };
            if let Some(field) = Field::new(kind) {
                if field == Field::Severity {
                    build.severity.write(value);
                } else if field == Field::NonlocalizedSeverity {
                    build.severity_nonlocalized = Some(value);
                } else if field == Field::Code {
                    build.code.write(value);
                } else if field == Field::Message {
                    build.message.write(value);
                } else if let Some(number) = build.context.field(field, value) {
                    *number = Some(reply::raw::number(Number(value)));
                }
            }
            cursor = unsafe { tail.get_unchecked(end + 1..) };
        }
        Self {
            severity: match build.severity_nonlocalized {
                Some(value) => value,
                None => unsafe { build.severity.assume_init() },
            },
            code: reply::Code::from_valid(unsafe { build.code.assume_init() }),
            message: unsafe { build.message.assume_init() },
            context: build.context,
        }
    }

    pub(crate) fn try_classify(
        payload: Wire<'a>,
    ) -> Result<(reply::Scope, reply::Class), protocol::Reason> {
        let mut severity = None;
        let mut code = None;
        let mut seen = 0u32;
        let mut cursor = payload;
        let length = payload.length().get();
        loop {
            let Some((kind, tail)) = cursor.split_first() else {
                return Err(protocol::Reason::DiagnosticResponseTerminator {
                    offset: Offset::from_u32(length - cursor.length().get()),
                });
            };
            if kind == 0 {
                if !tail.is_empty() {
                    return Err(protocol::Reason::DiagnosticTrailing {
                        offset: Offset::from_u32(length - tail.length().get()),
                        actual: tail.length(),
                    });
                }
                break;
            }
            let Some((value, rest)) = tail.split_byte(0) else {
                return Err(protocol::Reason::DiagnosticTerminator {
                    kind,
                    offset: Offset::from_u32(length - tail.length().get()),
                    actual: tail.length(),
                });
            };
            let value = match value.try_text() {
                Ok(value) => value,
                Err(error) => {
                    let offset = length - (tail.length().get() - error.valid.get());
                    return Err(protocol::Reason::DiagnosticUtf8 {
                        kind,
                        offset: Offset::from_u32(offset),
                        error: error.error,
                    });
                }
            };
            if let Some(field) = Field::new(kind) {
                let mask = field.mask();
                if seen & mask != 0 {
                    return Err(protocol::Reason::DuplicateDiagnostic { kind });
                }
                seen |= mask;
                if field.selects_severity(seen & Field::NonlocalizedSeverity.mask() != 0) {
                    severity = Some(value.as_str());
                } else {
                    match field {
                        Field::Code => code = Some(value),
                        Field::Position | Field::InnerPosition | Field::Line => {
                            reply::context::try_number(kind, value)?;
                        }
                        _ => {}
                    }
                }
            }
            cursor = rest;
        }
        let Some(severity) = severity.filter(|value| !value.is_empty()) else {
            return Err(protocol::Reason::DiagnosticSeverity);
        };
        let Some(code) = code else {
            return Err(protocol::Reason::DiagnosticSqlstateLength {
                actual: Length::from_u32(0),
            });
        };
        let code = reply::Code::try_new(code)?;
        if seen & Field::Message.mask() == 0 {
            return Err(protocol::Reason::DiagnosticMessage);
        }
        let scope = match severity {
            "ERROR" => reply::Scope::Command,
            "FATAL" | "PANIC" => reply::Scope::Connection,
            _ => reply::Scope::Unknown,
        };
        Ok((scope, code.class()))
    }

    pub const fn severity(self) -> &'a str {
        self.severity
    }

    pub const fn code(self) -> reply::Code<'a> {
        self.code
    }

    pub const fn message(self) -> &'a str {
        self.message
    }

    pub const fn context(self) -> reply::Context<'a> {
        self.context
    }
}

struct Build<'a> {
    severity: MaybeUninit<&'a str>,
    severity_nonlocalized: Option<&'a str>,
    code: MaybeUninit<&'a str>,
    message: MaybeUninit<&'a str>,
    context: reply::Context<'a>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Field {
    Severity,
    NonlocalizedSeverity,
    Code,
    Message,
    Detail,
    Hint,
    Position,
    InnerPosition,
    InnerQuery,
    Location,
    Schema,
    Table,
    Column,
    DataType,
    Constraint,
    File,
    Line,
    Routine,
}

impl Field {
    #[inline]
    fn new(kind: u8) -> Option<Self> {
        Some(match kind {
            b'S' => Self::Severity,
            b'V' => Self::NonlocalizedSeverity,
            b'C' => Self::Code,
            b'M' => Self::Message,
            b'D' => Self::Detail,
            b'H' => Self::Hint,
            b'P' => Self::Position,
            b'p' => Self::InnerPosition,
            b'q' => Self::InnerQuery,
            b'W' => Self::Location,
            b's' => Self::Schema,
            b't' => Self::Table,
            b'c' => Self::Column,
            b'd' => Self::DataType,
            b'n' => Self::Constraint,
            b'F' => Self::File,
            b'L' => Self::Line,
            b'R' => Self::Routine,
            _ => return None,
        })
    }

    const fn mask(self) -> u32 {
        1 << self as u32
    }

    fn selects_severity(self, nonlocalized: bool) -> bool {
        self == Self::NonlocalizedSeverity || self == Self::Severity && !nonlocalized
    }
}
