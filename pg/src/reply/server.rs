use crate::{
    protocol,
    reply::{self, owned},
};
use o3::{
    buffer::{resident, storage},
    mem::rc,
};
use protocol::{Length, Offset, Text, Wire};
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
        let mut build = Build::new();
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
            build.set(kind, value);
            cursor = unsafe { tail.get_unchecked(end + 1..) };
        }
        unsafe { build.finish() }
    }

    pub(crate) fn try_parse(payload: Wire<'a>) -> Result<Self, crate::Error> {
        let mut parts = Parts::default();
        let mut seen = 0u32;
        let mut cursor = payload;
        let length = payload.length().get();
        loop {
            let Some((kind, tail)) = cursor.split_first() else {
                return Err(crate::Error::Protocol(
                    protocol::Reason::DiagnosticResponseTerminator {
                        offset: Offset::from_u32(length - cursor.length().get()),
                    },
                ));
            };
            if kind == 0 {
                if !tail.is_empty() {
                    return Err(crate::Error::Protocol(
                        protocol::Reason::DiagnosticTrailing {
                            offset: Offset::from_u32(length - tail.length().get()),
                            actual: tail.length(),
                        },
                    ));
                }
                break;
            }
            let Some((value, rest)) = tail.split_byte(0) else {
                return Err(crate::Error::Protocol(
                    protocol::Reason::DiagnosticTerminator {
                        kind,
                        offset: Offset::from_u32(length - tail.length().get()),
                        actual: tail.length(),
                    },
                ));
            };
            let value = match value.try_text() {
                Ok(value) => value,
                Err(error) => {
                    let offset = length - (tail.length().get() - error.valid.get());
                    return Err(crate::Error::Protocol(protocol::Reason::DiagnosticUtf8 {
                        kind,
                        offset: Offset::from_u32(offset),
                        error: error.error,
                    }));
                }
            };
            if let Some(bit) = recognized_bit(kind) {
                let mask = 1u32 << bit;
                if seen & mask != 0 {
                    return Err(crate::Error::Protocol(
                        protocol::Reason::DuplicateDiagnostic { kind },
                    ));
                }
                seen |= mask;
            }
            parts.try_set(kind, value)?;
            cursor = rest;
        }
        parts.try_finish()
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

impl<'a> Build<'a> {
    fn new() -> Self {
        Self {
            severity: MaybeUninit::uninit(),
            severity_nonlocalized: None,
            code: MaybeUninit::uninit(),
            message: MaybeUninit::uninit(),
            context: reply::Context::default(),
        }
    }

    fn set(&mut self, kind: u8, value: &'a str) {
        if kind == b'S' {
            self.severity.write(value);
        } else if kind == b'V' {
            self.severity_nonlocalized = Some(value);
        } else if kind == b'C' {
            self.code.write(value);
        } else if kind == b'M' {
            self.message.write(value);
        } else if let Some(number) = self.context.write().field(kind, value) {
            *number = Some(reply::raw::number(Number(value)));
        }
    }

    unsafe fn finish(self) -> Fields<'a> {
        let severity = match self.severity_nonlocalized {
            Some(severity) => severity,
            None => unsafe { self.severity.assume_init() },
        };
        Fields {
            severity,
            code: reply::Code::from_valid(unsafe { self.code.assume_init() }),
            message: unsafe { self.message.assume_init() },
            context: self.context,
        }
    }
}

#[derive(Default)]
struct Parts<'a> {
    severity: Option<Text<'a>>,
    severity_nonlocalized: Option<Text<'a>>,
    code: Option<Text<'a>>,
    message: Option<Text<'a>>,
    context: reply::Context<'a>,
}

impl<'a> Parts<'a> {
    fn try_set(&mut self, kind: u8, value: Text<'a>) -> Result<(), crate::Error> {
        if kind == b'S' {
            self.severity = Some(value);
        } else if kind == b'V' {
            self.severity_nonlocalized = Some(value);
        } else if kind == b'C' {
            self.code = Some(value);
        } else if kind == b'M' {
            self.message = Some(value);
        } else {
            self.context.write().try_set(kind, value)?;
        }
        Ok(())
    }

    fn try_finish(self) -> Result<Fields<'a>, crate::Error> {
        let Some(severity) = self
            .severity_nonlocalized
            .or(self.severity)
            .filter(|value| !value.as_str().is_empty())
        else {
            return Err(crate::Error::Protocol(protocol::Reason::DiagnosticSeverity));
        };
        let Some(code) = self.code else {
            return Err(crate::Error::Protocol(
                protocol::Reason::DiagnosticSqlstateLength {
                    actual: Length::from_u32(0),
                },
            ));
        };
        let code = reply::Code::try_new(code).map_err(crate::Error::Protocol)?;
        let Some(message) = self.message else {
            return Err(crate::Error::Protocol(protocol::Reason::DiagnosticMessage));
        };
        Ok(Fields {
            severity: severity.as_str(),
            code,
            message: message.as_str(),
            context: self.context,
        })
    }
}

fn recognized_bit(kind: u8) -> Option<u8> {
    if kind == b'S' {
        Some(0)
    } else if kind == b'V' {
        Some(1)
    } else if kind == b'C' {
        Some(2)
    } else if kind == b'M' {
        Some(3)
    } else if kind == b'D' {
        Some(4)
    } else if kind == b'H' {
        Some(5)
    } else if kind == b'P' {
        Some(6)
    } else if kind == b'p' {
        Some(7)
    } else if kind == b'q' {
        Some(8)
    } else if kind == b'W' {
        Some(9)
    } else if kind == b's' {
        Some(10)
    } else if kind == b't' {
        Some(11)
    } else if kind == b'c' {
        Some(12)
    } else if kind == b'd' {
        Some(13)
    } else if kind == b'n' {
        Some(14)
    } else if kind == b'F' {
        Some(15)
    } else if kind == b'L' {
        Some(16)
    } else if kind == b'R' {
        Some(17)
    } else {
        None
    }
}
