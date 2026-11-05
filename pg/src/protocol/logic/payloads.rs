use o3::base::cell::region;
use o3::buffer::{self, hold};

use crate::{
    protocol::{
        Length, Offset, Reason, Text, TextError, Wire, frame, messages,
        runtime::{notify, tx::poll},
        scram,
    },
    reply,
};

pub(super) fn ready_status(bytes: Wire<'_>) -> Result<poll::Status, crate::Error> {
    match bytes.as_slice() {
        [b'I'] => Ok(poll::Status::Idle),
        [b'T'] | [b'E'] => Ok(poll::Status::Transaction),
        _ => Err(crate::Error::Protocol(Reason::ReadyUnknown)),
    }
}

pub(super) fn empty(tag: u8, bytes: Wire<'_>) -> Result<(), crate::Error> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(crate::Error::Protocol(Reason::ControlPayload {
            tag,
            actual: bytes.length(),
        }))
    }
}

pub(super) fn command(tag: u8, bytes: Wire<'_>) -> Result<(), crate::Error> {
    let valid = bytes
        .as_slice()
        .split_last()
        .is_some_and(|(last, body)| *last == 0 && !body.is_empty() && !body.contains(&0));
    if valid {
        Ok(())
    } else {
        Err(crate::Error::Protocol(Reason::ControlPayload {
            tag,
            actual: bytes.length(),
        }))
    }
}

pub(super) fn copy_format(bytes: Wire<'_>) -> Result<(), crate::Error> {
    let [overall, count0, count1, formats @ ..] = bytes.as_slice() else {
        return Err(copy_error(bytes));
    };
    let count = i16::from_be_bytes([*count0, *count1]);
    if *overall > 1 || count < 0 || formats.len() != count as usize * 2 {
        return Err(copy_error(bytes));
    }
    if formats
        .as_chunks::<2>()
        .0
        .iter()
        .any(|pair| u16::from_be_bytes(*pair) > 1)
    {
        return Err(copy_error(bytes));
    }
    Ok(())
}

pub(super) fn parameter(bytes: Wire<'_>) -> Result<(), crate::Error> {
    let (name, rest) = cstr_bytes(bytes).map_err(|error| {
        crate::Error::Protocol(Reason::ParameterNameTerminator {
            actual: error.actual,
        })
    })?;
    if name.is_empty() {
        return Err(crate::Error::Protocol(Reason::ParameterNameEmpty));
    }
    let at = bytes.length().get() - rest.length().get();
    let (_, trailing) = cstr_bytes(rest).map_err(|error| {
        crate::Error::Protocol(Reason::ParameterValueTerminator {
            offset: Offset::from_u32(at),
            actual: error.actual,
        })
    })?;
    if !trailing.is_empty() {
        return Err(crate::Error::Protocol(Reason::ParameterTrailing {
            offset: Offset::from_u32(bytes.length().get() - trailing.length().get()),
            actual: trailing.length(),
        }));
    }
    Ok(())
}

fn copy_error(bytes: Wire<'_>) -> crate::Error {
    crate::Error::Protocol(Reason::CopyFormat {
        actual: bytes.length(),
    })
}

#[derive(Clone, Copy)]
struct Terminator {
    actual: Length,
}

#[derive(Clone, Copy)]
enum Cstring {
    Terminator(Terminator),
    Utf8(TextError),
}

impl Cstring {
    fn into_sasl(self) -> crate::Error {
        let reason = match self {
            Self::Terminator(Terminator { actual }) => Reason::SaslMechanismTerminator { actual },
            Self::Utf8(TextError { valid, error }) => Reason::SaslMechanismUtf8 { valid, error },
        };
        crate::Error::Protocol(reason)
    }

    fn into_note(self, field: u8) -> crate::Error {
        let reason = match self {
            Self::Terminator(Terminator { actual }) => {
                Reason::NotificationTerminator { field, actual }
            }
            Self::Utf8(TextError { valid, error }) => Reason::NotificationUtf8 {
                field,
                valid,
                error,
            },
        };
        crate::Error::Protocol(reason)
    }
}

fn cstr_bytes(bytes: Wire<'_>) -> Result<(Wire<'_>, Wire<'_>), Terminator> {
    bytes.split_byte(0).ok_or(Terminator {
        actual: bytes.length(),
    })
}

fn cstr(bytes: Wire<'_>) -> Result<(Text<'_>, Wire<'_>), Cstring> {
    let (value, rest) = cstr_bytes(bytes).map_err(Cstring::Terminator)?;
    Ok((value.try_text().map_err(Cstring::Utf8)?, rest))
}

pub(super) enum AuthRequest<'a> {
    Ok,
    Sasl { mechanisms: scram::Mechanisms },
    SaslContinue { data: Wire<'a> },
    SaslFinal { data: Wire<'a> },
    Other(u32),
}

impl<'a> AuthRequest<'a> {
    pub(super) fn decode(bytes: Wire<'a>) -> Result<Self, crate::Error> {
        let Some((kind, rest)) = bytes.split_array::<4>() else {
            return Err(crate::Error::Protocol(Reason::AuthTruncated {
                actual: bytes.length(),
            }));
        };
        let kind = u32::from_be_bytes(*kind);
        match kind {
            messages::Auth::OK => {
                if !rest.is_empty() {
                    return Err(crate::Error::Protocol(Reason::AuthTrailing {
                        actual: rest.length(),
                    }));
                }
                Ok(Self::Ok)
            }
            messages::Auth::SASL => {
                let mut set = scram::Mechanisms::default();
                let mut cursor = rest;
                loop {
                    let Some((first, tail)) = cursor.split_first() else {
                        return Err(crate::Error::Protocol(Reason::SaslTerminator {
                            actual: rest.length(),
                        }));
                    };
                    if first == 0 {
                        if !tail.is_empty() {
                            return Err(crate::Error::Protocol(Reason::SaslTrailing {
                                actual: tail.length(),
                            }));
                        }
                        break;
                    }
                    let (name, next) = cstr(cursor).map_err(Cstring::into_sasl)?;
                    set.offer(name.as_str());
                    cursor = next;
                }
                Ok(Self::Sasl { mechanisms: set })
            }
            messages::Auth::SASL_CONTINUE => Ok(Self::SaslContinue { data: rest }),
            messages::Auth::SASL_FINAL => Ok(Self::SaslFinal { data: rest }),
            other => Ok(Self::Other(other)),
        }
    }
}

pub(super) fn notification<'input, 'd, R: buffer::Cursor + hold::raw::Retain<'d>>(
    source: frame::Payload<'input, 'd, R>,
    queue: &notify::Queue<'d>,
    token: &mut region::Token<region::Identity<'d>>,
) -> Result<(), crate::Error> {
    let bytes = source.wire();
    let Some((pid, body)) = bytes.split_array::<4>() else {
        return Err(crate::Error::Protocol(Reason::NotificationTruncated {
            actual: bytes.length(),
        }));
    };
    let pid = u32::from_be_bytes(*pid);
    let (name, rest) = cstr(body).map_err(|error| error.into_note(0))?;
    let start = bytes.len() - rest.len();
    let (data, tail) = cstr(rest).map_err(|error| error.into_note(1))?;
    if !tail.is_empty() {
        return Err(crate::Error::Protocol(Reason::NotificationTrailing {
            actual: tail.length(),
        }));
    }
    let first = 4 + name.bytes().len();
    let last = start + data.bytes().len();
    let ranges = [4..first, start..last];
    // SAFETY: parsing proves ordered in-bounds UTF-8 ranges, and the source
    // admitted frame length bounds every endpoint and their combined length.
    let fields = unsafe { reply::raw::Fields::new(source.into_inner(), ranges) };
    queue.try_push(fields, pid, token)
}
