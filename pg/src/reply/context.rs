use crate::protocol;
use std::{mem, num};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Origin<'a> {
    schema: Option<&'a str>,
    table: Option<&'a str>,
    column: Option<&'a str>,
    data_type: Option<&'a str>,
    constraint: Option<&'a str>,
    file: Option<&'a str>,
    line: Option<num::NonZero<u32>>,
    routine: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Context<'a> {
    detail: Option<&'a str>,
    hint: Option<&'a str>,
    position: Option<num::NonZero<u32>>,
    inner_position: Option<num::NonZero<u32>>,
    inner_query: Option<&'a str>,
    location: Option<&'a str>,
    origin: Origin<'a>,
}

const _: () = {
    let fields = 4 * mem::size_of::<Option<&str>>()
        + 2 * mem::size_of::<Option<num::NonZero<u32>>>()
        + mem::size_of::<Origin<'_>>();
    let align = mem::align_of::<Context<'_>>();
    assert!(mem::size_of::<Context<'_>>() == fields.next_multiple_of(align));
};

pub(super) struct Write<'a, 'value> {
    context: &'a mut Context<'value>,
}

impl<'a> Context<'a> {
    pub(super) fn write<'b>(&'b mut self) -> Write<'b, 'a> {
        Write { context: self }
    }

    pub const fn detail(self) -> Option<&'a str> {
        self.detail
    }

    pub const fn hint(self) -> Option<&'a str> {
        self.hint
    }

    pub const fn position(self) -> Option<num::NonZero<u32>> {
        self.position
    }

    pub const fn inner_pos(self) -> Option<num::NonZero<u32>> {
        self.inner_position
    }

    pub const fn inner_query(self) -> Option<&'a str> {
        self.inner_query
    }

    pub const fn location(self) -> Option<&'a str> {
        self.location
    }

    pub const fn schema(self) -> Option<&'a str> {
        self.origin.schema
    }

    pub const fn table(self) -> Option<&'a str> {
        self.origin.table
    }

    pub const fn column(self) -> Option<&'a str> {
        self.origin.column
    }

    pub const fn data_type(self) -> Option<&'a str> {
        self.origin.data_type
    }

    pub const fn constraint(self) -> Option<&'a str> {
        self.origin.constraint
    }

    pub const fn file(self) -> Option<&'a str> {
        self.origin.file
    }

    pub const fn line(self) -> Option<num::NonZero<u32>> {
        self.origin.line
    }

    pub const fn routine(self) -> Option<&'a str> {
        self.origin.routine
    }
}

impl<'context, 'value> Write<'context, 'value> {
    pub(super) fn field(
        self,
        kind: u8,
        value: &'value str,
    ) -> Option<&'context mut Option<num::NonZero<u32>>> {
        if kind == b'D' {
            self.context.detail = Some(value);
        } else if kind == b'H' {
            self.context.hint = Some(value);
        } else if kind == b'P' {
            return Some(&mut self.context.position);
        } else if kind == b'p' {
            return Some(&mut self.context.inner_position);
        } else if kind == b'q' {
            self.context.inner_query = Some(value);
        } else if kind == b'W' {
            self.context.location = Some(value);
        } else if kind == b's' {
            self.context.origin.schema = Some(value);
        } else if kind == b't' {
            self.context.origin.table = Some(value);
        } else if kind == b'c' {
            self.context.origin.column = Some(value);
        } else if kind == b'd' {
            self.context.origin.data_type = Some(value);
        } else if kind == b'n' {
            self.context.origin.constraint = Some(value);
        } else if kind == b'F' {
            self.context.origin.file = Some(value);
        } else if kind == b'L' {
            return Some(&mut self.context.origin.line);
        } else if kind == b'R' {
            self.context.origin.routine = Some(value);
        }
        None
    }

    pub(super) fn try_set(
        self,
        kind: u8,
        value: protocol::Text<'value>,
    ) -> Result<(), crate::Error> {
        if let Some(number) = self.field(kind, value.as_str()) {
            *number = Some(Self::try_number(kind, value)?);
        }
        Ok(())
    }

    fn try_number(kind: u8, value: protocol::Text<'_>) -> Result<num::NonZero<u32>, crate::Error> {
        use protocol::Offset;

        let mut bytes = value.bytes();
        let length = bytes.length().get();
        if bytes.is_empty() {
            return Err(crate::Error::Protocol(
                protocol::Reason::DiagnosticPosition {
                    kind,
                    error: protocol::Number::Empty,
                },
            ));
        }
        let mut number = 0u32;
        while let Some((byte, tail)) = bytes.split_first() {
            if !byte.is_ascii_digit() {
                return Err(crate::Error::Protocol(
                    protocol::Reason::DiagnosticPosition {
                        kind,
                        error: protocol::Number::Invalid {
                            offset: Offset::from_u32(length - bytes.length().get()),
                            byte,
                        },
                    },
                ));
            }
            let Some(next) = number
                .checked_mul(10)
                .and_then(|number| number.checked_add(u32::from(byte - b'0')))
            else {
                return Err(crate::Error::Protocol(
                    protocol::Reason::DiagnosticPosition {
                        kind,
                        error: protocol::Number::Overflow {
                            offset: Offset::from_u32(length - bytes.length().get()),
                            byte,
                        },
                    },
                ));
            };
            number = next;
            bytes = tail;
        }
        match num::NonZero::<u32>::new(number) {
            Some(number) => Ok(number),
            None => Err(crate::Error::Protocol(
                protocol::Reason::DiagnosticPosition {
                    kind,
                    error: protocol::Number::Zero,
                },
            )),
        }
    }
}
