use crate::{protocol, reply::server::Field};
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

impl<'a> Context<'a> {
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

    pub(super) fn field(
        &mut self,
        field: Field,
        value: &'a str,
    ) -> Option<&mut Option<num::NonZero<u32>>> {
        if field == Field::Detail {
            self.detail = Some(value);
        } else if field == Field::Hint {
            self.hint = Some(value);
        } else if field == Field::Position {
            return Some(&mut self.position);
        } else if field == Field::InnerPosition {
            return Some(&mut self.inner_position);
        } else if field == Field::InnerQuery {
            self.inner_query = Some(value);
        } else if field == Field::Location {
            self.location = Some(value);
        } else if field == Field::Schema {
            self.origin.schema = Some(value);
        } else if field == Field::Table {
            self.origin.table = Some(value);
        } else if field == Field::Column {
            self.origin.column = Some(value);
        } else if field == Field::DataType {
            self.origin.data_type = Some(value);
        } else if field == Field::Constraint {
            self.origin.constraint = Some(value);
        } else if field == Field::File {
            self.origin.file = Some(value);
        } else if field == Field::Line {
            return Some(&mut self.origin.line);
        } else if field == Field::Routine {
            self.origin.routine = Some(value);
        }
        None
    }
}

pub(super) fn try_number(
    kind: u8,
    value: protocol::Text<'_>,
) -> Result<num::NonZero<u32>, protocol::Reason> {
    use protocol::Offset;

    let mut bytes = value.bytes();
    let length = bytes.length().get();
    if bytes.is_empty() {
        return Err(protocol::Reason::DiagnosticPosition {
            kind,
            error: protocol::Number::Empty,
        });
    }
    let mut number = 0u32;
    while let Some((byte, tail)) = bytes.split_first() {
        if !byte.is_ascii_digit() {
            return Err(protocol::Reason::DiagnosticPosition {
                kind,
                error: protocol::Number::Invalid {
                    offset: Offset::from_u32(length - bytes.length().get()),
                    byte,
                },
            });
        }
        let Some(next) = number
            .checked_mul(10)
            .and_then(|number| number.checked_add(u32::from(byte - b'0')))
        else {
            return Err(protocol::Reason::DiagnosticPosition {
                kind,
                error: protocol::Number::Overflow {
                    offset: Offset::from_u32(length - bytes.length().get()),
                    byte,
                },
            });
        };
        number = next;
        bytes = tail;
    }
    match num::NonZero::<u32>::new(number) {
        Some(number) => Ok(number),
        None => Err(protocol::Reason::DiagnosticPosition {
            kind,
            error: protocol::Number::Zero,
        }),
    }
}
