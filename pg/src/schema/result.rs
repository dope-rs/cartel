/// A statement whose successful execution has no row payload.
pub enum Command {}

/// A statement whose successful execution yields typed rows.
pub enum Rows {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
#[doc(hidden)]
pub enum Kind {
    Command,
    Rows,
}

#[doc(hidden)]
pub trait Shape {
    const KIND: Kind;
}

impl Shape for Command {
    const KIND: Kind = Kind::Command;
}

impl Shape for Rows {
    const KIND: Kind = Kind::Rows;
}
