/// A statement whose successful execution has no row payload.
pub enum Command {}

/// A statement whose successful execution yields typed rows.
pub enum Rows<const SCALAR: bool = false> {}

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
    const SCALAR: bool = false;
}

impl Shape for Command {
    const KIND: Kind = Kind::Command;
}

impl<const S: bool> Shape for Rows<S> {
    const KIND: Kind = Kind::Rows;
    const SCALAR: bool = S;
}
