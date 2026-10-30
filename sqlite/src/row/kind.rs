#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Kind {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}
