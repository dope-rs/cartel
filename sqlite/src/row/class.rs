use crate::row;

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Class {
    Integer,
    Number,
    Text,
    Blob,
}

impl Class {
    pub(crate) const fn is_match(self, actual: row::raw::Kind) -> bool {
        match self {
            Self::Integer => matches!(actual, row::raw::Kind::Integer),
            Self::Number => matches!(actual, row::raw::Kind::Integer | row::raw::Kind::Real),
            Self::Text => matches!(actual, row::raw::Kind::Text),
            Self::Blob => matches!(actual, row::raw::Kind::Blob),
        }
    }
}
