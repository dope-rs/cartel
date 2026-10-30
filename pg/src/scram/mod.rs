mod error;
mod password;

pub use error::Error;
pub(crate) use password::Password;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Base64 {
    InvalidByte {
        index: usize,
        byte: u8,
    },
    InvalidLength(usize),
    InvalidLastSymbol {
        offset: usize,
        symbol: u8,
        symbol_value: u8,
    },
    InvalidPadding,
    OutputSliceTooSmall,
}

impl From<base64::DecodeSliceError> for Base64 {
    fn from(error: base64::DecodeSliceError) -> Self {
        match error {
            base64::DecodeSliceError::DecodeError(error) => match error {
                base64::DecodeError::InvalidByte(index, byte) => Self::InvalidByte { index, byte },
                base64::DecodeError::InvalidLength(len) => Self::InvalidLength(len),
                base64::DecodeError::InvalidLastSymbol {
                    offset,
                    symbol,
                    symbol_value: value,
                } => Self::InvalidLastSymbol {
                    offset,
                    symbol,
                    symbol_value: value,
                },
                base64::DecodeError::InvalidPadding => Self::InvalidPadding,
            },
            base64::DecodeSliceError::OutputSliceTooSmall => Self::OutputSliceTooSmall,
        }
    }
}
