use std::mem;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[repr(u8)]
pub enum Error {
    InvalidEncoding,
    ExtensionsNotSupported,
    InvalidProof,
    ChannelBindingsDontMatch,
    ServerDoesSupportChannelBinding,
    ChannelBindingNotSupported,
    UnsupportedChannelBindingType,
    UnknownUser,
    InvalidUsernameEncoding,
    NoResources,
    Other,
}

const _: () = assert!(mem::size_of::<Error>() == 1);
