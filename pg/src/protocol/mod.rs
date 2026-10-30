pub mod encode;
pub mod frame;
pub mod logic;
pub(crate) mod messages;
pub(crate) mod runtime;
mod scram;
pub mod value;
mod wire;

use o3::buffer::bytes::bounded;
use std::str;

pub use frame::Codec;
pub use wire::Wire;

/// Valid UTF-8 retaining the same wire extent bound.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Text<'a>(&'a str);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TextError {
    pub valid: Length,
    pub error: Utf8,
}

impl<'a> Text<'a> {
    pub const fn as_str(&self) -> &'a str {
        self.0
    }

    pub fn bytes(self) -> Wire<'a> {
        // SAFETY: Text is constructed by validating this exact admitted Wire.
        Wire::from_raw(unsafe { bounded::raw::Input::new(self.0.as_bytes()) })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Length(u32);

impl Length {
    pub(crate) const fn from_u32(value: u32) -> Self {
        Self(value)
    }

    pub(crate) fn try_new(value: usize) -> Result<Self, crate::Error> {
        u32::try_from(value)
            .map(Self)
            .map_err(|_| crate::Error::Protocol(Reason::WireLength { actual: value }))
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Offset(u32);

impl Offset {
    pub(crate) const fn from_u32(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Utf8 {
    Incomplete,
    One,
    Two,
    Three,
    Other,
}

impl Utf8 {
    pub(crate) fn from_error(error: str::Utf8Error) -> Self {
        match error.error_len() {
            None => Self::Incomplete,
            Some(1) => Self::One,
            Some(2) => Self::Two,
            Some(3) => Self::Three,
            Some(_) => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Number {
    Empty,
    Zero,
    LeadingZero { offset: Offset },
    Invalid { offset: Offset, byte: u8 },
    Overflow { offset: Offset, byte: u8 },
}

/// Closed reason codes for authentication and PostgreSQL protocol failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reason {
    WireLength {
        actual: usize,
    },
    RequiredLength {
        required: usize,
    },
    AttributeEquals {
        offset: Offset,
        actual: u8,
    },
    AttributeName {
        offset: Offset,
        actual: u8,
    },
    AttributeOrder {
        actual: u8,
        expected: u8,
    },
    AttributeTruncated {
        offset: Offset,
        actual: Length,
    },
    AuthBeforeFinal,
    AuthTruncated {
        actual: Length,
    },
    AuthTrailing {
        actual: Length,
    },
    BackendKeyBeforeAuth,
    BackendKeyStale,
    BackendKeyWidth {
        actual: Length,
    },
    Bool {
        column: u16,
        actual: u8,
    },
    ColumnBytes {
        column: u16,
        actual: Length,
        required: Length,
    },
    ColumnCount {
        actual: u16,
        expected: u16,
    },
    ColumnLength {
        column: u16,
        actual: Length,
    },
    ColumnNull {
        column: u16,
    },
    ColumnWidth {
        column: u16,
        actual: Length,
        expected: Length,
    },
    ControlPayload {
        tag: u8,
        actual: Length,
    },
    CopyFormat {
        actual: Length,
    },
    CommandData,
    CommandEmpty,
    CommandUnknown,
    CommitTag,
    GroupReply,
    DiagnosticMessage,
    DiagnosticPosition {
        kind: u8,
        error: Number,
    },
    DiagnosticResponseTerminator {
        offset: Offset,
    },
    DiagnosticSeverity,
    DiagnosticSqlstateByte {
        actual: Length,
        offset: Offset,
        byte: u8,
    },
    DiagnosticSqlstateLength {
        actual: Length,
    },
    DiagnosticTerminator {
        kind: u8,
        offset: Offset,
        actual: Length,
    },
    DiagnosticTrailing {
        offset: Offset,
        actual: Length,
    },
    DiagnosticUtf8 {
        kind: u8,
        offset: Offset,
        error: Utf8,
    },
    DuplicateAttribute {
        name: u8,
        offset: Offset,
    },
    DuplicateBackendKey,
    DuplicateDiagnostic {
        kind: u8,
    },
    DuplicateServerFirst,
    EmptyAttribute {
        name: u8,
        offset: Offset,
    },
    Extension {
        offset: Offset,
    },
    ExtensionValue {
        name: u8,
        offset: Offset,
        byte: u8,
    },
    FinalAttribute {
        actual: u8,
    },
    FinalState,
    FinalValue {
        offset: Offset,
        byte: u8,
    },
    FrameCapacity {
        declared: u32,
        capacity: Length,
    },
    FrameLength {
        declared: u32,
        minimum: u32,
    },
    FrameTruncated {
        actual: Length,
        required: Length,
    },
    Iteration {
        error: Number,
    },
    JsonTruncated {
        column: u16,
        actual: Length,
    },
    JsonUtf8 {
        column: u16,
        valid: Length,
        error: Utf8,
    },
    JsonVersion {
        column: u16,
        actual: u8,
    },
    LtreeMissing,
    MissingAttribute {
        expected: u8,
    },
    MissingResult,
    NoDataMismatch,
    NoSasl,
    NonceByte {
        offset: Offset,
        actual: u8,
        expected: u8,
    },
    NonceChar {
        offset: Offset,
        byte: u8,
    },
    NonceLength {
        actual: Length,
        minimum: Length,
    },
    NegativeArrayDimension {
        column: u16,
        actual: i32,
    },
    NegativeColumnCount {
        actual: i16,
    },
    NegativeColumnLength {
        column: u16,
        actual: i32,
    },
    NotificationChannelRange {
        start: Offset,
        end: Offset,
        actual: Length,
    },
    NotificationPayloadRange {
        start: Offset,
        end: Offset,
        actual: Length,
    },
    NotificationTerminator {
        field: u8,
        actual: Length,
    },
    NotificationTrailing {
        actual: Length,
    },
    NotificationTruncated {
        actual: Length,
    },
    NotificationUtf8 {
        field: u8,
        valid: Length,
        error: Utf8,
    },
    ParameterNameEmpty,
    ParameterNameTerminator {
        actual: Length,
    },
    ParameterTrailing {
        offset: Offset,
        actual: Length,
    },
    ParameterValueTerminator {
        offset: Offset,
        actual: Length,
    },
    PrepareReady,
    PrepareReplies,
    PrepareRetained,
    ReadyBeforeDescription,
    ReadyUnknown,
    ResponseUnknown,
    RowDecoderColumns {
        declared: u16,
    },
    RowEmpty,
    RowTrailing {
        actual: Length,
    },
    RowTruncated {
        actual: Length,
    },
    RowUnread {
        remaining: u16,
    },
    RowUnknown,
    SaslContinue,
    SaslMechanismTerminator {
        actual: Length,
    },
    SaslMechanismUtf8 {
        valid: Length,
        error: Utf8,
    },
    SaslPlus,
    SaslState,
    SaslTerminator {
        actual: Length,
    },
    SaslTrailing {
        actual: Length,
    },
    SaltPadding {
        offset: Offset,
    },
    SignatureLength {
        actual: Length,
        expected: Length,
    },
    SignatureMismatch,
    StandardExtension {
        name: u8,
        offset: Offset,
    },
    TrailingComma {
        offset: Offset,
    },
    TransactionLane,
    TransactionLease,
    TransactionOpen,
    TextUtf8 {
        column: u16,
        valid: Length,
        error: Utf8,
    },
    ArrayDimensions {
        column: u16,
        actual: i32,
    },
    ArrayHeader {
        column: u16,
        actual: Length,
        required: Length,
    },
    ArrayNulls {
        column: u16,
        actual: i32,
    },
    ArrayOid {
        column: u16,
        actual: u32,
        expected: u32,
    },
    ArrayPayload {
        column: u16,
        elements: u32,
        actual: Length,
    },
    ArrayTextOid {
        column: u16,
        actual: u32,
    },
    ArrayBytes {
        actual: Length,
        required: Length,
    },
    ArrayI32Width {
        actual: Length,
    },
    ArrayI64Width {
        actual: Length,
    },
    ArrayLength {
        actual: Length,
    },
    NegativeArrayLength {
        actual: i32,
    },
    ArrayNull,
    ArrayTextUtf8 {
        valid: Length,
        error: Utf8,
    },
    ArrayTrailing {
        actual: Length,
    },
}

const _: () = assert!(std::mem::size_of::<Length>() == std::mem::size_of::<u32>());
const _: () = assert!(size_of::<Text<'static>>() == size_of::<&str>());
const _: () = assert!(std::mem::size_of::<Offset>() == std::mem::size_of::<u32>());
const _: () = assert!(std::mem::size_of::<Number>() <= std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Reason>() <= 2 * std::mem::size_of::<usize>());

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PickPolicy {
    #[default]
    RoundRobin,
    /// Selects an eligible connection with the fewest reply orders plus
    /// pending Sync boundaries, including the boundary of an open pipeline.
    LeastInflight,
}
