pub(crate) struct Fe;

impl Fe {
    pub(super) const STARTUP_PROTOCOL: u32 = 0x0003_0000;
    pub(crate) const CANCEL_REQUEST: u32 = 80877102;

    pub(super) const PARSE: u8 = b'P';
    pub(super) const BIND: u8 = b'B';
    pub(super) const EXECUTE: u8 = b'E';
    pub(super) const SYNC: u8 = b'S';
    pub(super) const PASSWORD: u8 = b'p';
    pub(super) const COPY_DATA: u8 = b'd';
    pub(super) const COPY_DONE: u8 = b'c';
}

pub(super) struct Be;

impl Be {
    pub(super) const AUTH: u8 = b'R';
    pub(super) const PARAMETER_STATUS: u8 = b'S';
    pub(super) const BACKEND_KEY_DATA: u8 = b'K';
    pub(super) const READY_FOR_QUERY: u8 = b'Z';
    pub(super) const ROW_DESCRIPTION: u8 = b'T';
    pub(super) const DATA_ROW: u8 = b'D';
    pub(super) const COMMAND_COMPLETE: u8 = b'C';
    pub(super) const PARSE_COMPLETE: u8 = b'1';
    pub(super) const BIND_COMPLETE: u8 = b'2';
    pub(super) const ERROR_RESPONSE: u8 = b'E';
    pub(super) const NOTICE_RESPONSE: u8 = b'N';
    pub(super) const NOTIFICATION_RESPONSE: u8 = b'A';
    pub(super) const NO_DATA: u8 = b'n';
    pub(super) const EMPTY_QUERY_RESPONSE: u8 = b'I';
    pub(super) const PARAMETER_DESCRIPTION: u8 = b't';
    pub(super) const PORTAL_SUSPENDED: u8 = b's';
    pub(super) const COPY_IN_RESPONSE: u8 = b'G';
    pub(super) const COPY_OUT_RESPONSE: u8 = b'H';
    pub(super) const COPY_DATA: u8 = b'd';
    pub(super) const COPY_DONE: u8 = b'c';
}

pub(super) struct Auth;

impl Auth {
    pub(super) const OK: u32 = 0;
    pub(super) const SASL: u32 = 10;
    pub(super) const SASL_CONTINUE: u32 = 11;
    pub(super) const SASL_FINAL: u32 = 12;
}
