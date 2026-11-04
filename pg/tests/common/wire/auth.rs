use crate::common::postgres::{self, Frame, startup, startup::Stream as _};
use dope_test::harness::peer;
use std::str;

pub const CONNECTIONS: usize = 8;

pub const ROUNDS: u32 = 32_768;

pub const SALT: &[u8] = b"queue-admission-proof";

pub const PASSWORDS: [(&str, &[u8]); 6] = [
    (
        "\u{301}\u{316}|password-password-password-password-password-password-password-\u{2168}|A\u{305}\u{301}|A\u{315}\u{300}|B\u{315}\u{300}|\u{1100}\u{1161}\u{11a8}|\u{ad}",
        "\u{316}\u{301}|password-password-password-password-password-password-password-IX|A\u{305}\u{301}|\u{c0}\u{315}|B\u{300}\u{315}|\u{ac01}|".as_bytes(),
    ),
    ("\u{ad}", "\u{ad}".as_bytes()),
    ("A\u{0340}", "A\u{0340}".as_bytes()),
    ("\u{1d2c}", "\u{1d2c}".as_bytes()),
    (
        "\u{05d0}\u{1885}\u{ff11}\u{05d0}",
        "\u{05d0}\u{1885}\u{ff11}\u{05d0}".as_bytes(),
    ),
    (
        "\u{05d0}\u{2800}\u{ff11}\u{05d0}",
        "\u{05d0}\u{2800}1\u{05d0}".as_bytes(),
    ),
];

pub trait Auth {
    fn try_auth(&self, code: u32, body: &[u8]) -> Result<(), peer::Error>;
}

impl Auth for peer::Stream<'_> {
    fn try_auth(&self, code: u32, body: &[u8]) -> Result<(), peer::Error> {
        let mut message = Vec::from(code.to_be_bytes());
        message.extend_from_slice(body);
        self.write_all(&Frame::new(b'R', &message).encode())
    }
}

pub const REJECTED: [(&str, &str); 7] = [
    (
        "r",
        "ServerFirst(AttributeTruncated { offset: Offset(0), actual: Length(1) })",
    ),
    (
        "r=a,s=Zg==,i=4x",
        "ServerFirst(Iteration { error: Invalid { offset: Offset(14), byte: 120 } })",
    ),
    (
        "r=a,s=Zg==,i=4096,",
        "ServerFirst(TrailingComma { offset: Offset(17) })",
    ),
    (
        "r={nonce}server,s=QUJD!A==,i=4096",
        "ServerFirstBase64(InvalidByte { index: 4, byte: 33 })",
    ),
    (
        "r={nonce}server,s=QUJDZ,i=4096",
        "ServerFirstBase64(InvalidLength(5))",
    ),
    (
        "r={nonce}server,s=QUJDZh==,i=4096",
        "ServerFirstBase64(InvalidLastSymbol { offset: 5, symbol: 104, symbol_value: 33 })",
    ),
    (
        "r={nonce}server,s=QUJDZg=,i=4096",
        "ServerFirstBase64(InvalidPadding)",
    ),
];

pub fn reject(socket: &mut &peer::Stream<'_>, message: &str) -> Result<(), postgres::Error> {
    socket.prepare()?;
    startup::read(socket)?;
    socket.try_auth(10, b"SCRAM-SHA-256\0\0")?;
    let mut tag = [0];
    socket.read_exact(&mut tag)?;
    if tag != *b"p" {
        return Err(postgres::Error::UnexpectedRequest);
    }
    let first = startup::read(socket)?;
    let nonce = first
        .strip_prefix(b"SCRAM-SHA-256\0")
        .and_then(|body| body.get(4..))
        .and_then(|body| body.strip_prefix(b"n,,n=,r="))
        .ok_or(postgres::Error::UnexpectedRequest)?;
    let nonce = str::from_utf8(nonce).map_err(postgres::Error::Utf8)?;
    socket.try_auth(11, message.replace("{nonce}", nonce).as_bytes())?;
    socket.read_closed()
}
