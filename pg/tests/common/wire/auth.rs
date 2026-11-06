use crate::common::{
    postgres::{self, Frame, accept::Listener, startup, startup::Stream as _},
    wire::try_mac,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use dope_test::harness::peer;
use sha2::{Digest as _, Sha256};
use std::{borrow::Cow, str, time::Duration};
use unicode_normalization::UnicodeNormalization as _;

pub const CONNECTIONS: usize = 16;

pub const ROUNDS: u32 = 32_768;

pub const SALT: &[u8] = b"queue-admission-proof012345678901234567890123456789012345678";

pub const PASSWORDS: [(&str, &[u8], usize); 7] = [
    (
        "\u{301}\u{316}|password-password-password-password-password-password-password-\u{2168}|A\u{315}\u{305}\u{301}|A\u{315}\u{300}|B\u{315}\u{300}|\u{1100}\u{1161}\u{11a8}|\u{ad}",
        "\u{316}\u{301}|password-password-password-password-password-password-password-IX|A\u{305}\u{301}\u{315}|\u{c0}\u{315}|B\u{300}\u{315}|\u{ac01}|".as_bytes(),
        21,
    ),
    ("B\u{315}\u{300}", "B\u{300}\u{315}".as_bytes(), 9),
    ("\u{ad}", "\u{ad}".as_bytes(), 15),
    ("A\u{0340}", "A\u{0340}".as_bytes(), 21),
    ("\u{1d2c}", "\u{1d2c}".as_bytes(), 21),
    (
        "\u{05d0}\u{1885}\u{ff11}\u{05d0}",
        "\u{05d0}\u{1885}\u{ff11}\u{05d0}".as_bytes(),
        21,
    ),
    (
        "\u{05d0}\u{2800}\u{ff11}\u{05d0}",
        "\u{05d0}\u{2800}1\u{05d0}".as_bytes(),
        21,
    ),
];

pub fn passwords() -> impl Iterator<Item = (String, Cow<'static, [u8]>, &'static [u8], u32, bool)> {
    PASSWORDS
        .into_iter()
        .map(|(password, prepared, salt)| {
            (
                password.to_owned(),
                Cow::Borrowed(prepared),
                &SALT[..salt],
                ROUNDS,
                true,
            )
        })
        .chain(std::iter::once_with(|| {
            let password = format!("A{}{}", "\u{315}".repeat(512), "\u{300}".repeat(512),);
            let prepared = password.nfkc().collect::<String>().into_bytes();
            (password, Cow::Owned(prepared), &SALT[..21], ROUNDS, true)
        }))
        .chain(
            [
                (0, 51, 4_096, true),
                (64, 52, 4_096, true),
                (65, 59, 4_096, true),
                (119, 60, 4_097, true),
                (120, 9, 100_000, true),
                (127, 15, 100_000, true),
                (1, 21, 4_096, false),
            ]
            .into_iter()
            .map(|(length, salt, rounds, valid)| {
                let password = "p".repeat(length);
                (
                    password.clone(),
                    Cow::Owned(password.into_bytes()),
                    &SALT[..salt],
                    rounds,
                    valid,
                )
            }),
        )
}

fn try_auth(socket: &peer::Stream<'_>, code: u32, body: &[u8]) -> Result<(), peer::Error> {
    let mut message = Vec::from(code.to_be_bytes());
    message.extend_from_slice(body);
    socket.write_all(&Frame::new(b'R', &message).encode())
}

fn read(socket: &peer::Stream<'_>, tag: Option<u8>) -> Vec<u8> {
    if let Some(expected) = tag {
        let mut actual = [0];
        socket.read_exact(&mut actual).expect("frontend tag");
        assert_eq!(actual[0], expected);
    }
    startup::read(socket).expect("frontend frame")
}

pub fn serve(
    listener: &mut peer::TcpAcceptor<'_>,
    password: &[u8],
    salt: &[u8],
    rounds: u32,
    connections: usize,
    valid: bool,
) {
    let mut peers: Vec<_> = (0..connections)
        .map(|_| {
            let mut socket = Listener::accept_within(listener, Duration::from_secs(10))
                .expect("peer connection");
            socket.set_nodelay(true).expect("TCP_NODELAY");
            socket
                .read_wait(Some(Duration::from_secs(10)))
                .expect("read timeout");
            socket
                .write_wait(Some(Duration::from_secs(10)))
                .expect("write timeout");
            let startup = read(&mut socket, None);
            assert_eq!(&startup[..4], &196608_u32.to_be_bytes());
            try_auth(&mut socket, 10, b"SCRAM-SHA-256\0\0").expect("authentication message");
            socket
        })
        .collect();
    let messages: Vec<_> = peers
        .iter_mut()
        .enumerate()
        .map(|(index, socket)| {
            let first = read(socket, Some(b'p'));
            let end = first
                .iter()
                .position(|byte| *byte == 0)
                .expect("mechanism terminator");
            assert_eq!(&first[..end], b"SCRAM-SHA-256");
            let body = str::from_utf8(&first[end + 5..]).expect("client first UTF-8");
            assert_eq!(&first[end + 1..end + 5], &(body.len() as i32).to_be_bytes());
            let bare = body.strip_prefix("n,,").expect("GS2 header").to_owned();
            let nonce = bare.strip_prefix("n=,r=").expect("client nonce");
            let salt = if index < CONNECTIONS / 2 {
                salt
            } else {
                &salt[..salt.len() - (index - CONNECTIONS / 2 + 1)]
            };
            let server = format!("r={nonce}server,s={},i={rounds}", STANDARD.encode(salt));
            (bare, server, salt)
        })
        .collect();
    for (socket, (_, first, _)) in peers.iter_mut().zip(&messages) {
        try_auth(socket, 11, first.as_bytes()).expect("authentication message");
    }
    let mut peer_iter = peers.iter_mut();
    for group in messages.chunk_by(|left, right| left.2 == right.2) {
        let mut salted = [0; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password, group[0].2, rounds, &mut salted);
        let client = try_mac(&salted, b"Client Key").expect("client key");
        let stored = Sha256::digest(&client);
        let server = try_mac(&salted, b"Server Key").expect("server key");
        for ((first, offer, _), socket) in group.iter().zip(peer_iter.by_ref()) {
            let body = read(socket, Some(b'p'));
            let reply = str::from_utf8(&body).expect("client final UTF-8");
            let (bare, proof) = reply.rsplit_once(",p=").expect("client proof");
            assert_eq!(bare.strip_prefix("c=biws,"), offer.split(',').next());
            let message = format!("{first},{offer},{bare}");
            let mask = try_mac(&stored, message.as_bytes()).expect("client MAC");
            let expected: Vec<_> = client
                .iter()
                .zip(mask)
                .map(|(left, right)| left ^ right)
                .collect();
            assert_eq!(STANDARD.decode(proof).expect("proof base64"), expected);
            let mut verifier = try_mac(&server, message.as_bytes()).expect("server MAC");
            if !valid {
                verifier[0] ^= 1;
            }
            let reply = format!("v={}", STANDARD.encode(verifier));
            try_auth(socket, 12, reply.as_bytes()).expect("authentication message");
            if !valid {
                continue;
            }
            try_auth(socket, 0, &[]).expect("authentication message");
            socket
                .write_all(&Frame::new(b'Z', b"I").encode())
                .expect("ready");
            assert!(read(socket, Some(b'S')).is_empty());
            socket
                .write_all(&Frame::new(b'Z', b"I").encode())
                .expect("prepared");
        }
    }
    for peer in peers {
        peer.read_closed().expect("authenticated connection closed");
    }
}

pub const REJECTED: [(u32, &str, &str); 8] = [
    (
        11,
        "r",
        "ServerFirst(AttributeTruncated { offset: Offset(0), actual: Length(1) })",
    ),
    (
        11,
        "r=a,s=Zg==,i=4x",
        "ServerFirst(Iteration { error: Invalid { offset: Offset(14), byte: 120 } })",
    ),
    (
        11,
        "r=a,s=Zg==,i=4096,",
        "ServerFirst(TrailingComma { offset: Offset(17) })",
    ),
    (
        11,
        "r={nonce}server,s=QUJD!A==,i=4096",
        "ServerFirstBase64(InvalidByte { index: 4, byte: 33 })",
    ),
    (
        11,
        "r={nonce}server,s=QUJDZ,i=4096",
        "ServerFirstBase64(InvalidLength(5))",
    ),
    (
        11,
        "r={nonce}server,s=QUJDZh==,i=4096",
        "ServerFirstBase64(InvalidLastSymbol { offset: 5, symbol: 104, symbol_value: 33 })",
    ),
    (
        11,
        "r={nonce}server,s=QUJDZg=,i=4096",
        "ServerFirstBase64(InvalidPadding)",
    ),
    (12, "e=invalid-proof", "Protocol(FinalState)"),
];

pub fn reject(
    socket: &mut &peer::Stream<'_>,
    code: u32,
    message: &str,
) -> Result<(), postgres::Error> {
    socket.prepare()?;
    startup::read(socket)?;
    try_auth(socket, 10, b"SCRAM-SHA-256\0\0")?;
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
    try_auth(socket, code, message.replace("{nonce}", nonce).as_bytes())?;
    socket.read_closed()
}
