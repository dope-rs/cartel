use dope::manifold::timing::{Policy, Throughput};
use dope_test::harness::{GUARD, peer};
use o3::base::time::Positive;
use std::net::TcpListener;

use crate::common::postgres::{
    Error, Frame, accept::Listener as _, group::COMMAND_ERROR, read_empty, startup::Stream as _,
};

pub struct ShortIdle;

impl Policy for ShortIdle {
    const CONNECT_DEADLINE: Positive = Throughput::CONNECT_DEADLINE;
    const IDLE_WINDOW: Positive = Positive::from_millis::<250>();
    const SEND_DEADLINE: Positive = Throughput::SEND_DEADLINE;
}

pub fn partial_pool(listener: TcpListener) -> Result<(), Error> {
    let listener = peer::TcpAcceptor::try_adopt(listener.into())?;
    let mut first = listener.accept_within(GUARD)?;
    first.try_start(None)?;
    first.read_wait(Some(GUARD))?;
    let mut second = listener.accept_within(GUARD)?;
    second.try_start(None)?;
    drop(listener);
    drop(second);
    reply(&first)?;
    Ok(())
}

pub fn stalled_partial(listener: &mut peer::TcpAcceptor<'_>) -> Result<(), Error> {
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    read_empty(&socket, b'S')?;
    let notice = Frame::new(b'N', b"SNOTICE\0VNOTICE\0C00000\0Mpartial\0\0").encode();
    socket.write_all(
        &[
            Frame::new(b'C', b"SELECT 1\0").encode().as_slice(),
            Frame::new(b'Z', b"I").encode().as_slice(),
            &notice[..8],
        ]
        .concat(),
    )?;
    socket.read_closed()?;
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    reply(&socket)?;
    socket.read_closed()
}

fn reply(socket: &peer::Stream<'_>) -> Result<(), Error> {
    read_empty(socket, b'S')?;
    socket.write_all(&Frame::new(b'C', b"SELECT 1\0").encode())?;
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    Ok(())
}

pub fn unsolicited_partial(mut socket: peer::Stream<'_>) -> Result<(), Error> {
    socket.try_start(None)?;
    read_empty(&socket, b'S')?;
    let complete = Frame::new(b'C', b"SELECT 1\0").encode();
    let ready = Frame::new(b'Z', b"I").encode();
    socket.write_all(&[complete.as_slice(), ready.as_slice(), &complete[..8]].concat())?;
    match read_empty(&socket, b'S') {
        Ok(()) => {
            socket.write_all(&[&complete[8..], ready.as_slice()].concat())?;
            Ok(())
        }
        Err(Error::Io(peer::Error::UnexpectedEof)) => Ok(()),
        Err(error) => Err(error),
    }
}

pub const ASYNCHRONOUS_PARTIAL: [(&[u8], &[u8]); 3] = [
    (
        b"C\0\0\0\x0aBEGIN\0Z\0\0\0\x05TN\0\0\0\x25SNO",
        b"TICE\0VNOTICE\0C00000\0Mpartial\0\0",
    ),
    (
        b"C\0\0\0\x0dSELECT 1\0Z\0\0\0\x05TA\0\0\0\x16\0\0\0",
        b"\x07partial\0ready\0",
    ),
    (
        b"C\0\0\0\x0dSELECT 1\0Z\0\0\0\x05TS\0\0\0\x19cli",
        b"ent_encoding\0UTF8\0",
    ),
];
pub const TRANSACTION_SELECT: &[u8] = b"C\0\0\0\x0dSELECT 1\0Z\0\0\0\x05T";
pub const COMMITTED: &[u8] = b"C\0\0\0\x0bCOMMIT\0Z\0\0\0\x05I";

pub fn detached_partial(mut socket: peer::Stream<'_>) -> Result<(), Error> {
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    read_empty(&socket, b'S')?;
    let failed = Frame::new(b'E', COMMAND_ERROR).encode();
    let ready = Frame::new(b'Z', b"I").encode();
    socket.write_all(&failed[..8])?;
    read_empty(&socket, b'S')?;
    let complete = Frame::new(b'C', b"SELECT 2\0").encode();
    socket.write_all(
        &[
            &failed[8..],
            ready.as_slice(),
            complete.as_slice(),
            ready.as_slice(),
        ]
        .concat(),
    )?;
    socket.read_closed()
}

pub fn finalize(socket: &peer::Stream<'_>) -> Result<(), Error> {
    for (command, ready) in [
        (b"BEGIN\0".as_slice(), b"T"),
        (b"COMMIT\0", b"I"),
        (b"BEGIN\0", b"T"),
        (b"SELECT 1\0", b"T"),
        (b"COMMIT\0", b"I"),
    ] {
        read_empty(socket, b'S')?;
        socket.write_all(&Frame::new(b'C', command).encode())?;
        socket.write_all(&Frame::new(b'Z', ready).encode())?;
    }
    socket.read_closed()
}
