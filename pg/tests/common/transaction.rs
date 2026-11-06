use dope_test::harness::{GUARD, peer};

use crate::common::{
    postgres::{
        Error, Frame,
        group::COMMAND_ERROR,
        read_empty,
        startup::{self, Stream as _},
    },
    wire::checked::Database,
};

pub const DETACHED_RESPONSES: [&[u8]; 2] = [
    b"C\0\0\0\x0dSELECT 1\0Z\0\0\0\x05I",
    b"D\0\0\0\x0b\0\x01\0\0\0\x011E\0\0\0\x25SERROR\0VERROR\0C23505\0Mduplicate\0\0Z\0\0\0\x05I",
];

pub fn streamed(listener: &mut peer::TcpAcceptor<'_>) -> Result<(), Error> {
    let socket = listener.accept()?;
    socket.prepare()?;
    Database::start(&socket)?;
    socket.read_wait(Some(GUARD))?;
    // BEGIN must follow the stream's Sync without waiting for its response.
    for expected in [b'B', b'E', b'S', b'P', b'B', b'E', b'S'] {
        let mut tag = [0];
        socket.read_exact(&mut tag)?;
        let body = startup::read(&socket)?;
        if tag[0] != expected {
            return Err(Error::UnexpectedRequest);
        }
        if expected == b'S' && !body.is_empty() {
            return Err(Error::NonemptyFrame);
        }
    }
    socket.write_all(
        &[
            Frame::new(b'E', COMMAND_ERROR).encode(),
            Frame::new(b'Z', b"I").encode(),
            Frame::new(b'C', b"BEGIN\0").encode(),
            Frame::new(b'Z', b"T").encode(),
        ]
        .concat(),
    )?;
    read_empty(&socket, b'S')?;
    socket.write_all(
        &[
            Frame::new(b'C', b"ROLLBACK\0").encode(),
            Frame::new(b'Z', b"I").encode(),
        ]
        .concat(),
    )?;
    socket.read_closed()
}

pub fn throttled(listener: &mut peer::TcpAcceptor<'_>) -> Result<(), Error> {
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    for (command, status) in [
        (b"SELECT 1\0".as_slice(), b"I"),
        (b"BEGIN\0".as_slice(), b"T"),
        (b"COMMIT\0".as_slice(), b"I"),
        (b"SELECT 1\0".as_slice(), b"I"),
    ] {
        read_empty(&socket, b'S')?;
        socket.write_all(
            &[
                Frame::new(b'C', command).encode(),
                Frame::new(b'Z', status).encode(),
            ]
            .concat(),
        )?;
    }
    socket.read_closed()
}

pub fn unfinished(listener: &mut peer::TcpAcceptor<'_>, rejected: bool) -> Result<(), Error> {
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    read_empty(&socket, b'S')?;
    socket.write_all(
        &[
            Frame::new(b'C', b"BEGIN\0").encode(),
            Frame::new(b'Z', b"T").encode(),
        ]
        .concat(),
    )?;
    read_empty(&socket, b'S')?;
    let response = if rejected {
        Frame::new(b'E', COMMAND_ERROR)
    } else {
        Frame::new(b'C', b"COMMIT\0")
    };
    socket.write_all(
        &[
            response.encode(),
            Frame::new(b'Z', if rejected { b"E" } else { b"T" }).encode(),
        ]
        .concat(),
    )?;
    socket.read_closed()?;
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    read_empty(&socket, b'S')?;
    socket.write_all(
        &[
            Frame::new(b'C', b"SELECT 1\0").encode(),
            Frame::new(b'Z', b"I").encode(),
        ]
        .concat(),
    )?;
    socket.read_closed()
}

pub fn ended(
    listener: &mut peer::TcpAcceptor<'_>,
    invalid_begin: bool,
    unmanaged_begin: bool,
) -> Result<(), Error> {
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    read_empty(&socket, b'S')?;
    read_empty(&socket, b'S')?;
    socket.write_all(
        &[
            Frame::new(
                b'C',
                if unmanaged_begin {
                    b"BEGIN\0"
                } else {
                    b"SELECT 1\0"
                },
            )
            .encode(),
            Frame::new(b'Z', if unmanaged_begin { b"T" } else { b"I" }).encode(),
            Frame::new(b'C', b"BEGIN\0").encode(),
            Frame::new(b'Z', if invalid_begin { b"I" } else { b"T" }).encode(),
        ]
        .concat(),
    )?;
    if !invalid_begin && !unmanaged_begin {
        read_empty(&socket, b'S')?;
        socket.write_all(
            &[
                Frame::new(b'C', b"ROLLBACK\0").encode(),
                Frame::new(b'Z', b"I").encode(),
            ]
            .concat(),
        )?;
    }
    socket.read_closed()?;
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    for _ in 0..2 {
        read_empty(&socket, b'S')?;
        socket.write_all(
            &[
                Frame::new(b'C', b"SELECT 1\0").encode(),
                Frame::new(b'Z', b"I").encode(),
            ]
            .concat(),
        )?;
    }
    socket.read_closed()
}
