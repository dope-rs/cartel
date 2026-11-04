use dope::core::platform::wake::Wait;
use dope_test::harness::{GUARD, peer};

use crate::common::postgres::{
    Error, Frame, group::COMMAND_ERROR, read_empty, startup::Stream as _,
};

pub fn throttled(listener: &mut peer::TcpAcceptor<'_>, release: Wait) -> Result<(), Error> {
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    for (index, (command, status)) in [
        (b"SELECT 1\0".as_slice(), b"I"),
        (b"BEGIN\0".as_slice(), b"T"),
        (b"COMMIT\0".as_slice(), b"I"),
        (b"SELECT 1\0".as_slice(), b"I"),
    ]
    .into_iter()
    .enumerate()
    {
        read_empty(&socket, b'S')?;
        if index == 0 {
            release.wait_timeout(GUARD).expect("release first reply");
        }
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

pub fn ended(listener: &mut peer::TcpAcceptor<'_>, invalid_begin: bool) -> Result<(), Error> {
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    socket.read_wait(Some(GUARD))?;
    read_empty(&socket, b'S')?;
    read_empty(&socket, b'S')?;
    socket.write_all(
        &[
            Frame::new(b'C', b"SELECT 1\0").encode(),
            Frame::new(b'Z', b"I").encode(),
            Frame::new(b'C', b"BEGIN\0").encode(),
            Frame::new(b'Z', if invalid_begin { b"I" } else { b"T" }).encode(),
        ]
        .concat(),
    )?;
    if !invalid_begin {
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
