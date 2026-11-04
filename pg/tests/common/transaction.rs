use dope_test::harness::{GUARD, peer};

use crate::common::postgres::{Error, Frame, read_empty, startup::Stream as _};

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
