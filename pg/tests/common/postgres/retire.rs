use std::{io::Write as _, net::TcpStream};

use crate::common::postgres::{
    Error, Frame, accept::DATA_ROW, group::COMMAND_ERROR, read_empty, startup::Stream as _,
};

pub fn reply(socket: &mut TcpStream, failed: bool) -> Result<(), Error> {
    read_empty(socket, b'S')?;
    if failed {
        socket.write_all(&DATA_ROW.encode())?;
        socket.write_all(&Frame::new(b'E', COMMAND_ERROR).encode())?;
    } else {
        socket.write_all(&Frame::new(b'C', b"SELECT 1\0").encode())?;
    }
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    Ok(())
}

pub fn finalize(socket: &mut TcpStream) -> Result<(), Error> {
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
