use std::net::Shutdown;

use cartel_pg::api::results::group::Position;
use dope_test::harness::peer;

use crate::common::{
    postgres::{Error, Frame, read_empty, startup::Stream as _},
    wire::checked::{Database, TextRow},
};

pub const COMMAND_ERROR: &[u8] = b"SERROR\0VERROR\0C23505\0Mduplicate\0\0";
const RECOVERY_ERROR: &[u8] = b"SERROR\0VERROR\0CXX000\0Mrecovery\0\0";

pub const SCOPES: [(&[u8], bool, Position); 3] = [
    (
        b"SERROR\0VFATAL\0C57P01\0Mconnection\0\0",
        false,
        Position::Connection,
    ),
    (b"SFEHLER\0CXX000\0Mlocalized\0\0", false, Position::Unknown),
    (
        b"SFEHLER\0CXX000\0Mlocalized\0\0",
        true,
        Position::Command(0),
    ),
];

fn complete(socket: &peer::Stream<'_>, id: i64, name: &[u8]) -> Result<(), peer::Error> {
    socket.write_all(&Frame::new(b'2', &[]).encode())?;
    socket.write_all(&Frame::new(b'D', &TextRow::wire(id, name)).encode())?;
    socket.write_all(&Frame::new(b'C', b"SELECT 1\0").encode())
}

pub fn delay_ready(socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
    Database::start(&mut *socket)?;
    read_empty(socket, b'S')?;
    complete(socket, 3, b"first")?;
    complete(socket, 7, b"second")?;
    socket
        .write_all(&Frame::new(b'A', b"\0\0\0\x01group\0complete\0").encode())
        .map_err(Error::Io)
}

pub fn resume_ready(socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    read_empty(socket, b'S')?;
    complete(socket, 11, b"third")?;
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    socket.read_closed()
}

pub fn transaction(socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
    Database::start(&mut *socket)?;
    read_empty(socket, b'S')?;
    socket.write_all(&Frame::new(b'C', b"BEGIN\0").encode())?;
    socket.write_all(&Frame::new(b'Z', b"T").encode())?;
    read_empty(socket, b'S')?;
    complete(socket, 3, b"retained")?;
    socket.write_all(&Frame::new(b'E', COMMAND_ERROR).encode())?;
    socket.write_all(&Frame::new(b'E', RECOVERY_ERROR).encode())?;
    socket.write_all(&Frame::new(b'Z', b"E").encode())?;
    read_empty(socket, b'S')?;
    socket.write_all(&Frame::new(b'C', b"ROLLBACK\0").encode())?;
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    socket.read_closed()
}

pub fn late_error(socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
    Database::start(&mut *socket)?;
    read_empty(socket, b'S')?;
    complete(socket, 3, b"first")?;
    complete(socket, 7, b"second")?;
    socket.write_all(&Frame::new(b'E', COMMAND_ERROR).encode())?;
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    socket.read_closed()
}

pub fn classified(socket: &mut &peer::Stream<'_>, detail: &[u8], ready: bool) -> Result<(), Error> {
    Database::start(&mut *socket)?;
    read_empty(socket, b'S')?;
    socket.write_all(&Frame::new(b'E', detail).encode())?;
    if ready {
        socket.write_all(&Frame::new(b'Z', b"I").encode())?;
        socket.read_closed()
    } else {
        socket.shutdown(Shutdown::Both).map_err(Error::Io)
    }
}

pub fn overflow(socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
    Database::start(&mut *socket)?;
    read_empty(socket, b'S')?;
    socket.write_all(&Frame::new(b'E', COMMAND_ERROR).encode())?;
    socket.write_all(&Frame::new(b'E', RECOVERY_ERROR).encode())?;
    socket.read_closed()
}

pub fn settled(socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
    Database::start(&mut *socket)?;
    read_empty(socket, b'S')?;
    complete(socket, 3, b"retained")?;
    socket.write_all(&Frame::new(b'E', COMMAND_ERROR).encode())?;
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    socket.write_all(&Frame::new(b'A', b"\0\0\0\x01group\0settled\0").encode())?;
    socket.read_closed()
}

pub fn missing(socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
    Database::start(&mut *socket)?;
    read_empty(socket, b'S')?;
    complete(socket, 3, b"first")?;
    socket.write_all(&Frame::new(b'2', &[]).encode())?;
    socket.write_all(&Frame::new(b'C', b"SELECT 0\0").encode())?;
    complete(socket, 7, b"last")?;
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    socket.read_closed()
}
