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

pub const CATALOG_READY: &[u8] = b"1\0\0\0\x04t\0\0\0\x06\0\0\
    T\0\0\0\x2c\0\x02\
    \0\0\0\0\0\0\0\0\0\0\x14\xff\xff\xff\xff\xff\xff\0\0\
    \0\0\0\0\0\0\0\0\0\0\x19\xff\xff\xff\xff\xff\xff\0\0\
    1\0\0\0\x04t\0\0\0\x06\0\0T\0\0\0\x65\0\x05\
    \0\0\0\0\0\0\0\0\0\0\x14\xff\xff\xff\xff\xff\xff\0\0\
    \0\0\0\0\0\0\0\0\0\0\x17\xff\xff\xff\xff\xff\xff\0\0\
    \0\0\0\0\0\0\0\0\0\0\x10\xff\xff\xff\xff\xff\xff\0\0\
    \0\0\0\0\0\0\0\0\0\x0e\xda\xff\xff\xff\xff\xff\xff\0\0\
    \0\0\0\0\0\0\0\0\0\0\x14\xff\xff\xff\xff\xff\xff\0\0\
    1\0\0\0\x04t\0\0\0\x06\0\0T\0\0\0\x19\0\x01\
    \0\0\0\0\0\0\0\0\0\x03\xf1\xff\xff\xff\xff\xff\xff\0\0\
    1\0\0\0\x04t\0\0\0\x06\0\0T\0\0\0\x19\0\x01\
    \0\0\0\0\0\0\0\0\0\x03\xf1\xff\xff\xff\xff\xff\xff\0\0\
    Z\0\0\0\x05I";
pub const ROWS_COMPLETED: &[u8] = b"2\0\0\0\x04\
    D\0\0\0\x1b\0\x02\0\0\0\x08\0\0\0\0\0\0\0\x03\0\0\0\x05first\
    C\0\0\0\x0dSELECT 1\0\
    2\0\0\0\x04\
    D\0\0\0\x1c\0\x02\0\0\0\x08\0\0\0\0\0\0\0\x07\0\0\0\x06second\
    C\0\0\0\x0dSELECT 1\0\
    A\0\0\0\x17\0\0\0\x01group\0complete\0";
pub const NEXT_ROW: &[u8] = b"2\0\0\0\x04\
    D\0\0\0\x1b\0\x02\0\0\0\x08\0\0\0\0\0\0\0\x0b\0\0\0\x05third\
    C\0\0\0\x0dSELECT 1\0Z\0\0\0\x05I";

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
    read_empty(socket, b'S')?;
    complete(socket, 11, b"next")?;
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    socket.read_closed()
}
