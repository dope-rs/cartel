use std::time::{Duration, Instant};

use dope_test::harness::{
    GUARD,
    peer::{self, Stream},
};

#[derive(Debug)]
pub enum Error {
    Peer(peer::Error),
    Request([u8; PING.len()]),
}

impl From<peer::Error> for Error {
    fn from(error: peer::Error) -> Self {
        Self::Peer(error)
    }
}

pub const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

pub fn pong_once(socket: &Stream<'_>) -> Result<(), Error> {
    socket.read_wait(Some(Duration::from_secs(1)))?;
    let mut buf = [0_u8; PING.len()];
    socket.read_exact(&mut buf)?;
    if buf != PING {
        return Err(Error::Request(buf));
    }
    socket.write_all(b"+PONG\r\n").map_err(Error::Peer)
}

pub fn first_request(
    sockets: &mut [&Stream<'_>; 2],
    expected: &[u8],
    request: &mut [u8],
) -> Result<usize, peer::Error> {
    for socket in sockets.iter() {
        socket.read_wait(Some(Duration::from_millis(1)))?;
    }
    let started = Instant::now();
    loop {
        for (index, socket) in sockets.iter_mut().enumerate() {
            match socket.read(&mut request[..1]) {
                Ok(0) => return Err(peer::Error::UnexpectedEof),
                Ok(_) => {
                    socket.read_wait(Some(GUARD))?;
                    socket.read_exact(&mut request[1..])?;
                    assert_eq!(request, expected);
                    return Ok(index);
                }
                Err(peer::Error::TimedOut) => {}
                Err(error) => return Err(error),
            }
        }
        if started.elapsed() >= GUARD {
            return Err(peer::Error::TimedOut);
        }
    }
}
