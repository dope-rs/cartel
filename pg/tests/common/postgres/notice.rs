use crate::common::postgres::{Error, Frame, read_empty};
use std::{io, ops::Range};

pub const COMMAND: &str = "SELECT 1";
pub const SEED: [(u32, &str, &str); 3] = [(23, "é", "🦀"), (37, "", "body"), (41, "", "")];
type Batch = (
    &'static [(u32, &'static str, &'static str)],
    Range<usize>,
    u64,
);

pub const BATCHES: [Batch; 2] = [
    (
        &[
            (51, "", "aa"),
            (52, "", "bb"),
            (53, "", "cc"),
            (54, "", "dd"),
            (55, "", "01234567890"),
        ],
        1..4,
        2,
    ),
    (
        &[(61, "", "aaaa"), (62, "", "bbbbb"), (63, "", "ccccccc")],
        2..3,
        4,
    ),
];
pub const BUDGET: [(u32, &str, &str); 1] = [(73, "", "0123456789")];

pub fn try_write(socket: &mut impl io::Write, cases: &[(u32, &str, &str)]) -> io::Result<()> {
    for &(pid, channel, payload) in cases {
        let mut body = Vec::from(pid.to_be_bytes());
        body.extend_from_slice(channel.as_bytes());
        body.push(0);
        body.extend_from_slice(payload.as_bytes());
        body.push(0);
        socket.write_all(&Frame::new(b'A', &body).encode())?;
    }
    Ok(())
}

pub fn try_batch(
    socket: &mut (impl io::Read + io::Write),
    cases: &[(u32, &str, &str)],
) -> Result<(), Error> {
    read_empty(socket, b'S')?;
    socket.write_all(&Frame::new(b'1', &[]).encode())?;
    socket.write_all(&Frame::new(b'2', &[]).encode())?;
    try_write(socket, cases)?;
    socket.write_all(&Frame::new(b'C', b"SELECT 1\0").encode())?;
    socket.write_all(&Frame::new(b'Z', b"I").encode())?;
    Ok(())
}
