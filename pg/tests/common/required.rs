use cartel_pg::{PgTable, pg_instance, query_group};
use dope_test::harness::{GUARD, peer};

use super::postgres::{
    Error, Frame, read_empty,
    startup::{self, Stream as _},
};

#[derive(PgTable)]
pub struct Item {
    pub id: i32,
    pub name: String,
}

#[query_group]
impl Item {
    pub fn rename(id: i32, name: &str) {
        Item::filter(|row| row.id == id)
            .update(|row| row.name = name)
            .required()
    }
}

pg_instance! { pub Database: Item }

#[derive(Clone, Copy)]
pub enum Case {
    Settle,
    Malformed,
    Empty,
}

impl Case {
    pub fn serve(self, socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
        socket.prepare()?;
        socket.read_wait(Some(GUARD))?;
        startup::read(&socket)?;
        socket.write_all(startup::AUTHENTICATED)?;
        read_empty(&socket, b'S')?;
        socket.write_all(
            &[
                Frame::new(b'1', b"").encode(),
                Frame::new(b't', b"\0\x02\0\0\0\x17\0\0\0\x19").encode(),
                Frame::new(b'n', b"").encode(),
                Frame::new(b'Z', b"I").encode(),
            ]
            .concat(),
        )?;

        if matches!(self, Self::Settle) {
            for (late, count) in [
                (false, b"1".as_slice()),
                (true, b"18446744073709551615"),
                (false, b"1"),
            ] {
                read_empty(&socket, b'S')?;
                socket.write_all(&Frame::new(b'C', b"UPDATE 0\0").encode())?;
                socket.write_all(&Frame::new(b'A', b"\0\0\0\x01required\0pending\0").encode())?;
                read_empty(&socket, b'S')?;
                if late {
                    socket
                        .write_all(&Frame::new(b'E', b"SERROR\0C23505\0Mlate sync\0\0").encode())?;
                }
                socket.write_all(startup::IDLE)?;
                socket
                    .write_all(&Frame::new(b'C', &[b"UPDATE ", count, b"\0"].concat()).encode())?;
                socket.write_all(startup::IDLE)?;
            }
            read_empty(&socket, b'S')?;
            socket.write_all(&Frame::new(b'C', b"UPDATE 0\0").encode())?;
            socket.write_all(startup::IDLE)?;
        } else {
            read_empty(&socket, b'S')?;
            let frame = match self {
                Self::Malformed => Frame::new(b'C', b"UPDATE 0x\0"),
                Self::Empty => Frame::new(b'I', b""),
                Self::Settle => unreachable!("case"),
            };
            socket.write_all(&frame.encode())?;
        }
        socket.read_closed()
    }
}
