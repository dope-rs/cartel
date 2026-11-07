use crate::common::postgres::{Error, Frame, startup};
use cartel_pg::{
    PgRow, port::config, protocol::encode, protocol::value::encode::BindWriter, schema,
    schema::groups, schema::result,
};
use dope_test::harness::peer;
use std::cell::Cell;
use std::iter;

#[derive(PgRow)]
#[row(view)]
pub struct TextRow {
    pub lease: i64,
    pub name: String,
}

#[derive(PgRow)]
#[row(view)]
pub struct MixedRow {
    pub low: i64,
    pub middle: i32,
    pub active: bool,
    pub tags: cartel_pg::Jsonb,
    pub high: i64,
}

pub struct TextQuery;

pub struct MixedQuery;

pub struct ArrayQuery<const SCALAR: bool = false>;

pub struct Group;

pub struct Database;

impl schema::TypedQuery for TextQuery {
    type Params<'p> = ();
    type Row = TextRow;
    type Result = result::Rows;
    type Group = Group;
    const STATEMENT_NAME: &str = "text";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 2;
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::TypedQuery for MixedQuery {
    type Params<'p> = ();
    type Row = MixedRow;
    type Result = result::Rows;
    type Group = Group;
    const STATEMENT_NAME: &str = "mixed";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 5;
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl<const SCALAR: bool> schema::TypedQuery for ArrayQuery<SCALAR> {
    type Params<'p> = ();
    type Row = Vec<String>;
    type Result = result::Rows<SCALAR>;
    type Group = Group;
    const STATEMENT_NAME: &str = if SCALAR { "scalararray" } else { "namearray" };
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 1;
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::QueryGroup for Group {
    type Items = groups::Item<
        TextQuery,
        groups::Item<
            MixedQuery,
            groups::Item<ArrayQuery, groups::Item<ArrayQuery<true>, groups::Empty>>,
        >,
    >;
    const QUERIES: &[schema::QueryMeta<'_>] = &[
        schema::QueryMeta::fixed::<TextQuery>(),
        schema::QueryMeta::fixed::<MixedQuery>(),
        schema::QueryMeta::fixed::<ArrayQuery>(),
        schema::QueryMeta::fixed::<ArrayQuery<true>>(),
    ];
}

impl schema::QuerySet for Database {
    type Groups = groups::Cons<Group, groups::End>;
    const QUERIES: &[&[schema::QueryMeta<'_>]] = &[<Group as schema::QueryGroup>::QUERIES];
    const PREPARE: schema::Prepare<'_> = PREPARED.prepare();
}

impl schema::HasGroup<Group> for Database {}

const SIZE: usize = schema::Prepare::fixed_len::<Database>();

const PREPARED: schema::Wire<'_, SIZE> = schema::Prepare::fixed::<Database, SIZE>();

pub struct Command<const OTHER: bool>;

pub struct CommandGroup;

pub struct Mismatch;

impl<const OTHER: bool> schema::TypedQuery for Command<OTHER> {
    type Params<'p> = ();
    type Row = ();
    type Result = result::Command;
    type Group = CommandGroup;
    const STATEMENT_NAME: &str = "command";
    const SQL: &str = if OTHER {
        "SET application_name='other'"
    } else {
        "SET application_name='expected'"
    };
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 0;
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::QueryGroup for CommandGroup {
    type Items = groups::Item<Command<false>, groups::Empty>;
    const QUERIES: &[schema::QueryMeta<'_>] = &[schema::QueryMeta::fixed::<Command<true>>()];
}

impl schema::QuerySet for Mismatch {
    type Groups = groups::Cons<CommandGroup, groups::End>;
    const QUERIES: &[&[schema::QueryMeta<'_>]] = &[<CommandGroup as schema::QueryGroup>::QUERIES];
    const PREPARE: schema::Prepare<'_> = {
        const SIZE: usize = schema::Prepare::fixed_len::<Mismatch>();
        const WIRE: schema::Wire<'_, SIZE> = schema::Prepare::fixed::<Mismatch, SIZE>();
        WIRE.prepare()
    };
}

impl schema::HasGroup<CommandGroup> for Mismatch {}

impl Mismatch {
    pub fn serve(socket: &mut &peer::Stream<'_>) -> Result<(), Error> {
        use startup::Stream as _;

        socket.prepare()?;
        startup::read(socket)?;
        socket.write_all(&Frame::new(b'R', &0u32.to_be_bytes()).encode())?;
        socket.write_all(&Frame::new(b'Z', b"I").encode())?;
        let (tag, _) = match try_frame(socket) {
            Ok(frame) => frame,
            Err(Error::Io(peer::Error::UnexpectedEof)) => return Ok(()),
            Err(Error::Io(peer::Error::Syscall(error))) if error.is_reset() => return Ok(()),
            Err(error) => return Err(error),
        };
        if tag != b'P' {
            return Err(Error::UnexpectedRequest);
        }
        socket.write_all(&Frame::new(b'1', &[]).encode())?;
        if try_frame(socket)?.0 != b'D' {
            return Err(Error::UnexpectedRequest);
        }
        socket.write_all(&Frame::new(b't', &0u16.to_be_bytes()).encode())?;
        socket.write_all(&Frame::new(b'n', &[]).encode())?;
        if try_frame(socket)?.0 != b'S' {
            return Err(Error::UnexpectedRequest);
        }
        socket.write_all(&Frame::new(b'Z', b"I").encode())?;
        socket.read_closed()
    }
}

impl TextRow {
    pub fn wire(lease: i64, name: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2i16.to_be_bytes());
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&lease.to_be_bytes());
        bytes.extend_from_slice(&(name.len() as i32).to_be_bytes());
        bytes.extend_from_slice(name);
        bytes
    }
}

impl MixedRow {
    pub fn wire(low: i64, middle: i32, active: u8, tags: &[u8], high: i64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&5i16.to_be_bytes());
        for value in [
            low.to_be_bytes().as_slice(),
            middle.to_be_bytes().as_slice(),
            &[active],
            tags,
            high.to_be_bytes().as_slice(),
        ] {
            bytes.extend_from_slice(&(value.len() as i32).to_be_bytes());
            bytes.extend_from_slice(value);
        }
        bytes
    }
}

impl ArrayQuery {
    pub const NULL: &[u8] = b"\0\x01\xff\xff\xff\xff";

    pub fn wire(values: &[&[u8]], trailing: &[u8]) -> Vec<u8> {
        let length =
            20 + values.iter().map(|value| 4 + value.len()).sum::<usize>() + trailing.len();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(&(length as i32).to_be_bytes());
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&25u32.to_be_bytes());
        bytes.extend_from_slice(&(values.len() as i32).to_be_bytes());
        bytes.extend_from_slice(&1i32.to_be_bytes());
        for value in values {
            bytes.extend_from_slice(&(value.len() as i32).to_be_bytes());
            bytes.extend_from_slice(value);
        }
        bytes.extend_from_slice(trailing);
        bytes
    }
}

pub fn description(oids: &[u32]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(oids.len() as u16).to_be_bytes());
    for oid in oids {
        bytes.push(0);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        bytes.extend_from_slice(&oid.to_be_bytes());
        bytes.extend_from_slice(&(-1i16).to_be_bytes());
        bytes.extend_from_slice(&(-1i32).to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
    }
    bytes
}

fn try_frame(socket: &peer::Stream<'_>) -> Result<(u8, Vec<u8>), Error> {
    let mut tag = [0];
    socket.read_exact(&mut tag)?;
    Ok((tag[0], startup::read(socket)?))
}

impl Database {
    pub fn serve_rows<'a>(
        socket: &peer::Stream<'_>,
        rows: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<(), Error> {
        Self::serve(socket, rows.into_iter().map(iter::once))
    }

    pub fn serve_batch<'a>(
        socket: &peer::Stream<'_>,
        rows: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<(), Error> {
        Self::serve(socket, [rows])
    }

    pub fn serve_timer_queries(socket: &peer::Stream<'_>, row: &[u8]) -> Result<(), Error> {
        Self::serve(
            socket,
            (0..4).map(|index| {
                iter::repeat_n(row, if matches!(index, 1 | 2) { 9 } else { 1 }).inspect(move |_| {
                    if index == 3 {
                        socket
                            .write_all(&Frame::new(b'A', b"\0\0\0\x01next\0ready\0").encode())
                            .expect("fourth query notification");
                    }
                })
            }),
        )?;
        crate::common::postgres::read_empty(socket, b'S')?;
        socket.write_all(&Frame::new(b'2', &[]).encode())?;
        for _ in 0..9 {
            socket.write_all(&Frame::new(b'D', row).encode())?;
        }
        socket.write_all(&Frame::new(b'C', b"SELECT 9\0").encode())?;
        socket
            .write_all(&Frame::new(b'E', crate::common::postgres::group::COMMAND_ERROR).encode())?;
        socket.write_all(&Frame::new(b'Z', b"I").encode())?;
        Ok(())
    }

    pub(in crate::common) fn start(socket: &peer::Stream<'_>) -> Result<(), Error> {
        startup::read(socket)?;
        socket.write_all(&Frame::new(b'R', &0u32.to_be_bytes()).encode())?;
        socket.write_all(&Frame::new(b'Z', b"I").encode())?;
        loop {
            let (tag, body) = try_frame(socket)?;
            match tag {
                b'P' => socket.write_all(&Frame::new(b'1', &[]).encode())?,
                b'D' => {
                    if body.get(1..) == Some(b"bind\0") {
                        socket.write_all(
                            &Frame::new(b't', b"\0\x03\0\0\0\x17\0\0\0\x19\0\0\x03\xf1").encode(),
                        )?;
                        socket.write_all(&Frame::new(b'n', &[]).encode())?;
                        continue;
                    }
                    let oids: &[u32] = if body.get(1..) == Some(b"text\0") {
                        &[20, 25]
                    } else if body.get(1..) == Some(b"namearray\0")
                        || body.get(1..) == Some(b"scalararray\0")
                    {
                        &[1009]
                    } else {
                        &[20, 23, 16, 3802, 20]
                    };
                    socket.write_all(&Frame::new(b't', &0u16.to_be_bytes()).encode())?;
                    socket.write_all(&Frame::new(b'T', &description(oids)).encode())?;
                }
                b'S' => break,
                _ => return Err(Error::UnexpectedRequest),
            }
        }
        socket.write_all(&Frame::new(b'Z', b"I").encode())?;
        Ok(())
    }

    pub(crate) fn serve<'a, R: IntoIterator<Item = &'a [u8]>>(
        socket: &peer::Stream<'_>,
        queries: impl IntoIterator<Item = R>,
    ) -> Result<(), Error> {
        Self::start(socket)?;
        for rows in queries {
            loop {
                let (tag, _) = try_frame(socket)?;
                match tag {
                    b'B' | b'E' => {}
                    b'S' => break,
                    _ => return Err(Error::UnexpectedRequest),
                }
            }
            socket.write_all(&Frame::new(b'2', &[]).encode())?;
            let mut count = 0;
            for bytes in rows {
                socket.write_all(&Frame::new(b'D', bytes).encode())?;
                count += 1;
            }
            socket.write_all(&Frame::new(b'C', format!("SELECT {count}\0").as_bytes()).encode())?;
            socket.write_all(&Frame::new(b'Z', b"I").encode())?;
        }
        Ok(())
    }

    pub fn config() -> Result<config::Config, config::Error> {
        config::Config::new(config::Capacities {
            connections: 1,
            inflight: 1,
            admission_waiters: 1,
            requests: config::Request {
                frames: 1,
                frame_bytes: 512,
            },
            responses: config::Response {
                items: 8,
                bytes: 4096,
                frame_bytes: 4096,
            },
            notifications: config::Notification {
                items: 1,
                bytes: 64,
            },
        })
    }
}

const OVERSIZED: &str = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

#[derive(Clone, Copy)]
pub enum BindCase {
    MissingText,
    MissingArray,
    Retry,
}

pub struct Bind;

impl schema::TypedQuery for Bind {
    type Params<'p> = (BindCase, &'p [Cell<Option<encode::Error>>; 2]);
    type Row = ();
    type Result = result::Command;
    type Group = Self;
    const STATEMENT_NAME: &str = "bind";
    const SQL: &str = "INSERT INTO bindings VALUES ($1, $2, $3)";
    const PARAM_OIDS: &[u32] = &[23, 25, 1009];
    const N_PARAMS: i16 = 3;
    const N_RESULT_COLS: u16 = 0;

    fn encode(
        (case, observed): Self::Params<'_>,
        w: &mut BindWriter<'_, '_>,
    ) -> Result<(), encode::Error> {
        w.scalar().write_i32(7)?;
        if !matches!(case, BindCase::MissingArray) {
            observed[0].set(w.data().write_text(OVERSIZED).err());
            if matches!(case, BindCase::MissingText) {
                return Ok(());
            }
        }
        w.data().write_text("ok")?;
        observed[1].set(w.array().write_text(&["kept", OVERSIZED]).err());
        if matches!(case, BindCase::Retry) {
            w.array().write_text(&["x"])?;
        }
        Ok(())
    }
}

impl schema::QueryGroup for Bind {
    type Items = groups::Item<Self, groups::Empty>;
    const QUERIES: &[schema::QueryMeta<'_>] = &[schema::QueryMeta::fixed::<Self>()];
}

impl schema::QuerySet for Bind {
    type Groups = groups::Cons<Self, groups::End>;
    const QUERIES: &[&[schema::QueryMeta<'_>]] = &[<Self as schema::QueryGroup>::QUERIES];
    const PREPARE: schema::Prepare<'_> = {
        const SIZE: usize = schema::Prepare::fixed_len::<Bind>();
        const WIRE: schema::Wire<'_, SIZE> = schema::Prepare::fixed::<Bind, SIZE>();
        WIRE.prepare()
    };
}

impl schema::HasGroup<Bind> for Bind {}

impl Bind {
    pub const BODY: &[u8] = b"\0bind\0\0\x01\0\x01\0\x03\
        \0\0\0\x04\0\0\0\x07\0\0\0\x02ok\
        \0\0\0\x19\0\0\0\x01\0\0\0\0\0\0\0\x19\
        \0\0\0\x01\0\0\0\x01\0\0\0\x01x\0\x01\0\x01";

    pub fn serve(socket: &mut &peer::Stream<'_>) -> Result<Vec<Vec<u8>>, Error> {
        Database::start(&mut *socket)?;
        let mut received = Vec::new();
        loop {
            let mut tag = [0];
            if socket.read(&mut tag)? == 0 {
                return Ok(received);
            }
            let body = startup::read(socket)?;
            let tag = tag[0];
            if tag != b'B' {
                return Err(Error::UnexpectedRequest);
            }
            received.push(body);
            for (expected, bytes) in [(b'E', &b"\0\0\0\0\0"[..]), (b'S', &b""[..])] {
                let (tag, body) = try_frame(socket)?;
                if tag != expected || body != bytes {
                    return Err(Error::UnexpectedRequest);
                }
            }
            socket.write_all(
                &[
                    Frame::new(b'2', &[]).encode(),
                    Frame::new(b'C', b"INSERT 0 1\0").encode(),
                    Frame::new(b'Z', b"I").encode(),
                ]
                .concat(),
            )?;
        }
    }
}
