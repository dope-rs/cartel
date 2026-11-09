use crate::common::postgres::{Error, Frame, SettleError, read_empty, startup::Stream as _};
use crate::common::wire::{
    checked::{Database, TextRow},
    queries::{DIAGNOSTIC, DUPLICATE},
};
use cartel_pg::protocol::{Number, Reason};
use dope_test::harness::peer;
use std::time::{Duration, Instant};

pub const DATA_ROW: Frame<'static> = Frame::new(b'D', b"\0\x01\0\0\0\x011");

#[derive(Debug)]
pub enum Rejection<'a> {
    Protocol(Reason),
    Debug(&'a str),
}

pub const REPLIES: [(Frame<'static>, Option<Rejection<'static>>); 14] = [
    (Frame::new(b'E', DIAGNOSTIC), None),
    (
        Frame::new(b'E', DIAGNOSTIC.split_at(b"Slocalized\0".len()).1),
        None,
    ),
    (
        Frame::new(b'E', b"Cinvalid\0\0"),
        Some(Rejection::Protocol(Reason::DiagnosticSeverity)),
    ),
    (
        Frame::new(b'E', b"V\0SERROR\0Cinvalid\0\0"),
        Some(Rejection::Protocol(Reason::DiagnosticSeverity)),
    ),
    (
        Frame::new(b'E', b"SERROR\0\0"),
        Some(Rejection::Debug(
            "DiagnosticSqlstateLength { actual: Length(0) }",
        )),
    ),
    (
        Frame::new(b'E', b"SERROR\0C23505\0\0"),
        Some(Rejection::Protocol(Reason::DiagnosticMessage)),
    ),
    (
        Frame::new(b'E', b"SERROR\0C23505\0Mduplicate\0P1\0P\0\0"),
        Some(Rejection::Protocol(Reason::DuplicateDiagnostic {
            kind: b'P',
        })),
    ),
    (
        Frame::new(b'E', DUPLICATE),
        Some(Rejection::Protocol(Reason::DuplicateDiagnostic {
            kind: b'C',
        })),
    ),
    (DATA_ROW, Some(Rejection::Protocol(Reason::CommandData))),
    (
        Frame::new(b'E', b"SERROR\0C23505\0Mempty\0P\0\0"),
        Some(Rejection::Protocol(Reason::DiagnosticPosition {
            kind: b'P',
            error: Number::Empty,
        })),
    ),
    (
        Frame::new(b'E', b"SERROR\0C23505\0Mzero\0p0\0\0"),
        Some(Rejection::Protocol(Reason::DiagnosticPosition {
            kind: b'p',
            error: Number::Zero,
        })),
    ),
    (
        Frame::new(b'E', b"SERROR\0C23505\0Mdigit\0L12x\0\0"),
        Some(Rejection::Debug(
            "DiagnosticPosition { kind: 76, error: Invalid { offset: Offset(2), byte: 120 } }",
        )),
    ),
    (
        Frame::new(b'E', b"SERROR\0C23505\0Maddition\0P4294967296\0\0"),
        Some(Rejection::Debug(
            "DiagnosticPosition { kind: 80, error: Overflow { offset: Offset(9), byte: 54 } }",
        )),
    ),
    (
        Frame::new(b'E', b"SERROR\0C23505\0Mmultiplication\0p4294967300\0\0"),
        Some(Rejection::Debug(
            "DiagnosticPosition { kind: 112, error: Overflow { offset: Offset(9), byte: 48 } }",
        )),
    ),
];

pub fn reply(socket: &mut &peer::Stream<'_>, frame: Frame<'_>) -> Result<(), Error> {
    socket.prepare()?;
    Database::start(socket)?;
    read_empty(socket, b'S')?;
    socket.write_all(&[frame.encode(), Frame::new(b'Z', b"I").encode()].concat())?;
    socket.read_closed()
}

pub fn copy_finish(listener: &mut peer::TcpAcceptor<'_>) -> Result<(), Error> {
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    read_empty(&socket, b'd')?;
    socket.write_all(
        &[
            Frame::new(b'1', &[]).encode(),
            Frame::new(b'2', &[]).encode(),
            Frame::new(b'G', b"\0\0\0").encode(),
            Frame::new(b'A', b"\0\0\0\x01copy\0ready\0").encode(),
        ]
        .concat(),
    )?;
    let mut finish = [0; 10];
    socket.read_exact(&mut finish)?;
    if &finish != b"c\0\0\0\x04S\0\0\0\x04" {
        return Err(Error::UnexpectedRequest);
    }
    socket.write_all(
        &[
            Frame::new(b'C', b"COPY 0\0").encode(),
            Frame::new(b'Z', b"I").encode(),
        ]
        .concat(),
    )?;
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

pub fn copy_out_done(listener: &mut peer::TcpAcceptor<'_>, extra: Option<u8>) -> Result<(), Error> {
    let mut socket = listener.accept()?;
    socket.try_start(None)?;
    read_empty(&socket, b'S')?;
    let mut reply = [
        Frame::new(b'1', &[]).encode(),
        Frame::new(b'2', &[]).encode(),
        Frame::new(b'H', b"\0\0\0").encode(),
        Frame::new(b'c', &[]).encode(),
    ]
    .concat();
    if let Some(tag) = extra {
        reply.extend(Frame::new(tag, if tag == b'd' { b"x" } else { b"" }).encode());
    }
    reply.extend(Frame::new(b'C', b"COPY 0\0").encode());
    reply.extend(Frame::new(b'Z', b"I").encode());
    socket.write_all(&reply)?;
    socket.read_closed()
}

pub fn queued_cancel_reuse(listener: &mut peer::TcpAcceptor<'_>) -> Result<(), Error> {
    const PID: i32 = 0x1020_3040;
    const SECRET: i32 = 0x5060_7080;

    let mut primary = listener.accept_within(Duration::from_secs(1))?;
    primary.try_start(Some((PID, SECRET)))?;

    read_empty(&primary, b'S')?;
    primary.write_all(&Frame::new(b'C', b"BEGIN\0").encode())?;
    primary.write_all(&Frame::new(b'Z', b"T").encode())?;

    {
        let cancel = listener.accept_within(Duration::from_secs(1))?;
        let mut frame = [0_u8; 16];
        cancel.read_exact(&mut frame)?;
        let expected = [16, 80_877_102, PID, SECRET].map(i32::to_be_bytes);
        if frame.as_slice() != expected.as_flattened() {
            return Err(Error::UnexpectedRequest);
        }
    }

    read_empty(&primary, b'S')?;
    primary.write_all(&Frame::new(b'C', b"ROLLBACK\0").encode())?;
    primary.write_all(&Frame::new(b'Z', b"I").encode())?;

    primary.read_closed()?;
    if listener.try_accept()?.is_some() {
        return Err(Error::UnexpectedRequest);
    }
    Ok(())
}

pub fn fatal(
    socket: &mut &peer::Stream<'_>,
    detail: &[u8],
    buffered: bool,
) -> Result<(), SettleError> {
    socket
        .prepare()
        .map_err(Error::Io)
        .map_err(SettleError::Startup)?;
    Database::start(socket).map_err(SettleError::Startup)?;
    read_empty(socket, b'S').map_err(SettleError::Command)?;
    if buffered {
        socket
            .write_all(&Frame::new(b'D', &TextRow::wire(1, b"retained")).encode())
            .map_err(Error::Io)
            .map_err(SettleError::Command)?;
    }
    socket
        .write_all(&Frame::new(b'E', detail).encode())
        .map_err(Error::Io)
        .map_err(SettleError::Command)?;
    socket.read_closed().map_err(SettleError::Close)
}

pub trait Listener<'a> {
    fn accept_within(&self, timeout: Duration) -> Result<peer::Stream<'a>, peer::Error>;
    fn group_settle(&self) -> Result<(), SettleError>;
    fn reply_settle(&self) -> Result<(), Error>;
    fn finalizer(&self) -> Result<(), Error>;
    fn copy_recover(&self) -> Result<(), Error>;
    fn recovery(&self, idle: bool, held: bool) -> Result<(), Error>;
    fn least_loaded(&self, initial: usize) -> Result<([peer::Stream<'a>; 2], usize), Error>;
}

impl<'a> Listener<'a> for peer::TcpAcceptor<'a> {
    fn accept_within(&self, timeout: Duration) -> Result<peer::Stream<'a>, peer::Error> {
        self.accept_for(timeout)?.ok_or(peer::Error::TimedOut)
    }

    fn group_settle(&self) -> Result<(), SettleError> {
        let stream = self
            .accept_within(Duration::from_secs(1))
            .map_err(SettleError::Connect)?;
        stream
            .read_wait(Some(Duration::from_secs(1)))
            .map_err(Error::Io)
            .map_err(SettleError::Startup)?;
        Database::start(&stream).map_err(SettleError::Startup)?;
        read_empty(&stream, b'S').map_err(SettleError::Begin)?;
        stream
            .write_all(
                &[
                    Frame::new(b'C', b"BEGIN\0").encode(),
                    Frame::new(b'Z', b"T").encode(),
                ]
                .concat(),
            )
            .map_err(Error::Io)
            .map_err(SettleError::Begin)?;

        read_empty(&stream, b'S').map_err(SettleError::Group)?;
        stream
            .write_all(
                &[
                    Frame::new(b'E', b"SERROR\0VERROR\0C23505\0Mgroup failure\0\0").encode(),
                    Frame::new(b'Z', b"E").encode(),
                ]
                .concat(),
            )
            .map_err(Error::Io)
            .map_err(SettleError::Group)?;
        stream.read_closed().map_err(SettleError::Close)?;

        let stream = self
            .accept_within(Duration::from_secs(1))
            .map_err(SettleError::Reconnect)?;
        stream
            .read_wait(Some(Duration::from_secs(1)))
            .map_err(Error::Io)
            .map_err(SettleError::Restart)?;
        Database::start(&stream).map_err(SettleError::Restart)?;
        read_empty(&stream, b'S').map_err(SettleError::Command)?;
        stream
            .write_all(
                &[
                    Frame::new(b'C', b"SELECT 1\0").encode(),
                    Frame::new(b'Z', b"I").encode(),
                ]
                .concat(),
            )
            .map_err(Error::Io)
            .map_err(SettleError::Command)?;
        stream.read_closed().map_err(SettleError::Stop)
    }

    fn reply_settle(&self) -> Result<(), Error> {
        let mut stream = self.accept_within(Duration::from_secs(1))?;
        stream.try_start(None)?;

        read_empty(&stream, b'S')?;
        let reply = [
            Frame::new(b'C', b"SELECT 1\0").encode(),
            Frame::new(b'A', b"\0\0\0\x01settlement\0complete\0").encode(),
        ]
        .concat();
        stream.write_all(&reply)?;

        read_empty(&stream, b'S')?;
        let response = [
            Frame::new(b'E', b"SERROR\0C23505\0Mlate sync\0\0").encode(),
            Frame::new(b'Z', b"I").encode(),
            Frame::new(b'C', b"SELECT 1\0").encode(),
            Frame::new(b'Z', b"I").encode(),
        ]
        .concat();
        stream.write_all(&response)?;
        stream.read_closed()
    }

    fn finalizer(&self) -> Result<(), Error> {
        let mut stream = self.accept_within(Duration::from_secs(1))?;
        stream.try_start(None)?;
        read_empty(&stream, b'S')?;
        stream.write_all(
            &[
                Frame::new(b'C', b"BEGIN\0").encode(),
                Frame::new(b'Z', b"T").encode(),
            ]
            .concat(),
        )?;
        read_empty(&stream, b'S')?;
        stream.write_all(
            &[
                Frame::new(b'E', b"SERROR\0VERROR\0C23505\0Mfinish rejected\0\0").encode(),
                Frame::new(b'A', b"\0\0\0\x01finalizer\0failed\0").encode(),
            ]
            .concat(),
        )?;
        stream.read_closed()?;

        let mut stream = self.accept_within(Duration::from_secs(1))?;
        stream.try_start(None)?;
        read_empty(&stream, b'S')?;
        stream.write_all(
            &[
                Frame::new(b'C', b"SELECT 1\0").encode(),
                Frame::new(b'Z', b"I").encode(),
            ]
            .concat(),
        )?;
        stream.read_closed()
    }

    fn copy_recover(&self) -> Result<(), Error> {
        let mut stream = self.accept_within(Duration::from_secs(1))?;
        stream.try_start(None)?;
        for _ in 0..2 {
            read_empty(&stream, b'd')?;
            let rejected = [
                Frame::new(b'E', b"SERROR\0C42P01\0Mmissing copy relation\0\0").encode(),
                Frame::new(b'A', b"\0\0\0\x01copy\0rejected\0").encode(),
            ]
            .concat();
            stream.write_all(&rejected)?;
            stream.read_closed()?;
            stream = self.accept_within(Duration::from_secs(1))?;
            stream.try_start(None)?;
            read_empty(&stream, b'S')?;
            let reply = [
                Frame::new(b'C', b"SELECT 1\0").encode(),
                Frame::new(b'Z', b"I").encode(),
            ]
            .concat();
            stream.write_all(&reply)?;
        }
        stream.read_closed()
    }

    fn recovery(&self, idle: bool, held: bool) -> Result<(), Error> {
        let mut first = self.accept_within(Duration::from_secs(1))?;
        first.try_start(None)?;
        if idle {
            first.write_all(&Frame::new(b'Z', b"I").encode())?;
            first.read_closed()?;
        } else {
            read_empty(&first, b'S')?;
        }
        let first = if held {
            first.write_all(&Frame::new(b'E', b"SERROR\0C23505\0Mheld response\0\0").encode())?;
            first.write_all(&Frame::new(b'Z', b"I").encode())?;
            drop(first);
            None
        } else {
            Some(first)
        };
        let mut next = self.accept_within(Duration::from_secs(1))?;
        next.try_start(None)?;
        if idle || held {
            next.write_all(&Frame::new(b'A', b"\0\0\0\x02replacement\0ready\0").encode())?;
        } else if let Some(first) = &first {
            let _ = first
                .write_all(&Frame::new(b'E', b"SERROR\0CXX000\0Mstale generation\0\0").encode());
            let _ = first.write_all(&Frame::new(b'Z', b"I").encode());
        }
        next.recover()?;
        drop(first);
        Ok(())
    }

    fn least_loaded(&self, initial: usize) -> Result<([peer::Stream<'a>; 2], usize), Error> {
        let mut first = self.accept_within(Duration::from_secs(1))?;
        first.try_start(None)?;
        for _ in 0..initial {
            read_empty(&first, b'S')?;
        }

        let mut second = self.accept_within(Duration::from_secs(1))?;
        second.try_start(None)?;
        read_empty(&second, b'S')?;
        let response = [
            Frame::new(b'C', b"SELECT 1\0").encode(),
            Frame::new(b'Z', b"I").encode(),
        ]
        .concat();
        let done = usize::from(initial == 3);
        if done != 0 {
            first.write_all(&response)?;
        }

        let mut sockets = [first, second];
        for socket in &sockets {
            socket.read_wait(Some(Duration::from_millis(20)))?;
        }
        let started = Instant::now();
        let last = 'selection: loop {
            for (index, socket) in sockets.iter_mut().enumerate() {
                match socket.peek(&mut [0_u8; 1]) {
                    Ok(0) => return Err(Error::Io(peer::Error::UnexpectedEof)),
                    Ok(_) => {
                        socket.read_wait(Some(Duration::from_secs(1)))?;
                        read_empty(socket, b'S')?;
                        break 'selection index;
                    }
                    Err(error)
                        if (matches!(error, peer::Error::TimedOut)
                            || matches!(error, peer::Error::Syscall(error) if error.is_block()))
                            && started.elapsed() < Duration::from_secs(1) => {}
                    Err(error) => return Err(Error::Io(error)),
                }
            }
        };
        let mut counts = [initial - done, 1];
        counts[last] += 1;
        for (socket, count) in sockets.iter_mut().zip(counts) {
            for _ in 0..count {
                socket.write_all(&response)?;
            }
            socket.read_wait(Some(Duration::from_secs(1)))?;
        }
        Ok((sockets, last))
    }
}
