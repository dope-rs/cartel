use crate::common::postgres::{Error, Frame, SettleError, read_empty, startup::Stream as _};
use crate::common::wire::{
    checked::{Database, TextRow},
    queries::{DIAGNOSTIC, DUPLICATE},
};
use cartel_pg::protocol::{Number, Reason};
use dope::core::platform::wake::Wait;
use dope_test::harness::peer;
use std::{
    io,
    io::ErrorKind,
    io::{Read as _, Write as _},
    net::TcpListener,
    net::TcpStream,
    thread,
    time::Duration,
    time::Instant,
};

pub const DATA_ROW: Frame<'static> = Frame::new(b'D', b"\0\x01\0\0\0\x011");

#[derive(Debug)]
pub enum Rejection<'a> {
    Protocol(Reason),
    Number { kind: u8, error: &'a str },
}

pub const REPLIES: [(Frame<'static>, Option<Rejection<'static>>); 8] = [
    (Frame::new(b'E', DIAGNOSTIC), None),
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
        Some(Rejection::Number {
            kind: b'L',
            error: "Invalid { offset: Offset(2), byte: 120 }",
        }),
    ),
    (
        Frame::new(b'E', b"SERROR\0C23505\0Maddition\0P4294967296\0\0"),
        Some(Rejection::Number {
            kind: b'P',
            error: "Overflow { offset: Offset(9), byte: 54 }",
        }),
    ),
    (
        Frame::new(b'E', b"SERROR\0C23505\0Mmultiplication\0p4294967300\0\0"),
        Some(Rejection::Number {
            kind: b'p',
            error: "Overflow { offset: Offset(9), byte: 48 }",
        }),
    ),
];

pub fn reply(socket: &mut &peer::Stream<'_>, frame: Frame<'_>) -> Result<(), Error> {
    socket.prepare()?;
    Database::start(&mut *socket)?;
    read_empty(socket, b'S')?;
    socket.write_all(&[frame.encode(), Frame::new(b'Z', b"I").encode()].concat())?;
    socket.read_closed()
}

pub fn fatal(
    socket: &mut &peer::Stream<'_>,
    detail: &[u8],
    buffered: bool,
    release: Wait,
) -> Result<(), SettleError> {
    socket.prepare().map_err(SettleError::Startup)?;
    Database::start(&mut *socket).map_err(SettleError::Startup)?;
    read_empty(socket, b'S').map_err(SettleError::Command)?;
    if buffered {
        socket
            .write_all(&Frame::new(b'D', &TextRow::wire(1, b"retained")).encode())
            .map_err(Error::Io)
            .map_err(SettleError::Command)?;
    }
    release
        .wait_timeout(Duration::from_secs(1))
        .map_err(SettleError::Release)?;
    socket
        .write_all(&Frame::new(b'E', detail).encode())
        .map_err(Error::Io)
        .map_err(SettleError::Command)?;
    socket.read_closed().map_err(SettleError::Close)
}

pub trait Listener {
    fn try_accept(&self, timeout: Duration) -> io::Result<TcpStream>;
    fn group_settle(self, ready: Wait) -> Result<(), SettleError>;
    fn reply_settle(self) -> Result<TcpStream, Error>;
    fn finalizer(self) -> Result<(), Error>;
    fn copy_recover(self) -> Result<(), Error>;
    fn recovery(self) -> io::Result<(TcpStream, TcpStream)>;
    fn least_loaded(self, initial: usize) -> Result<([TcpStream; 2], Vec<usize>), Error>;
}

impl Listener for TcpListener {
    fn try_accept(&self, timeout: Duration) -> io::Result<TcpStream> {
        self.set_nonblocking(true)?;
        let started = Instant::now();
        let socket = loop {
            match self.accept() {
                Ok((socket, _)) => break Ok(socket),
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock && started.elapsed() < timeout =>
                {
                    thread::yield_now();
                }
                Err(error) => break Err(error),
            }
        }?;
        socket.set_nonblocking(false)?;
        Ok(socket)
    }

    fn group_settle(self, ready: Wait) -> Result<(), SettleError> {
        let mut stream = self
            .try_accept(Duration::from_secs(1))
            .map_err(SettleError::Connect)?;
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(SettleError::Startup)?;
        Database::start(&stream).map_err(SettleError::Startup)?;
        read_empty(&mut stream, b'S').map_err(SettleError::Begin)?;
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

        read_empty(&mut stream, b'S').map_err(SettleError::Group)?;
        ready
            .wait_timeout(Duration::from_secs(1))
            .map_err(SettleError::Release)?;
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

        let mut stream = self
            .try_accept(Duration::from_secs(1))
            .map_err(SettleError::Reconnect)?;
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(SettleError::Restart)?;
        Database::start(&stream).map_err(SettleError::Restart)?;
        read_empty(&mut stream, b'S').map_err(SettleError::Command)?;
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

    fn reply_settle(self) -> Result<TcpStream, Error> {
        let mut stream = self.try_accept(Duration::from_secs(1))?;
        stream.try_start(None)?;

        for _ in 0..2 {
            read_empty(&mut stream, b'S')?;
        }

        let response = [
            Frame::new(b'C', b"SELECT 1\0").encode(),
            Frame::new(b'E', b"SERROR\0C23505\0Mlate sync\0\0").encode(),
            Frame::new(b'Z', b"I").encode(),
            Frame::new(b'C', b"SELECT 1\0").encode(),
            Frame::new(b'Z', b"I").encode(),
        ]
        .concat();
        stream.write_all(&response)?;

        read_empty(&mut stream, b'S')?;
        let reply = [
            Frame::new(b'C', b"SELECT 1\0").encode(),
            Frame::new(b'A', b"\0\0\0\x01settlement\0complete\0").encode(),
        ]
        .concat();
        stream.write_all(&reply)?;
        Ok(stream)
    }

    fn finalizer(self) -> Result<(), Error> {
        let mut stream = self.try_accept(Duration::from_secs(1))?;
        stream.try_start(None)?;
        read_empty(&mut stream, b'S')?;
        stream.write_all(
            &[
                Frame::new(b'C', b"BEGIN\0").encode(),
                Frame::new(b'Z', b"T").encode(),
            ]
            .concat(),
        )?;
        read_empty(&mut stream, b'S')?;
        stream.write_all(
            &[
                Frame::new(b'E', b"SERROR\0VERROR\0C23505\0Mfinish rejected\0\0").encode(),
                Frame::new(b'A', b"\0\0\0\x01finalizer\0failed\0").encode(),
            ]
            .concat(),
        )?;
        stream.read_closed()?;

        let mut stream = self.try_accept(Duration::from_secs(1))?;
        stream.try_start(None)?;
        read_empty(&mut stream, b'S')?;
        stream.write_all(
            &[
                Frame::new(b'C', b"SELECT 1\0").encode(),
                Frame::new(b'Z', b"I").encode(),
            ]
            .concat(),
        )?;
        stream.read_closed()
    }

    fn copy_recover(self) -> Result<(), Error> {
        let mut stream = self.try_accept(Duration::from_secs(1))?;
        stream.try_start(None)?;
        for _ in 0..2 {
            read_empty(&mut stream, b'd')?;
            let rejected = [
                Frame::new(b'E', b"SERROR\0C42P01\0Mmissing copy relation\0\0").encode(),
                Frame::new(b'A', b"\0\0\0\x01copy\0rejected\0").encode(),
            ]
            .concat();
            stream.write_all(&rejected)?;
            stream.read_closed()?;
            stream = self.try_accept(Duration::from_secs(1))?;
            stream.try_start(None)?;
            read_empty(&mut stream, b'S')?;
            let reply = [
                Frame::new(b'C', b"SELECT 1\0").encode(),
                Frame::new(b'Z', b"I").encode(),
            ]
            .concat();
            stream.write_all(&reply)?;
        }
        stream.read_closed()
    }

    fn recovery(self) -> io::Result<(TcpStream, TcpStream)> {
        let mut first = self.try_accept(Duration::from_secs(1))?;
        first.try_start(None)?;
        let mut request = [0_u8; 512];
        if first.read(&mut request)? == 0 {
            return Err(io::Error::from(ErrorKind::UnexpectedEof));
        }
        let mut next = self.try_accept(Duration::from_secs(1))?;
        next.try_start(None)?;
        let _ =
            first.write_all(&Frame::new(b'E', b"SERROR\0CXX000\0Mstale generation\0\0").encode());
        let _ = first.write_all(&Frame::new(b'Z', b"I").encode());
        Ok((first, next))
    }

    fn least_loaded(self, initial: usize) -> Result<([TcpStream; 2], Vec<usize>), Error> {
        let mut selected = Vec::new();
        let mut first = self.try_accept(Duration::from_secs(1))?;
        first.try_start(None)?;
        for _ in 0..initial {
            read_empty(&mut first, b'S')?;
            selected.push(0);
        }

        let mut second = self.try_accept(Duration::from_secs(1))?;
        second.try_start(None)?;
        read_empty(&mut second, b'S')?;
        selected.push(1);
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
            socket.set_read_timeout(Some(Duration::from_millis(20)))?;
        }
        let started = Instant::now();
        let last = 'selection: loop {
            for (index, socket) in sockets.iter_mut().enumerate() {
                match socket.peek(&mut [0_u8; 1]) {
                    Ok(0) => return Err(Error::Io(io::Error::from(ErrorKind::UnexpectedEof))),
                    Ok(_) => {
                        socket.set_read_timeout(Some(Duration::from_secs(1)))?;
                        read_empty(socket, b'S')?;
                        selected.push(index);
                        break 'selection index;
                    }
                    Err(error)
                        if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
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
            socket.set_read_timeout(Some(Duration::from_secs(1)))?;
        }
        Ok((sockets, selected))
    }
}
