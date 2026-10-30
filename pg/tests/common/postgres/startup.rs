use crate::common::{
    postgres::{Error, Frame},
    wire::queries::DIAGNOSTIC,
};
use dope_test::harness::peer;
use std::{io, io::ErrorKind, net::TcpStream, time::Duration};

pub trait Stream: io::Read + io::Write + Sized
where
    for<'a> &'a Self: io::Read + io::Write,
{
    fn prepare(&self) -> io::Result<()>;

    fn try_start(&mut self, key: Option<(i32, i32)>) -> io::Result<()> {
        self.prepare()?;
        read(self)?;

        self.write_all(&Frame::new(b'R', &0_u32.to_be_bytes()).encode())?;
        if let Some((pid, secret)) = key {
            let mut bytes = [0_u8; 8];
            bytes[..4].copy_from_slice(&pid.to_be_bytes());
            bytes[4..].copy_from_slice(&secret.to_be_bytes());
            self.write_all(&Frame::new(b'K', &bytes).encode())?;
        }
        self.write_all(&Frame::new(b'Z', b"I").encode())?;

        let mut sync = [0_u8; 5];
        self.read_exact(&mut sync)?;
        if sync != [b'S', 0, 0, 0, 4] {
            return Err(ErrorKind::InvalidData.into());
        }
        self.write_all(&Frame::new(b'Z', b"I").encode())?;
        Ok(())
    }

    fn ready(mut self) -> Result<(), Error> {
        self.write_all(&Frame::new(b'Z', b"I").encode())?;
        self.read_closed()
    }

    fn recover(mut self) -> io::Result<()> {
        let mut request = [0_u8; 512];
        if self.read(&mut request)? == 0 {
            return Err(io::Error::from(ErrorKind::UnexpectedEof));
        }
        self.write_all(&Frame::new(b'C', b"SELECT 1\0").encode())?;
        self.write_all(&Frame::new(b'Z', b"I").encode())
    }

    fn read_closed(&self) -> Result<(), Error> {
        let mut stream = self;
        match io::Read::read(&mut stream, &mut [0_u8; 1]) {
            Ok(0) => Ok(()),
            Err(error) if error.kind() == ErrorKind::ConnectionReset => Ok(()),
            Err(error) => Err(Error::Io(error)),
            Ok(_) => Err(Error::UnexpectedRequest),
        }
    }

    fn reject(&self) -> Result<(), Error> {
        let mut socket = self;
        read(&mut socket)?;
        io::Write::write_all(&mut socket, &Frame::new(b'E', DIAGNOSTIC).encode())?;
        self.read_closed()
    }
}

impl Stream for TcpStream {
    fn prepare(&self) -> io::Result<()> {
        self.set_nodelay(true)?;
        self.set_read_timeout(Some(Duration::from_secs(1)))
    }
}

impl Stream for peer::Stream<'_> {
    fn prepare(&self) -> io::Result<()> {
        self.read_wait(Some(Duration::from_secs(1)))
    }
}

pub(crate) fn read(stream: &mut impl io::Read) -> io::Result<Vec<u8>> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    let mut startup = vec![0_u8; length.checked_sub(4).ok_or(ErrorKind::InvalidData)?];
    stream.read_exact(&mut startup)?;
    Ok(startup)
}
