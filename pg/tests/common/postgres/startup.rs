use crate::common::{
    postgres::{Error, Frame, read_empty},
    wire::queries::DIAGNOSTIC,
};
use dope_test::harness::peer;
use std::time::Duration;

pub const AUTHENTICATED: &[u8] = b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I";
pub const IDLE: &[u8] = b"Z\0\0\0\x05I";

pub fn request<'io, 'scope, 'w, 'd: 'w>(
    io: &'io mut dope_fiber::net::Io<
        'scope,
        'w,
        'd,
        dope::net::wire::Identity,
        dope::manifold::network::listener::conn::Id<'d, dope::core::driver::route::KeyTag<1>>,
    >,
    startup: bool,
) -> impl dope_fiber::abi::Fiber<'d, Output = ()> + 'io {
    dope_fiber::fiber!('d => async move {
        let mut header = [0; 5];
        let mut filled = 0;
        let mut remaining = 0;
        let width = 4 + usize::from(!startup);
        loop {
            let chunk = io.read().await.ok().expect("peer read").expect("request EOF");
            let mut bytes = chunk.as_ref();
            assert!(!bytes.is_empty(), "empty request chunk");
            while !bytes.is_empty() {
                if filled < width {
                    let count = (width - filled).min(bytes.len());
                    header[filled..filled + count].copy_from_slice(&bytes[..count]);
                    filled += count;
                    bytes = &bytes[count..];
                    if filled < width {
                        continue;
                    }
                    let length = u32::from_be_bytes(
                        header[width - 4..width].try_into().expect("request length"),
                    );
                    assert!(length >= 4, "invalid request length: {length}");
                    assert!(startup || header[0] != b'S' || length == 4, "nonempty Sync");
                    remaining = length as usize - 4;
                }
                let count = remaining.min(bytes.len());
                remaining -= count;
                bytes = &bytes[count..];
                if remaining == 0 {
                    if startup || header[0] == b'S' {
                        assert!(bytes.is_empty(), "unexpected trailing request");
                        return;
                    }
                    filled = 0;
                }
            }
        }
    })
}

pub trait Stream {
    fn prepare(&self) -> Result<(), peer::Error>;
    fn try_start(&mut self, key: Option<(i32, i32)>) -> Result<(), Error>;
    fn recover(self) -> Result<(), Error>;
    fn read_closed(&self) -> Result<(), Error>;
    fn reject(&self) -> Result<(), Error>;
}

impl Stream for peer::Stream<'_> {
    fn prepare(&self) -> Result<(), peer::Error> {
        self.set_nodelay(true)?;
        self.read_wait(Some(Duration::from_secs(1)))
    }

    fn try_start(&mut self, key: Option<(i32, i32)>) -> Result<(), Error> {
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
            return Err(Error::UnexpectedRequest);
        }
        self.write_all(&Frame::new(b'Z', b"I").encode())?;
        Ok(())
    }

    fn recover(self) -> Result<(), Error> {
        read_empty(&self, b'S')?;
        self.write_all(&Frame::new(b'C', b"SELECT 1\0").encode())?;
        self.write_all(&Frame::new(b'Z', b"I").encode())
            .map_err(Error::Io)
    }

    fn read_closed(&self) -> Result<(), Error> {
        match self.read(&mut [0_u8; 1]) {
            Ok(0) => Ok(()),
            Err(peer::Error::Syscall(error)) if error.is_reset() => Ok(()),
            Err(error) => Err(Error::Io(error)),
            Ok(_) => Err(Error::UnexpectedRequest),
        }
    }

    fn reject(&self) -> Result<(), Error> {
        read(self)?;
        self.write_all(&Frame::new(b'E', DIAGNOSTIC).encode())?;
        self.read_closed()
    }
}

pub(crate) fn read(stream: &peer::Stream<'_>) -> Result<Vec<u8>, Error> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length);
    let mut startup =
        vec![0_u8; length.checked_sub(4).ok_or(Error::InvalidLength(length))? as usize];
    stream.read_exact(&mut startup)?;
    Ok(startup)
}
