pub mod accept;
mod frame;
pub use frame::Frame;
pub mod group;
pub mod notice;
pub mod retire;
pub mod startup;

use dope::core::platform::wake::{Notify, NotifyError, WaitError};
use dope::runtime::executor::root;
use std::{
    io::{self, ErrorKind},
    pin::Pin,
    task::Poll,
};

pub struct Quarantine<F, C> {
    query: Option<Pin<Box<F>>>,
    release: Option<Notify>,
    quarantine: Option<C>,
}

#[derive(Debug)]
pub enum RetireError {
    Release(NotifyError),
    Consumed,
}

impl<F, C> Quarantine<F, C> {
    pub fn new(query: Pin<Box<F>>, release: Notify, close: C) -> Self {
        Self {
            query: Some(query),
            release: Some(release),
            quarantine: Some(close),
        }
    }
}

impl<'d, F, C: FnOnce() + Unpin> root::Root<'d> for Quarantine<F, C> {
    type Output = Result<(), RetireError>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (this, wake, work, _) = context.into_parts();
        let this = this.get_mut();
        if let Some(release) = this.release.take() {
            release.notify().map_err(RetireError::Release)?;
            wake.wake();
            return Poll::Pending;
        }
        if work.remaining() != 0 {
            wake.wake();
            return Poll::Pending;
        }
        drop(this.query.take());
        this.quarantine.take().ok_or(RetireError::Consumed)?();
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    NonemptyFrame,
    UnexpectedRequest,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
pub enum SettleError {
    Connect(io::Error),
    Startup(io::Error),
    Begin(Error),
    Group(Error),
    Release(WaitError),
    Close(Error),
    Reconnect(io::Error),
    Restart(io::Error),
    Command(Error),
    Stop(Error),
}

fn read_empty(stream: &mut impl io::Read, expected: u8) -> Result<(), Error> {
    loop {
        let mut header = [0_u8; 5];
        stream.read_exact(&mut header)?;
        let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut body = vec![
            0;
            length
                .checked_sub(4)
                .ok_or(io::Error::from(ErrorKind::InvalidData))?
        ];
        stream.read_exact(&mut body)?;
        if header[0] == expected {
            if !body.is_empty() {
                return Err(Error::NonemptyFrame);
            }
            return Ok(());
        }
    }
}
