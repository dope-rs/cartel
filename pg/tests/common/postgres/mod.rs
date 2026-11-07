use dope_test::harness::peer;
pub mod accept;
mod frame;
pub use frame::Frame;
pub mod group;
pub mod notice;
pub mod retire;
pub mod startup;

use dope::runtime::executor::root;
use dope_fiber::{
    abi::{Fiber, Task},
    context::{Context, PollResult},
};
use std::{pin::Pin, task::Poll};

pub struct Quarantine<F, C> {
    query: Option<Pin<Box<Task<F>>>>,
    admitted: bool,
    quarantine: Option<C>,
}

#[derive(Debug)]
pub enum RetireError {
    Completed,
    Consumed,
}

impl<F, C> Quarantine<F, C> {
    pub fn new(query: Pin<Box<Task<F>>>, close: C) -> Self {
        Self {
            query: Some(query),
            admitted: false,
            quarantine: Some(close),
        }
    }
}

impl<'d, F: Fiber<'d>, C: FnOnce() + Unpin> root::Root<'d> for Quarantine<F, C> {
    type Output = Result<(), RetireError>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (this, wake, work, driver) = context.into_parts();
        let this = this.get_mut();
        if !this.admitted {
            let query = this.query.as_mut().ok_or(RetireError::Consumed)?;
            this.admitted = match Context::with_target(wake, work, driver, |context| {
                context.try_poll(query.as_mut())
            }) {
                PollResult::Exhausted => false,
                PollResult::Polled(Poll::Pending) => true,
                PollResult::Polled(Poll::Ready(_)) => {
                    return Poll::Ready(Err(RetireError::Completed));
                }
            };
            wake.wake();
            return Poll::Pending;
        }
        let _permit = work.permit();
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
    Io(peer::Error),
    InvalidLength(u32),
    Utf8(std::str::Utf8Error),
    NonemptyFrame,
    UnexpectedRequest,
}

impl From<peer::Error> for Error {
    fn from(error: peer::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
pub enum SettleError {
    Connect(peer::Error),
    Startup(Error),
    Begin(Error),
    Group(Error),
    Close(Error),
    Reconnect(peer::Error),
    Restart(Error),
    Command(Error),
    Stop(Error),
}

pub(crate) fn read_empty(stream: &peer::Stream<'_>, expected: u8) -> Result<(), Error> {
    loop {
        let mut tag = [0_u8; 1];
        stream.read_exact(&mut tag)?;
        let body = startup::read(stream)?;
        if tag[0] == expected {
            if !body.is_empty() {
                return Err(Error::NonemptyFrame);
            }
            return Ok(());
        }
    }
}
