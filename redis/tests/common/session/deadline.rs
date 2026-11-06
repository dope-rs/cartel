use std::{pin, time::Duration};

use dope_fiber::abi::{Fiber, PollFn, Task};
use dope_test::harness::peer::{self, Stream, TcpAcceptor};

pub fn accept_within<'a>(peer: &TcpAcceptor<'a>) -> Result<Stream<'a>, peer::Error> {
    peer.accept_for(Duration::from_secs(1))?
        .ok_or(peer::Error::TimedOut)
}

pub fn wait_closed(socket: &Stream<'_>) -> Result<(), peer::Error> {
    let mut bytes = [0; 128];
    loop {
        match socket.read(&mut bytes) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(peer::Error::Syscall(error)) if error.is_reset() => return Ok(()),
            Err(error) => return Err(error),
        }
    }
}

pub fn recovery_server(peer: &mut TcpAcceptor<'_>) -> Result<(), peer::Error> {
    let first = accept_within(peer)?;
    first.read_wait(Some(Duration::from_secs(1)))?;
    let mut buf = [0_u8; 128];
    assert_ne!(first.read(&mut buf)?, 0);

    let next = accept_within(peer)?;
    next.read_wait(Some(Duration::from_secs(1)))?;
    let _ = first.write_all(b"-ERR stale generation\r\n");
    assert_ne!(next.read(&mut buf)?, 0);
    next.write_all(b"+PONG\r\n")
}

pub fn release_after_poll<'a, 'd: 'a, F, H>(
    mut fiber: pin::Pin<Box<Task<F>>>,
    held: H,
) -> impl Fiber<'d, Output = F::Output> + 'a
where
    F: Fiber<'d> + 'a,
    H: 'a,
{
    let mut held = Some(held);
    PollFn::admitted(move |admit| {
        let (outcome, _) = admit.poll(fiber.as_mut());
        if outcome.is_pending() && held.is_some() {
            drop(held.take());
        }
        outcome
    })
}
