use std::{
    io,
    io::{Read as _, Write as _},
    pin,
    task::Poll,
    time::{Duration, Instant},
};

use dope_fiber::{
    abi::{Fiber, PollFn, Task},
    context,
};
use dope_test::harness::{GUARD, peer::Stream};

pub const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

pub fn pong_once(mut socket: &Stream<'_>) -> io::Result<()> {
    socket.read_wait(Some(Duration::from_secs(1)))?;
    let mut buf = [0_u8; PING.len()];
    socket.read_exact(&mut buf)?;
    if buf != PING {
        return Err(io::ErrorKind::InvalidData.into());
    }
    socket.write_all(b"+PONG\r\n")
}

pub struct Held<F>(pin::Pin<Box<Task<F>>>);

impl<F> Held<F> {
    pub fn poll_pending<'a, 'd: 'a>(&'a mut self) -> impl Fiber<'d, Output = bool> + 'a
    where
        F: Fiber<'d> + 'a,
    {
        PollFn::admitted(move |admit| Poll::Ready(admit.poll(self.0.as_mut()).0.is_pending()))
    }
}

impl<'d, F> Fiber<'d> for Held<F>
where
    F: Fiber<'d>,
{
    type Output = F::Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        admit.poll(this.get_mut().0.as_mut()).0
    }
}

pub fn retain_after_pending<'a, 'd: 'a, F>(
    fiber: F,
) -> impl Fiber<'d, Output = (Held<F>, bool)> + 'a
where
    F: Fiber<'d> + 'a,
{
    let mut fiber = Some(Held(Box::pin(Task::new(fiber))));
    PollFn::admitted(move |admit| {
        let Some(mut child) = fiber.take() else {
            return Poll::Pending;
        };
        let (poll, _) = admit.poll(child.0.as_mut());
        Poll::Ready((child, poll.is_pending()))
    })
}

pub fn first_request(sockets: &mut [&Stream<'_>; 2], expected: &[u8]) -> io::Result<usize> {
    for socket in sockets.iter() {
        socket.read_wait(Some(Duration::from_millis(1)))?;
    }
    let mut request = vec![0; expected.len()];
    let started = Instant::now();
    loop {
        for (index, socket) in sockets.iter_mut().enumerate() {
            match socket.read(&mut request[..1]) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(_) => {
                    socket.read_wait(Some(GUARD))?;
                    socket.read_exact(&mut request[1..])?;
                    assert_eq!(request, expected);
                    return Ok(index);
                }
                Err(error) if error.kind() == io::ErrorKind::TimedOut => {}
                Err(error) => return Err(error),
            }
        }
        if started.elapsed() >= GUARD {
            return Err(io::ErrorKind::TimedOut.into());
        }
    }
}
