use crate::api::work::guard::{Guard, open};
use crate::api::{state, stmt};
use crate::{catalogs, conn, retry, runtime, sys};
use dope_core::driver::{
    schedule::clock,
    schedule::ready::{self, wake},
};
use dope_runtime::executor::boot;
use std::{mem, pin, task};

mod failing;
mod opening;

use failing::{Failing, Failure};
use opening::{Opening, Progress};
use retry::lease;
use runtime::{local, slot};

pub(crate) struct Open<'local, 'd: 'local, I: catalogs::Catalog> {
    phase: Result<(Phase<'local, 'd, I>, conn::raw::Vacant<'local, 'd, I>), crate::open::Error>,
}

const OPEN_WORDS: usize = 32;

const _: () = assert!(
    mem::size_of::<Open<'static, 'static, catalogs::Empty>>()
        <= OPEN_WORDS * mem::size_of::<usize>()
);

enum Phase<'local, 'd: 'local, I: catalogs::Catalog> {
    Opening(Opening<'local, 'd, I>),
    Prepare(slot::Prepare<'local, 'd, I>),
    Failing(Failing<'local, 'd, I>),
}

enum Next<'local, 'd: 'local, I: catalogs::Catalog> {
    Pending(Phase<'local, 'd, I>),
    Blocked(Phase<'local, 'd, I>),
    Failed {
        phase: Phase<'local, 'd, I>,
        error: crate::Error,
    },
    Return(crate::open::Error),
    Ready(state::State<'local, 'd, I>),
}

impl<'local, 'd: 'local, I: catalogs::Catalog> Open<'local, 'd, I> {
    pub(crate) fn new(
        local: pin::Pin<&'local local::State<'d, I>>,
        report: &'local mut crate::open::Completion,
    ) -> Self {
        let phase =
            Opening::new(local, report).map(|(opening, vacant)| (Phase::Opening(opening), vacant));
        Self { phase }
    }
}

impl<'local, 'd: 'local, I: catalogs::Catalog> Phase<'local, 'd, I> {
    fn retry(&mut self) -> Option<lease::Lease<'_, 'd, lease::Open>> {
        match self {
            Self::Opening(opening) => Some(opening.retry().1),
            Self::Prepare(slot) => Some(slot.retry().1),
            Self::Failing(_) => None,
        }
    }

    fn defer(
        (guard, mut retry): (
            &Guard<'local, open::Open>,
            lease::Lease<'_, 'd, lease::Open>,
        ),
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
        fault: sys::Fault,
    ) -> task::Poll<Result<(), crate::Error>> {
        if guard.check(Ok(())).is_err() {
            return task::Poll::Ready(Err(fault.busy()));
        }
        retry.defer(now, fault, wake)
    }

    fn advance(mut self, now: clock::Moment<'d>, target: ready::Target<'d>) -> Next<'local, 'd, I> {
        let wait = match self.retry() {
            Some(mut retry) => match retry.wait() {
                retry::Wait::Ready => task::Poll::Ready(Ok(())),
                retry::Wait::Pending => retry.poll(now, target.into()),
            },
            None => task::Poll::Ready(Ok(())),
        };
        match wait {
            task::Poll::Pending => return Next::Blocked(self),
            task::Poll::Ready(Ok(())) => {}
            task::Poll::Ready(Err(error)) => return Next::Failed { phase: self, error },
        }

        match self {
            Self::Opening(mut opening) => match opening.advance() {
                Ok(Progress::Pending) => Next::Pending(Self::Opening(opening)),
                Ok(Progress::Ready) => Next::Pending(Self::Prepare(opening.prepare())),
                Err(crate::open::Error::Step(crate::Error::Busy { action, code })) => {
                    let fault = sys::Fault::new(action, code);
                    match Self::defer(opening.retry(), now, target.into(), fault) {
                        task::Poll::Pending => Next::Blocked(Self::Opening(opening)),
                        task::Poll::Ready(Ok(())) => Next::Pending(Self::Opening(opening)),
                        task::Poll::Ready(Err(error)) => Next::Failed {
                            phase: Self::Opening(opening),
                            error,
                        },
                    }
                }
                Err(error) => Next::Return(error),
            },
            Self::Prepare(slot) => match slot.advance() {
                Ok(stmt::Progress::Pending(slot)) => Next::Pending(Self::Prepare(slot)),
                Ok(stmt::Progress::Ready(ready)) => Next::Ready(ready),
                Err((mut slot, crate::Error::Busy { action, code })) => {
                    let fault = sys::Fault::new(action, code);
                    match Self::defer(slot.retry(), now, target.into(), fault) {
                        task::Poll::Pending => Next::Blocked(Self::Prepare(slot)),
                        task::Poll::Ready(Ok(())) => Next::Pending(Self::Prepare(slot)),
                        task::Poll::Ready(Err(error)) => Next::Failed {
                            phase: Self::Prepare(slot),
                            error,
                        },
                    }
                }
                Err((slot, error)) => Next::Failed {
                    phase: Self::Prepare(slot),
                    error,
                },
            },
            Self::Failing(mut failing) => match failing.advance() {
                Failure::Pending => Next::Pending(Self::Failing(failing)),
                Failure::Done => Next::Return(failing.error.into()),
            },
        }
    }

    fn fail(self, error: crate::Error) -> Result<Self, crate::open::Error> {
        match self {
            Self::Prepare(slot) => Ok(Self::Failing(Failing {
                prepare: slot,
                error,
            })),
            Self::Opening(_) => Err(error.into()),
            Self::Failing(_) => Err(error.into()),
        }
    }
}

impl<'local, 'd: 'local, I: catalogs::Catalog> boot::Root<'d> for Open<'local, 'd, I> {
    type Output = Result<conn::Installed<'local, 'd, I>, crate::open::Error>;

    fn poll(self, context: boot::Context<'_, 'd>) -> boot::Poll<Self, Self::Output> {
        let (budget, clock) = context.into_poll();
        let now = clock.now();
        let (mut phase, vacant) = match self.phase {
            Ok(phase) => phase,
            Err(error) => return boot::Poll::Ready(Err(error)),
        };
        loop {
            match phase.advance(now, budget.target()) {
                Next::Blocked(phase) => {
                    return boot::Poll::Pending(Self {
                        phase: Ok((phase, vacant)),
                    });
                }
                Next::Pending(next) => {
                    if budget.try_continue().is_some() {
                        phase = next;
                    } else {
                        return boot::Poll::Pending(Self {
                            phase: Ok((next, vacant)),
                        });
                    }
                }
                Next::Failed {
                    phase: failed,
                    error,
                } => match failed.fail(error) {
                    Ok(next) => phase = next,
                    Err(error) => return boot::Poll::Ready(Err(error)),
                },
                Next::Return(error) => return boot::Poll::Ready(Err(error)),
                Next::Ready(ready) => return boot::Poll::Ready(Ok(vacant.insert(ready))),
            }
        }
    }

    fn cancel(self) {
        drop(self.phase);
    }
}
