use crate::api::work::guard::{Guard, open};
use crate::api::{state, stmt};
use crate::open::schema::{Autocommit, Setup};
use crate::{catalogs, conn, retry, runtime, sys};
use dope_core::driver::schedule::{clock, ready};
use dope_runtime::executor::boot;
use std::{mem, pin, task};

mod opening;

use opening::{Opening, Progress};
use retry::lease;
use runtime::{local, slot};

pub(crate) struct Open<'local, 'd: 'local, I: catalogs::Catalog, S: Setup> {
    phase: Result<(Phase<'local, 'd, I, S>, conn::raw::Vacant<'local, 'd, I>), crate::open::Error>,
}

const _: () = assert!(
    mem::size_of::<Open<'static, 'static, catalogs::Empty, Autocommit>>()
        <= 32 * mem::size_of::<usize>()
);

enum Phase<'local, 'd: 'local, I: catalogs::Catalog, S: Setup> {
    Opening(Opening<'local, 'd, I>, S),
    Begin(Opening<'local, 'd, I>, S, S::Transaction),
    Apply(Opening<'local, 'd, I>, S, S::Transaction),
    Commands(Opening<'local, 'd, I>),
    Prepare(slot::Prepare<'local, 'd, I>),
    Commit(slot::Complete<'local, 'd, I>, S::Transaction),
    Failing(slot::Failed<'local, 'd, I>, crate::Error),
}

enum Next<'local, 'd: 'local, I: catalogs::Catalog, S: Setup> {
    Pending(Phase<'local, 'd, I, S>),
    Blocked(Phase<'local, 'd, I, S>),
    Retire(slot::Failed<'local, 'd, I>, crate::Error),
    Return(S::Error),
    Ready(state::State<'local, 'd, I>),
}

impl<'local, 'd: 'local, I: catalogs::Catalog, S: Setup> Open<'local, 'd, I, S> {
    pub(crate) fn new(
        local: pin::Pin<&'local local::State<'d, I>>,
        report: &'local mut crate::open::Completion,
        setup: S,
    ) -> Self {
        let phase = Opening::new(local, report)
            .map(|(opening, vacant)| (Phase::Opening(opening, setup), vacant));
        Self { phase }
    }
}

impl<'local, 'd: 'local, I: catalogs::Catalog, S: Setup> Phase<'local, 'd, I, S> {
    fn retry(
        &mut self,
    ) -> Option<(
        &Guard<'local, open::Open>,
        lease::Lease<'_, 'd, lease::Open>,
    )> {
        match self {
            Self::Opening(o, _) | Self::Begin(o, _, _) | Self::Commands(o) => Some(o.retry()),
            Self::Prepare(slot) => Some(slot.retry()),
            Self::Commit(slot, _) => Some(slot.retry()),
            Self::Apply(_, _, _) | Self::Failing(_, _) => None,
        }
    }

    fn fail(self, error: crate::Error) -> Next<'local, 'd, I, S> {
        let prepare = match self {
            Self::Prepare(slot) => slot.retire(),
            Self::Commit(slot, _) => slot.retire(),
            _ => return Next::Return(crate::open::Error::Step(error).into()),
        };
        Next::Retire(prepare, error)
    }

    fn error(
        mut self,
        error: crate::Error,
        now: clock::Moment<'d>,
        target: ready::Target<'d>,
    ) -> Next<'local, 'd, I, S> {
        let deferred = if let crate::Error::Busy { action, code } = error
            && !(S::TRANSACTION.is_some() && matches!(self, Self::Commands(_)))
            && let Some((guard, mut retry)) = self.retry()
            && guard.check(Ok(())).is_ok()
        {
            retry.defer(now, sys::Fault::new(action, code), target.into())
        } else {
            task::Poll::Ready(Err(error))
        };
        match deferred {
            task::Poll::Pending => Next::Blocked(self),
            task::Poll::Ready(Ok(())) => Next::Pending(self),
            task::Poll::Ready(Err(error)) => self.fail(error),
        }
    }

    fn advance(
        mut self,
        now: clock::Moment<'d>,
        target: ready::Target<'d>,
    ) -> Next<'local, 'd, I, S> {
        let wait = if let Some((_, mut retry)) = self.retry()
            && let retry::Wait::Pending = retry.wait()
        {
            retry.poll(now, target.into())
        } else {
            task::Poll::Ready(Ok(()))
        };
        match wait {
            task::Poll::Pending => return Next::Blocked(self),
            task::Poll::Ready(Err(error)) => return self.fail(error),
            task::Poll::Ready(Ok(())) => {}
        }
        match self {
            Self::Opening(mut opening, setup) => match opening.configure_next() {
                Ok(Progress::Pending) => Next::Pending(Self::Opening(opening, setup)),
                Ok(Progress::Ready) if let Some(transaction) = S::TRANSACTION => {
                    opening.resources.parameters();
                    opening.resources.authorize::<true>();
                    Next::Pending(Self::Begin(opening, setup, transaction))
                }
                Ok(Progress::Ready) => {
                    opening.resources.authorize::<false>();
                    Next::Pending(Self::Commands(opening))
                }
                Err(error) => Self::Opening(opening, setup).error(error, now, target),
            },
            Self::Begin(opening, setup, transaction) => match opening
                .resources
                .begin_immediate(transaction.into())
            {
                Ok(()) => Next::Pending(Self::Apply(opening, setup, transaction)),
                Err(error) => Self::Begin(opening, setup, transaction).error(error, now, target),
            },
            Self::Apply(opening, setup, transaction) => {
                match setup.apply(transaction, opening.resources.schema()) {
                    Ok(()) => {
                        opening.resources.authorize::<false>();
                        Next::Pending(Self::Commands(opening))
                    }
                    Err(error) => Next::Return(error),
                }
            }
            Self::Commands(mut opening) => match opening.command() {
                Ok(Progress::Pending) => Next::Pending(Self::Commands(opening)),
                Ok(Progress::Ready) => {
                    if S::TRANSACTION.is_none() {
                        opening.resources.parameters();
                    }
                    Next::Pending(Self::Prepare(opening.prepare()))
                }
                Err(error) => Self::Commands(opening).error(error, now, target),
            },
            Self::Prepare(slot) => match slot.advance() {
                Ok(stmt::Progress::Pending(slot)) => Next::Pending(Self::Prepare(slot)),
                Ok(stmt::Progress::Ready(slot)) if let Some(transaction) = S::TRANSACTION => {
                    Next::Pending(Self::Commit(slot, transaction))
                }
                Ok(stmt::Progress::Ready(slot)) => Next::Ready(slot.ready()),
                Err((slot, error)) => Self::Prepare(slot).error(error, now, target),
            },
            Self::Commit(mut slot, transaction) => match slot.commit(transaction.into()) {
                Ok(()) => Next::Ready(slot.ready()),
                Err(error) => Self::Commit(slot, transaction).error(error, now, target),
            },
            Self::Failing(mut slot, error) => match slot.retire_one() {
                stmt::Inventory::Populated => Next::Pending(Self::Failing(slot, error)),
                stmt::Inventory::Empty => Next::Return(crate::open::Error::Step(error).into()),
            },
        }
    }
}

impl<'local, 'd: 'local, I: catalogs::Catalog, S: Setup> boot::Root<'d> for Open<'local, 'd, I, S> {
    type Output = Result<conn::Installed<'local, 'd, I>, S::Error>;

    fn poll(self, context: boot::Context<'_, 'd>) -> boot::Poll<Self, Self::Output> {
        let (budget, clock) = context.into_poll();
        let now = clock.now();
        let (mut phase, vacant) = match self.phase {
            Ok(phase) => phase,
            Err(error) => return boot::Poll::Ready(Err(error.into())),
        };
        loop {
            let next = match phase.advance(now, budget.target()) {
                Next::Pending(next) if budget.try_continue().is_some() => {
                    phase = next;
                    continue;
                }
                Next::Pending(next) | Next::Blocked(next) => next,
                Next::Retire(slot, error) => {
                    phase = Phase::Failing(slot, error);
                    continue;
                }
                Next::Return(error) => return boot::Poll::Ready(Err(error)),
                Next::Ready(ready) => return boot::Poll::Ready(Ok(vacant.insert(ready))),
            };
            return boot::Poll::Pending(Self {
                phase: Ok((next, vacant)),
            });
        }
    }

    fn cancel(self) {
        drop(self.phase);
    }
}
