use std::{cell, mem};

use cartel_core::reply::{layout::Position, order::Cutoff};
use dope::{
    core::driver::{route, schedule::ready},
    manifold::client::{close, conn},
};

use crate::port::schedule::routes::{Access, Acquire, Bind, Bound, Closing, State};
use crate::{client, port::schedule::Release};

pub(crate) struct Conn<'b, 'd, const ID: u8> {
    pub(super) phase: cell::UnsafeCell<State<'b, 'd, ID>>,
}

impl<'b, 'd, const ID: u8> Default for Conn<'b, 'd, ID> {
    fn default() -> Self {
        Self {
            phase: cell::UnsafeCell::new(State::Vacant),
        }
    }
}

impl<'b, 'd, const ID: u8> Conn<'b, 'd, ID> {
    pub(in crate::port::schedule) fn wake(&self) {
        // SAFETY: this thread-local leaf only copies the ready capability.
        // The phase borrow ends before invoking the external wake operation.
        let ready = unsafe { &*self.phase.get() }
            .bound()
            .map(|bound| bound.ready);
        if let Some(ready) = ready {
            ready.wake();
        }
    }

    pub(in crate::port::schedule) fn target(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Option<ready::Target<'d>> {
        // SAFETY: this thread-local leaf only reads Copy identity fields;
        // no callback runs and no reference into the phase escapes.
        unsafe { &*self.phase.get() }
            .bound()
            .filter(|bound| bound.position.connection() == token)
            .map(|bound| bound.ready)
    }

    pub(super) fn bound<R>(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
        apply: impl FnOnce(Position<'b, 'd, route::KeyTag<ID>>, Option<[Cutoff<'b>; 2]>) -> R,
    ) -> Option<R> {
        let (position, frontier) = {
            // SAFETY: this thread-local leaf copies the phase fields before
            // invoking its private projection; no phase reference escapes.
            let (bound, frontier) = match unsafe { &*self.phase.get() } {
                State::Vacant => return None,
                State::Shared(bound, frontier) | State::Exclusive(bound, frontier) => {
                    (bound, Some(*frontier))
                }
                State::Initializing(bound) => (bound, None),
                State::Closing(closing) | State::Reconnecting(closing) => (&closing.bound, None),
            };
            (bound.position, frontier)
        };
        if position.connection() != token {
            return None;
        }
        Some(apply(position, frontier))
    }
}

impl<'b, 'd, const ID: u8> Conn<'b, 'd, ID> {
    /// Internal transitions finish before any scheduler callback or owner drop.
    pub(super) fn transition<R>(
        &self,
        apply: impl for<'phase> FnOnce(&'phase mut State<'b, 'd, ID>) -> R,
    ) -> R {
        // SAFETY: Conn is not Sync and this private transition is called only
        // by the closed set of schedule operations. They move owners or update
        // Copy fields without callbacks, allocation, or owner destruction.
        // The callback cannot lend the phase; discarded owners are returned in R.
        unsafe { apply(&mut *self.phase.get()) }
    }

    pub(in crate::port::schedule) fn bind(
        &self,
        position: Position<'b, 'd, route::KeyTag<ID>>,
        ready: ready::Target<'d>,
    ) -> Bind {
        self.transition(|phase| match phase {
            State::Vacant => {
                *phase = State::Initializing(Bound { position, ready });
                Bind::Bound
            }
            _ => Bind::Occupied,
        })
    }

    pub(super) fn mark_ready(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Option<Position<'b, 'd, route::KeyTag<ID>>> {
        self.transition(|phase| match phase {
            State::Initializing(bound) if bound.position.connection() == token => {
                let bound = *bound;
                *phase = State::Shared(bound, [Cutoff::EMPTY; 2]);
                Some(bound.position)
            }
            _ => None,
        })
    }

    pub(in crate::port::schedule) fn access(
        &self,
    ) -> Option<(Position<'b, 'd, route::KeyTag<ID>>, Access)> {
        // SAFETY: this thread-local leaf only returns Copy fields;
        // no callback runs and the phase reference stays within this match.
        match unsafe { &*self.phase.get() } {
            State::Shared(bound, _) => Some((bound.position, Access::Shared)),
            State::Exclusive(bound, _) => Some((bound.position, Access::Exclusive)),
            _ => None,
        }
    }

    pub(in crate::port::schedule) fn with_frontier<R>(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
        apply: impl for<'phase> FnOnce(
            Position<'b, 'd, route::KeyTag<ID>>,
            Access,
            &'phase mut [Cutoff<'b>; 2],
        ) -> R,
    ) -> Option<R> {
        self.transition(|phase| {
            let (bound, access, frontier) = match phase {
                State::Shared(bound, frontier) => (bound, Access::Shared, frontier),
                State::Exclusive(bound, frontier) => (bound, Access::Exclusive, frontier),
                _ => return None,
            };
            if bound.position.connection() != token {
                return None;
            }
            Some(apply(bound.position, access, frontier))
        })
    }

    /// The caller just removed this route from the idle FIFO. Publication checks
    /// both lanes are empty; shared admission and withdrawal unlink that FIFO.
    pub(in crate::port::schedule) fn acquire(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Acquire {
        self.transition(|phase| match phase {
            State::Shared(bound, frontier) if bound.position.connection() == token => {
                *phase = State::Exclusive(*bound, *frontier);
                Acquire::Acquired
            }
            _ => Acquire::Stale,
        })
    }

    /// The affine lease may outlive retirement; only its own live generation becomes shared.
    pub(in crate::port::schedule) fn release(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Release {
        self.transition(|phase| match phase {
            State::Exclusive(bound, frontier) if bound.position.connection() == token => {
                *phase = State::Shared(*bound, *frontier);
                Release::Shared
            }
            _ => Release::Retired,
        })
    }

    pub(in crate::port::schedule) fn failure(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Option<(Position<'b, 'd, route::KeyTag<ID>>, client::Failure<'d>)> {
        // SAFETY: this thread-local observation only retains the same immutable owner;
        // no callback runs and no reference into the phase escapes.
        match unsafe { &*self.phase.get() } {
            State::Closing(closing) | State::Reconnecting(closing)
                if closing.bound.position.connection() == token =>
            {
                Some((
                    closing.bound.position,
                    client::Failure::from(&closing.failure),
                ))
            }
            _ => None,
        }
    }

    pub(in crate::port::schedule) fn retry_reason(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Option<close::Reason> {
        // SAFETY: close_reason only classifies the stored failure into Copy
        // metadata. It does not retain, destroy, or invoke any owner callback.
        match unsafe { &*self.phase.get() } {
            State::Reconnecting(closing) if closing.bound.position.connection() == token => {
                Some(closing.failure.close_reason())
            }
            _ => None,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Bound<'static, 'static, 0>>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Closing<'static, 'static, 0>>() == 11 * mem::size_of::<usize>());
    assert!(mem::size_of::<State<'static, 'static, 0>>() == 12 * mem::size_of::<usize>());
    assert!(mem::size_of::<Conn<'static, 'static, 0>>() == 12 * mem::size_of::<usize>());
};
