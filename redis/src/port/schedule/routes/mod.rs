use std::{cell, mem};

use cartel_core::reply::{layout::Position, order::Cutoff};

use dope::{
    core::driver::{route, schedule::ready},
    manifold::client::{close, conn::Id, session::turns},
};
use o3::{
    alloc, depot,
    depot::{index::Initialized, queue::indexed::shared},
};

use crate::{Owner, client};

mod conn;
pub(in crate::port) mod slot;

pub(crate) use conn::Conn;
use slot::Slot;

/// Counts exactly the shared or exclusive connections owned by these routes.
/// Initializing → Shared adds; Shared/Exclusive → Closing/Reconnecting removes.
/// Each transition consumes its phase, bounding the count by the fixed capacity.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct Active(usize);

pub(crate) struct Routes<'b, 'd, const ID: u8> {
    conns: shared::Fifo<Position<'b, 'd, route::KeyTag<ID>>, Conn<'b, 'd, ID>, 2>,
    next: cell::Cell<usize>,
    active: cell::Cell<Active>,
}

impl<'b, 'd, const ID: u8> Routes<'b, 'd, ID> {
    pub(super) fn try_new(capacity: usize) -> Result<Self, alloc::Error> {
        Ok(Self {
            conns: shared::Fifo::try_new(capacity)?,
            next: cell::Cell::new(0),
            active: cell::Cell::new(Active(0)),
        })
    }

    pub(crate) fn target(&self, token: Id<'d, route::KeyTag<ID>>) -> Option<ready::Target<'d>> {
        self.get(token.index().into_usize())?.target(token)
    }

    pub(super) fn get(&self, lane: usize) -> Option<Slot<'_, 'b, 'd, ID>> {
        self.conns.get(lane).map(Slot)
    }

    pub(super) fn len(&self) -> usize {
        self.conns.capacity()
    }

    pub(super) fn next(&self) -> Option<Slot<'_, 'b, 'd, ID>> {
        let len = self.conns.capacity();
        let index = self.next.get();
        let slot = self.get(index)?;
        self.next.set(if index + 1 == len { 0 } else { index + 1 });
        Some(slot)
    }

    pub(super) fn pop<const P: usize>(&self) -> Option<slot::Bound<'_, 'b, 'd, ID>> {
        self.conns.next::<P>().map(|(slot, position)| slot::Bound {
            slot: Slot(slot),
            position,
        })
    }

    pub(super) fn front<const P: usize>(&self) -> Option<Id<'d, route::KeyTag<ID>>> {
        self.conns.front::<P>().map(Position::connection)
    }

    pub(in crate::port) fn is_active(&self) -> bool {
        self.active.get().0 != 0
    }

    pub(super) fn mark_ready(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
    ) -> Option<slot::Bound<'_, 'b, 'd, ID>> {
        let conn = self.get(token.index().into_usize())?;
        let position = conn.mark_ready(token)?;
        self.active.set(Active(self.active.get().0.wrapping_add(1)));
        Some(slot::Bound {
            slot: conn,
            position,
        })
    }

    pub(super) fn fail(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        fail: client::Failure<'d>,
    ) -> Option<slot::Bound<'_, 'b, 'd, ID>> {
        let conn = self.get(token.index().into_usize())?;
        let position = {
            // SAFETY: this thread-local leaf replaces only Copy Bound variants
            // or a resource-free remote-close cause refined by EOF validation.
            // No callback runs while phase is borrowed; rejected owners remain untouched.
            let phase = unsafe { &mut *conn.phase.get() };
            let bound = match phase {
                State::Closing(closing)
                    if closing.bound.position.connection() == token
                        && matches!(
                            closing.failure,
                            client::Failure::Closed(close::Reason::Remote)
                        )
                        && matches!(
                            fail,
                            client::Failure::Protocol(
                                crate::protocol::Error::Truncated(_)
                                    | crate::protocol::Error::Unparsed { .. }
                            ) | client::Failure::ResponseFrameCapacity(_)
                        ) =>
                {
                    let position = closing.bound.position;
                    closing.failure = fail;
                    return Some(slot::Bound {
                        slot: conn,
                        position,
                    });
                }
                State::Initializing(bound) if bound.position.connection() == token => {
                    let bound = *bound;
                    *phase = State::Closing(Closing {
                        bound,
                        failure: fail,
                    });
                    return Some(slot::Bound {
                        slot: conn,
                        position: bound.position,
                    });
                }
                State::Shared(bound, _) | State::Exclusive(bound, _)
                    if bound.position.connection() == token =>
                {
                    *bound
                }
                _ => return None,
            };
            *phase = State::Closing(Closing {
                bound,
                failure: fail,
            });
            bound.position
        };
        self.active.set(Active(self.active.get().0.wrapping_sub(1)));
        Some(slot::Bound {
            slot: conn,
            position,
        })
    }

    pub(super) fn reconnect(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
        fail: client::Failure<'d>,
    ) -> Option<slot::Bound<'_, 'b, 'd, ID>> {
        let conn = self.get(token.index().into_usize())?;
        let position = {
            // SAFETY: this thread-local leaf replaces only a Copy Bound variant.
            // The phase borrow ends before the external wake or rejected failure drop.
            let phase = unsafe { &mut *conn.phase.get() };
            let bound = match phase {
                State::Shared(bound, _) | State::Exclusive(bound, _)
                    if bound.position.connection() == token =>
                {
                    *bound
                }
                _ => return None,
            };
            *phase = State::Reconnecting(Closing {
                bound,
                failure: fail,
            });
            bound.position
        };
        conn.wake();
        self.active.set(Active(self.active.get().0.wrapping_sub(1)));
        Some(slot::Bound {
            slot: conn,
            position,
        })
    }

    /// The exact endpoint's native retirement consumes its admitted closing slot.
    /// Rejected activations never attach; the immutable root cannot replace this FIFO.
    pub(crate) fn retire<'r, 's, 'storage, 'c, P>(
        &self,
        proof: turns::Drained<'d, route::KeyTag<ID>, Owner<'r, 's, 'd, P, ID>>,
    ) where
        's: 'b,
        'b: 'storage,
        'c: 'b,
        'd: 'c,
        P: client::Root<'d, ID, Port = crate::Port<'s, 'storage, 'b, 'c, 'd, ID>>,
    {
        let index = proof.connection().index().into_usize();
        // SAFETY: the invariant source is this Port domain. Its unique admitted
        // slot remains initialized and closing until this consuming callback;
        // the projection retains the exact FIFO's original shared borrow.
        let live: depot::raw::Proof<_, Initialized<()>> =
            unsafe { depot::raw::Proof::new((&self.conns, index)) };
        let conn = Slot(live.into());
        let previous = conn.transition(|phase| mem::replace(phase, State::Vacant));
        drop(previous);
    }

    pub(super) fn bound<'a, R>(
        &'a self,
        token: Id<'d, route::KeyTag<ID>>,
        apply: impl FnOnce(slot::Bound<'a, 'b, 'd, ID>, Option<[Cutoff<'b>; 2]>) -> R,
    ) -> Option<R> {
        let conn = self.get(token.index().into_usize())?;
        conn.0.state().bound(token, |position, frontier| {
            apply(
                slot::Bound {
                    slot: conn,
                    position,
                },
                frontier,
            )
        })
    }

    pub(super) fn exclusive(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
    ) -> Option<slot::Bound<'_, 'b, 'd, ID>> {
        let conn = self.get(token.index().into_usize())?;
        let (position, access) = conn.access()?;
        (position.connection() == token && access == Access::Exclusive).then_some(slot::Bound {
            slot: conn,
            position,
        })
    }

    pub(super) fn failure(
        &self,
        token: Id<'d, route::KeyTag<ID>>,
    ) -> Option<(slot::Bound<'_, 'b, 'd, ID>, client::Failure<'d>)> {
        let conn = self.get(token.index().into_usize())?;
        let (position, failure) = conn.failure(token)?;
        Some((
            slot::Bound {
                slot: conn,
                position,
            },
            failure,
        ))
    }
}

const _: [(); mem::size_of::<usize>()] = [(); mem::size_of::<Active>()];
const _: [(); 48] = [(); mem::size_of::<Routes<'_, '_, 0>>()];

#[derive(Clone, Copy)]
struct Bound<'b, 'd, const ID: u8> {
    position: Position<'b, 'd, route::KeyTag<ID>>,
    ready: ready::Target<'d>,
}

struct Closing<'b, 'd, const ID: u8> {
    bound: Bound<'b, 'd, ID>,
    failure: client::Failure<'d>,
}

#[derive(Clone, Copy)]
pub(in crate::port::schedule) enum Bind {
    Bound,
    Occupied,
}

#[derive(Clone, Copy)]
pub(in crate::port::schedule) enum Acquire {
    Acquired,
    Stale,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::port::schedule) enum Access {
    Shared,
    Exclusive,
}

enum State<'b, 'd, const ID: u8> {
    Vacant,
    Initializing(Bound<'b, 'd, ID>),
    Shared(Bound<'b, 'd, ID>, [Cutoff<'b>; 2]),
    Exclusive(Bound<'b, 'd, ID>, [Cutoff<'b>; 2]),
    Closing(Closing<'b, 'd, ID>),
    Reconnecting(Closing<'b, 'd, ID>),
}

impl<'b, 'd, const ID: u8> State<'b, 'd, ID> {
    fn bound(&self) -> Option<Bound<'b, 'd, ID>> {
        match self {
            State::Vacant => None,
            State::Initializing(bound) | State::Shared(bound, _) | State::Exclusive(bound, _) => {
                Some(*bound)
            }
            State::Closing(closing) | State::Reconnecting(closing) => Some(closing.bound),
        }
    }
}
