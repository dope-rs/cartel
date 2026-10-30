use std::{cell, num};

use dope::{
    core::driver::{route::KeyTag, schedule::ready},
    manifold::client::{close, conn},
};
use o3::base::num::bounded;

use crate::{
    port::{
        Disposition, Retirement,
        pool::{access, key, phases},
        replies, requests,
    },
    protocol,
};

type Pending = bounded::U32<0, { u32::MAX / 2 + 1 }>;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Sync {
    Synced,
    Automatic(num::NonZero<u32>),
    Explicit(num::NonZero<u32>),
}

#[derive(Clone, Copy)]
pub(crate) struct Bound<'d, const ID: u8> {
    pub(super) token: conn::Id<'d, KeyTag<ID>>,
    pub(super) ready: ready::Target<'d>,
}

#[derive(Clone, Copy)]
pub(crate) enum Bind {
    Bound,
    Occupied,
}

pub(crate) enum Settle {
    Applied,
    Closing,
    Empty,
    Stale,
}

pub(crate) enum Egress {
    Open,
    Close(close::Directive),
}

#[derive(Clone, Copy)]
pub(super) enum Phase<'d, const ID: u8> {
    Vacant,
    Handshaking(Bound<'d, ID>),
    Ready {
        bound: Bound<'d, ID>,
        access: access::State,
    },
    Closing {
        bound: Bound<'d, ID>,
        failure: replies::Failure,
        disposition: Disposition,
    },
}

impl<'d, const ID: u8> Phase<'d, ID> {
    fn bound(self) -> Option<Bound<'d, ID>> {
        match self {
            Self::Vacant => None,
            Self::Handshaking(bound) | Self::Ready { bound, .. } | Self::Closing { bound, .. } => {
                Some(bound)
            }
        }
    }
}

impl Sync {
    fn try_next(self, edge: requests::Boundary) -> Result<Self, crate::Error> {
        match (self, edge) {
            (Self::Synced, requests::Boundary::Close | requests::Boundary::Commit)
            | (Self::Synced, requests::Boundary::Group)
            | (Self::Automatic(_), requests::Boundary::Close | requests::Boundary::Commit) => {
                Ok(Self::Synced)
            }
            (Self::Automatic(depth) | Self::Explicit(depth), requests::Boundary::Group) => {
                Err(crate::Error::GroupOpen { depth })
            }
            (Self::Synced, requests::Boundary::Open) => {
                Ok(Self::Automatic(num::NonZero::<u32>::MIN))
            }
            (Self::Automatic(depth), requests::Boundary::Open) => {
                let depth = depth
                    .checked_add(1)
                    .ok_or(crate::Error::RequestDepthOverflow { depth })?;
                Ok(Self::Automatic(depth))
            }
            (Self::Synced, requests::Boundary::External) => {
                Ok(Self::Explicit(num::NonZero::<u32>::MIN))
            }
            (Self::Explicit(depth), requests::Boundary::Close | requests::Boundary::Commit) => {
                Err(crate::Error::RequestCloseDuringCopy { depth })
            }
            (Self::Explicit(depth), requests::Boundary::Open) => {
                Err(crate::Error::RequestOpenDuringCopy { depth })
            }
            (Self::Automatic(depth), requests::Boundary::External) => {
                Err(crate::Error::CopyDuringPipeline { depth })
            }
            (Self::Explicit(depth), requests::Boundary::External) => {
                Err(crate::Error::CopyAlreadyOpen { depth })
            }
        }
    }
}

pub(crate) struct Slot<'d, const ID: u8> {
    pub(super) phase: phases::State<'d, ID>,
    pub(super) key: cell::Cell<key::Key>,
    pub(super) pid: cell::Cell<Option<i32>>,
    sync: cell::Cell<Sync>,
    batch: cell::Cell<replies::Batch>,
    pending: cell::Cell<Pending>,
}

/// One connection proven ready to accept a PostgreSQL request.
pub(crate) struct Ready<'a, 'd, const ID: u8> {
    slot: &'a Slot<'d, ID>,
}

pub(crate) struct Close<'d, const ID: u8> {
    pub(crate) state: Closed<'d, ID>,
    pub(crate) failure: replies::Failure,
}

#[repr(u8)]
pub(crate) enum Closed<'d, const ID: u8> {
    Handshaking,
    Shared,
    Reserved(access::Transaction<'d, ID>),
    Held(access::Transaction<'d, ID>),
    Finalizing(access::Transaction<'d, ID>),
    Draining,
    Closing,
}

pub(crate) enum Flush<'a> {
    Idle,
    Pending(Permit<'a>),
}

pub(crate) enum Change {
    Available,
    Unchanged,
}

#[must_use = "a PostgreSQL reply sync transition must be committed or discarded"]
pub(crate) struct Permit<'a> {
    sync: &'a cell::Cell<Sync>,
    next: Sync,
    batch: &'a cell::Cell<replies::Batch>,
    current: replies::Batch,
    boundary: requests::Boundary,
    pending: &'a cell::Cell<Pending>,
    queued: Pending,
}

impl Permit<'_> {
    pub(crate) const fn batch(&self) -> replies::Batch {
        self.current
    }

    /// Publication admits an order, or an existing automatic/COPY order owns its Sync.
    /// Future Syncs retain an order; only the current Sync may outlive its orders.
    /// This synchronous slot transition retains at most u32::MAX / 2 orders plus one Sync.
    pub(in crate::port) fn commit(self) -> Change {
        let change =
            if matches!(self.next, Sync::Synced) && !matches!(self.sync.get(), Sync::Synced) {
                Change::Available
            } else {
                Change::Unchanged
            };
        self.sync.set(self.next);
        let queued = match self.boundary {
            requests::Boundary::Close | requests::Boundary::Commit | requests::Boundary::Group => {
                self.batch.set(self.current.next());
                let count = self.queued.get() + 1;
                // SAFETY: consumed Sync admission retains at most one pending Sync
                // per live order plus the single currently parsed Sync.
                unsafe { bounded::raw::Proof::assume(count).retain() }
            }
            requests::Boundary::Open | requests::Boundary::External => self.queued,
        };
        self.pending.set(queued);
        change
    }
}

impl<'a, 'd, const ID: u8> Ready<'a, 'd, ID> {
    pub(crate) fn try_reply(
        self,
        boundary: requests::Boundary,
    ) -> Result<Permit<'a>, crate::Error> {
        let next = self.slot.sync.get().try_next(boundary)?;
        Ok(self.slot.permit(next, boundary))
    }

    pub(crate) fn try_finish(self) -> Result<Permit<'a>, crate::Error> {
        match self.slot.sync.get() {
            Sync::Explicit(_) => {}
            Sync::Synced => return Err(crate::Error::CopyFinishWithoutStart),
            Sync::Automatic(depth) => {
                return Err(crate::Error::CopyFinishDuringPipeline { depth });
            }
        }
        Ok(self.slot.permit(Sync::Synced, requests::Boundary::Close))
    }

    pub(crate) fn wake(self) {
        self.slot.wake();
    }
}

impl<'d, const ID: u8> Slot<'d, ID> {
    pub(crate) fn is_synced(&self) -> bool {
        matches!(self.sync.get(), Sync::Synced)
    }

    pub(crate) fn new() -> Self {
        Self {
            phase: phases::State::new(),
            key: cell::Cell::new(key::Key::empty()),
            pid: cell::Cell::new(None),
            sync: cell::Cell::new(Sync::Synced),
            batch: cell::Cell::new(replies::Batch::INITIAL),
            pending: cell::Cell::new(Pending::exact::<0>()),
        }
    }

    pub(crate) fn bind(&self, token: conn::Id<'d, KeyTag<ID>>, ready: ready::Target<'d>) -> Bind {
        if !matches!(self.phase.get(), Phase::Vacant) {
            return Bind::Occupied;
        }
        self.sync.set(Sync::Synced);
        self.batch.set(replies::Batch::INITIAL);
        self.pending.set(Pending::exact::<0>());
        self.phase.set(Phase::Handshaking(Bound { token, ready }));
        Bind::Bound
    }

    pub(crate) fn try_ready(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> Result<(), protocol::Reason> {
        match self.phase.get() {
            Phase::Handshaking(bound) if bound.token == token => {
                self.sync.set(Sync::Synced);
                self.phase.set(Phase::Ready {
                    bound,
                    access: access::State::Shared(access::Sequence::INITIAL),
                });
                Ok(())
            }
            _ => Err(protocol::Reason::PrepareReady),
        }
    }

    pub(crate) fn matches(&self, token: conn::Id<'d, KeyTag<ID>>) -> bool {
        self.phase
            .get()
            .bound()
            .is_some_and(|bound| bound.token == token)
    }

    pub(crate) fn ready(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> Result<Ready<'_, 'd, ID>, crate::Error> {
        match self.phase.get() {
            Phase::Ready { bound, access }
                if bound.token == token && !matches!(access, access::State::Draining) =>
            {
                Ok(Ready { slot: self })
            }
            _ => Err(crate::Error::Closed),
        }
    }

    pub(crate) fn barriers(&self) -> usize {
        let open = matches!(self.phase.get(), Phase::Ready { .. })
            && matches!(self.sync.get(), Sync::Automatic(_) | Sync::Explicit(_));
        self.pending.get().into_usize() + usize::from(open)
    }

    pub(crate) fn settle(&self, token: conn::Id<'d, KeyTag<ID>>) -> Settle {
        match self.phase.get() {
            Phase::Ready { bound, .. } if bound.token == token => {}
            Phase::Closing { bound, .. } if bound.token == token => return Settle::Closing,
            _ => return Settle::Stale,
        }
        let Some(next) = self.pending.get().get().checked_sub(1) else {
            return Settle::Empty;
        };
        // SAFETY: checked subtraction preserves the retained inclusive upper bound.
        self.pending
            .set(unsafe { bounded::raw::Proof::assume(next).retain() });
        Settle::Applied
    }

    pub(crate) fn egress(&self) -> Egress {
        match self.phase.get() {
            Phase::Closing {
                failure: cause,
                disposition: Disposition::Reconnect,
                ..
            } => Egress::Close(close::Directive::cut_retry(cause.close_reason())),
            Phase::Closing {
                failure: cause,
                disposition: Disposition::Permanent,
                ..
            } => Egress::Close(close::Directive::cut_final(cause.close_reason())),
            _ => Egress::Open,
        }
    }

    pub(crate) fn wake(&self) {
        if let Some(bound) = self.phase.get().bound() {
            bound.ready.wake();
        }
    }

    pub(crate) fn try_close(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        cause: replies::Failure,
        fate: Disposition,
    ) -> Option<Close<'d, ID>> {
        let (bound, state, cause, fate) = match self.phase.get() {
            Phase::Handshaking(bound) if bound.token == token => {
                (bound, Closed::Handshaking, cause, fate)
            }
            Phase::Ready {
                bound,
                access: state,
            } if bound.token == token => {
                let state = match state {
                    access::State::Shared(_) => Closed::Shared,
                    access::State::Reserved(value) => Closed::Reserved(access::Transaction {
                        conn: token,
                        generation: value,
                    }),
                    access::State::Held(value) => Closed::Held(access::Transaction {
                        conn: token,
                        generation: value,
                    }),
                    access::State::Finalizing(value) => Closed::Finalizing(access::Transaction {
                        conn: token,
                        generation: value,
                    }),
                    access::State::Draining => Closed::Draining,
                };
                (bound, state, cause, fate)
            }
            Phase::Closing {
                bound,
                failure,
                disposition: old,
            } if bound.token == token => {
                let fate = if matches!(fate, Disposition::Permanent) {
                    Disposition::Permanent
                } else {
                    old
                };
                (bound, Closed::Closing, failure, fate)
            }
            _ => return None,
        };
        self.pending.set(Pending::exact::<0>());
        self.pid.set(None);
        self.key.set(key::Key::empty());
        self.phase.set(Phase::Closing {
            bound,
            failure: cause,
            disposition: fate,
        });
        Some(Close {
            state,
            failure: cause,
        })
    }

    pub(super) fn try_retire(&self, token: conn::Id<'d, KeyTag<ID>>) -> Result<(), crate::Error> {
        let phase = match self.phase.get() {
            Phase::Closing { bound, .. } if bound.token == token => {
                self.phase.set(Phase::Vacant);
                return Ok(());
            }
            Phase::Handshaking(bound) if bound.token == token => Retirement::Handshaking,
            Phase::Ready { bound, access } if bound.token == token => match access {
                access::State::Shared(_) => Retirement::Shared,
                access::State::Reserved(_) => Retirement::Reserved,
                access::State::Held(_) => Retirement::Held,
                access::State::Finalizing(_) => Retirement::Finalizing,
                access::State::Draining => Retirement::Draining,
            },
            _ => return Ok(()),
        };
        Err(crate::Error::Retirement {
            slot: token.index(),
            phase,
        })
    }

    pub(crate) fn flush(&self, token: conn::Id<'d, KeyTag<ID>>) -> Flush<'_> {
        let Phase::Ready { bound, .. } = self.phase.get() else {
            return Flush::Idle;
        };
        if bound.token != token || !matches!(self.sync.get(), Sync::Automatic(_)) {
            return Flush::Idle;
        }
        Flush::Pending(self.permit(Sync::Synced, requests::Boundary::Close))
    }

    fn permit(&self, next: Sync, boundary: requests::Boundary) -> Permit<'_> {
        let current = self.batch.get();
        Permit {
            sync: &self.sync,
            next,
            batch: &self.batch,
            current,
            boundary,
            pending: &self.pending,
            queued: self.pending.get(),
        }
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Closed<'static, 0>>()
            == std::mem::size_of::<access::Transaction<'static, 0>>()
                + std::mem::align_of::<access::Transaction<'static, 0>>()
    );
    assert!(
        std::mem::size_of::<Close<'static, 0>>()
            == std::mem::size_of::<Closed<'static, 0>>() + std::mem::size_of::<replies::Failure>()
    );
    assert!(std::mem::size_of::<Slot<'static, 0>>() == 17 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Permit<'static>>() <= 7 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Ready<'static, 'static, 0>>() == std::mem::size_of::<&()>());
};
