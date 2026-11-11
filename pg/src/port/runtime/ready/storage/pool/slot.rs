use std::{cell, mem::MaybeUninit, num};

use cartel_core::queue;
use dope::{
    core::driver::{route::KeyTag, schedule::ready},
    manifold::client::{close, conn},
    net::link::egress,
};
use o3::base::{cell::region, num::bounded};
use o3::depot::{self, index::Initialized};

use crate::{
    port::{Disposition, Retirement, pool::key, replies, requests},
    protocol,
};

pub(crate) mod access;

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

pub(crate) enum Egress {
    Open,
    Close(close::Directive),
}

#[derive(Clone, Copy)]
pub(super) enum Lifecycle {
    Handshaking,
    Ready {
        access: access::State,
        receiving: bool,
    },
    Closing {
        failure: replies::Failure,
        disposition: Disposition,
    },
}

impl Sync {
    fn try_next(self, edge: requests::Boundary) -> Result<Self, crate::Error> {
        match (self, edge) {
            (
                Self::Synced,
                requests::Boundary::Close | requests::Boundary::Begin | requests::Boundary::Commit,
            )
            | (Self::Synced, requests::Boundary::Group)
            | (
                Self::Automatic(_),
                requests::Boundary::Close | requests::Boundary::Begin | requests::Boundary::Commit,
            ) => Ok(Self::Synced),
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
            (
                Self::Explicit(depth),
                requests::Boundary::Close | requests::Boundary::Begin | requests::Boundary::Commit,
            ) => Err(crate::Error::RequestCloseDuringCopy { depth }),
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
    bound: cell::Cell<MaybeUninit<Bound<'d, ID>>>,
    phase: cell::Cell<Option<Lifecycle>>,
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

#[must_use = "a PostgreSQL reply sync transition must be committed or discarded"]
pub(crate) struct Permit<'a, 'd, const ID: u8> {
    slot: &'a Slot<'d, ID>,
    next: Sync,
    current: replies::Batch,
}

impl<'a, 'd, const ID: u8> Permit<'a, 'd, ID> {
    pub(in crate::port) const fn slot(&self) -> &'a Slot<'d, ID> {
        self.slot
    }

    pub(in crate::port) const fn batch(&self) -> replies::Batch {
        self.current
    }

    /// Publication admits an order, or an existing automatic/COPY order owns its Sync.
    /// Future Syncs retain an order; only the current Sync may outlive its orders.
    /// This synchronous slot transition retains at most u32::MAX / 2 orders plus one Sync.
    pub(in crate::port) fn commit(self) -> bool {
        let unchanged =
            !matches!(self.next, Sync::Synced) || matches!(self.slot.sync.get(), Sync::Synced);
        self.slot.sync.set(self.next);
        if matches!(self.next, Sync::Synced) {
            self.slot.batch.set(self.current.next());
            let count = self.slot.pending.get().get() + 1;
            // SAFETY: consumed Sync admission retains at most one pending Sync
            // per live order plus the single currently parsed Sync.
            self.slot.pending.set(unsafe { bounded::raw::u32(count) });
        }
        unchanged
    }
}

impl<'a, 'd, const ID: u8> Ready<'a, 'd, ID> {
    pub(crate) const fn reborrow(&self) -> Self {
        Self { slot: self.slot }
    }

    pub(crate) fn listed(listed: &protocol::runtime::ready::Listed<'a, 'd, ID>) -> Self {
        Self {
            slot: listed.slot(),
        }
    }

    pub(crate) fn reserved(proof: &access::Reservation<'a, 'd, ID>) -> Self {
        Self { slot: proof.slot }
    }

    pub(crate) fn connection(&self) -> conn::Id<'d, KeyTag<ID>> {
        // SAFETY: every Ready constructor retains its original initialized binding.
        Slot::bound(unsafe { depot::raw::Proof::new(self.slot) }).token
    }

    pub(crate) fn try_reply(
        self,
        boundary: requests::Boundary,
    ) -> Result<Permit<'a, 'd, ID>, crate::Error> {
        let next = self.slot.sync.get().try_next(boundary)?;
        Ok(self.slot.permit(next))
    }

    pub(crate) fn try_finish(self) -> Result<Permit<'a, 'd, ID>, crate::Error> {
        match self.slot.sync.get() {
            Sync::Explicit(_) => {}
            Sync::Synced => return Err(crate::Error::CopyFinishWithoutStart),
            Sync::Automatic(depth) => {
                return Err(crate::Error::CopyFinishDuringPipeline { depth });
            }
        }
        Ok(self.slot.permit(Sync::Synced))
    }

    pub(crate) fn wake(self) {
        self.slot.wake();
    }
}

impl<'d, const ID: u8> Slot<'d, ID> {
    pub(crate) fn is_receiving(&self) -> bool {
        matches!(
            self.phase.get(),
            Some(Lifecycle::Ready {
                receiving: true,
                ..
            })
        )
    }

    fn set_access(&self, access: access::State) {
        self.phase.set(Some(Lifecycle::Ready {
            access,
            receiving: self.is_receiving(),
        }));
    }

    pub(super) fn snapshot(&self) -> Option<(Bound<'d, ID>, Lifecycle)> {
        let phase = self.phase.get()?;
        // SAFETY: bind initializes the binding before publishing a live phase.
        // Retirement leaves it initialized until the next binding replaces it.
        Some((unsafe { self.bound.get().assume_init() }, phase))
    }

    fn bound(proof: depot::raw::Proof<&Self, Initialized<Bound<'d, ID>>>) -> Bound<'d, ID> {
        // SAFETY: the synchronous Ready proof retains this exact initialized binding.
        unsafe { proof.into_inner().bound.get().assume_init() }
    }

    pub(crate) fn is_synced(&self) -> bool {
        matches!(self.sync.get(), Sync::Synced)
    }

    pub(crate) fn can_begin(&self) -> bool {
        !matches!(self.sync.get(), Sync::Automatic(_))
    }

    pub(crate) fn new() -> Self {
        Self {
            bound: cell::Cell::new(MaybeUninit::uninit()),
            phase: cell::Cell::new(None),
            key: cell::Cell::new(key::Key::empty()),
            pid: cell::Cell::new(None),
            sync: cell::Cell::new(Sync::Synced),
            batch: cell::Cell::new(replies::Batch::INITIAL),
            pending: cell::Cell::new(Pending::exact::<0>()),
        }
    }

    pub(crate) fn bind(&self, token: conn::Id<'d, KeyTag<ID>>, ready: ready::Target<'d>) -> Bind {
        if self.phase.get().is_some() {
            return Bind::Occupied;
        }
        self.batch.set(replies::Batch::INITIAL);
        self.bound.set(MaybeUninit::new(Bound { token, ready }));
        self.phase.set(Some(Lifecycle::Handshaking));
        Bind::Bound
    }

    pub(crate) fn try_ready(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> Result<(), protocol::Reason> {
        match self.snapshot() {
            Some((bound, Lifecycle::Handshaking)) if bound.token == token => {
                self.phase.set(Some(Lifecycle::Ready {
                    access: access::State::Shared(access::Sequence::INITIAL),
                    receiving: false,
                }));
                Ok(())
            }
            _ => Err(protocol::Reason::PrepareReady),
        }
    }

    pub(crate) fn matches(&self, token: conn::Id<'d, KeyTag<ID>>) -> bool {
        self.snapshot()
            .is_some_and(|(bound, _)| bound.token == token)
    }

    pub(crate) fn ready(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> Result<Ready<'_, 'd, ID>, crate::Error> {
        match self.snapshot() {
            Some((bound, Lifecycle::Ready { access, .. }))
                if bound.token == token && !matches!(access, access::State::Draining) =>
            {
                Ok(Ready { slot: self })
            }
            _ => Err(crate::Error::Closed),
        }
    }

    pub(crate) fn select(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        generation: Option<access::Generation>,
    ) -> Option<Ready<'_, 'd, ID>> {
        let Some((bound, Lifecycle::Ready { access, .. })) = self.snapshot() else {
            return None;
        };
        if bound.token != token {
            return None;
        }
        match (access, generation) {
            (access::State::Shared(_), None) => Some(Ready { slot: self }),
            (access::State::Held(current), Some(generation)) if current == generation => {
                Some(Ready { slot: self })
            }
            _ => None,
        }
    }

    pub(crate) fn barriers(&self) -> usize {
        self.pending.get().into_usize() + usize::from(!self.is_synced())
    }

    pub(super) fn try_settle(&self) -> Option<()> {
        let next = self.pending.get().get().checked_sub(1)?;
        // SAFETY: checked subtraction preserves the retained inclusive upper bound.
        self.pending.set(unsafe { bounded::raw::u32(next) });
        Some(())
    }

    pub(crate) fn egress(&self) -> Egress {
        match self.phase.get() {
            Some(Lifecycle::Closing {
                failure: cause,
                disposition: Disposition::Reconnect,
            }) => Egress::Close(close::Directive::cut_retry(cause.close_reason())),
            Some(Lifecycle::Closing {
                failure: cause,
                disposition: Disposition::Permanent,
            }) => Egress::Close(close::Directive::cut_final(cause.close_reason())),
            _ => Egress::Open,
        }
    }

    pub(crate) fn wake(&self) {
        if let Some((bound, _)) = self.snapshot() {
            bound.ready.wake();
        }
    }

    pub(crate) fn try_close(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        cause: &mut replies::Failure,
        fate: Disposition,
    ) -> Option<Closed<'d, ID>> {
        let (state, fate) = match self.snapshot() {
            Some((bound, Lifecycle::Handshaking)) if bound.token == token => {
                (Closed::Handshaking, fate)
            }
            Some((bound, Lifecycle::Ready { access: state, .. })) if bound.token == token => {
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
                (state, fate)
            }
            Some((
                bound,
                Lifecycle::Closing {
                    failure,
                    disposition: old,
                },
            )) if bound.token == token => {
                *cause = failure;
                let fate = if matches!(fate, Disposition::Permanent) {
                    Disposition::Permanent
                } else {
                    old
                };
                (Closed::Closing, fate)
            }
            _ => return None,
        };
        self.sync.set(Sync::Synced);
        self.pending.set(Pending::exact::<0>());
        self.pid.set(None);
        self.key.set(key::Key::empty());
        self.phase.set(Some(Lifecycle::Closing {
            failure: *cause,
            disposition: fate,
        }));
        Some(state)
    }

    pub(super) fn try_retire(&self, token: conn::Id<'d, KeyTag<ID>>) -> Result<(), crate::Error> {
        let phase = match self.snapshot() {
            Some((bound, Lifecycle::Closing { .. })) if bound.token == token => {
                self.phase.set(None);
                return Ok(());
            }
            Some((bound, Lifecycle::Handshaking)) if bound.token == token => {
                Retirement::Handshaking
            }
            Some((bound, Lifecycle::Ready { access, .. })) if bound.token == token => {
                match access {
                    access::State::Shared(_) => Retirement::Shared,
                    access::State::Reserved(_) => Retirement::Reserved,
                    access::State::Held(_) => Retirement::Held,
                    access::State::Finalizing(_) => Retirement::Finalizing,
                    access::State::Draining => Retirement::Draining,
                }
            }
            _ => return Ok(()),
        };
        Err(crate::Error::Retirement {
            slot: token.index(),
            phase,
        })
    }

    fn permit(&self, next: Sync) -> Permit<'_, 'd, ID> {
        Permit {
            slot: self,
            next,
            current: self.batch.get(),
        }
    }
}

impl<'d, const ID: u8> protocol::runtime::ready::Entry<'_, 'd, ID> {
    pub(crate) fn try_flush<E>(
        self,
        requests: queue::Lane<'_, 'd, egress::NonEmpty<requests::Frame<'d>>>,
        region: &mut region::Token<region::Identity<'d>>,
        send: impl FnOnce(&mut region::Token<region::Identity<'d>>) -> Result<(), E>,
    ) -> Result<(), E> {
        let slot = self.slot();
        if !matches!(slot.phase.get(), Some(Lifecycle::Ready { .. }))
            || !matches!(slot.sync.get(), Sync::Automatic(_))
        {
            return Ok(());
        }
        let sync = slot.permit(Sync::Synced);
        if !requests.is_empty(region) {
            return Ok(());
        }
        send(region)?;
        if !sync.commit() {
            self.core.wake_ready();
        }
        Ok(())
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Closed<'static, 0>>()
            == std::mem::size_of::<access::Transaction<'static, 0>>()
                + std::mem::align_of::<access::Transaction<'static, 0>>()
    );
    assert!(std::mem::size_of::<Slot<'static, 0>>() == 17 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Permit<'static, 'static, 0>>() <= 7 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Ready<'static, 'static, 0>>() == std::mem::size_of::<&()>());
};
