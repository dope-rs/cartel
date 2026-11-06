//! Admission-owned diagnostic storage for explicitly synchronized groups.

use std::{cell::Cell, mem::MaybeUninit, num::NonZero, slice};

use cartel_core::reply::{retain, usage};
use o3::{
    alloc::{self, array},
    buffer::{self, pool},
    depot::{
        arena::pool::{Shared, Unique, recycle, shared},
        capacity,
    },
    mem::rc,
};

use crate::{
    api::results::group::Position,
    port::config::{Error, Groups},
    reply::{self, group},
};

use crate::reply::group::Resolution;

pub(crate) struct Active<'r, 's> {
    records: Unique<'r, group::Records<'s>, recycle::Header<group::Records<'s>>>,
    bytes: pool::Span,
}

pub(crate) struct State<'r, 's, 'b, 'c, 'd> {
    limits: (NonZero<usize>, NonZero<usize>),
    active: Cell<Option<Active<'r, 's>>>,
    reservation: Cell<Option<retain::Reservation<'b, 'c, 'd>>>,
}

type Header<'r, 's, 'b, 'c, 'd> = shared::Header<State<'r, 's, 'b, 'c, 'd>>;
pub(crate) struct Owner<'g, 'r, 's, 'b, 'c, 'd> {
    state: Shared<'g, State<'r, 's, 'b, 'c, 'd>>,
}

/// The sole admission authority for a newly initialized, unpublished collector.
#[repr(transparent)]
pub(crate) struct Draft<'g, 'r, 's, 'b, 'c, 'd>(Owner<'g, 'r, 's, 'b, 'c, 'd>);

pub(crate) struct Access<'g, 'r, 's, 'b, 'c, 'd> {
    bytes: pool::Pool,
    records: &'r recycle::Header<group::Records<'s>>,
    owners: &'g Header<'r, 's, 'b, 'c, 'd>,
    limits: (NonZero<usize>, NonZero<usize>),
}

impl<'g, 'r, 's, 'b, 'c, 'd> Access<'g, 'r, 's, 'b, 'c, 'd> {
    pub(crate) const fn new(
        bytes: pool::Pool,
        records: &'r recycle::Header<group::Records<'s>>,
        owners: &'g Header<'r, 's, 'b, 'c, 'd>,
        limits: Groups,
    ) -> Self {
        Self {
            bytes,
            records,
            owners,
            limits: (limits.records, limits.bytes),
        }
    }

    pub(crate) fn prepare(&self) -> Result<Draft<'g, 'r, 's, 'b, 'c, 'd>, crate::Error> {
        let owner = self
            .owners
            .vacancy()
            .ok_or(crate::Error::GroupOwnerCapacity)?;
        let mut records = self
            .records
            .take()
            .ok_or(crate::Error::GroupRecordCapacity)?;
        let bytes = self
            .bytes
            .acquire()
            .ok_or(crate::Error::GroupBytesCapacity)?;
        records.get_mut().get_mut().clear();
        let active = Cell::new(Some(Active { records, bytes }));
        Ok(Draft(Owner {
            state: owner.reserve().commit(State {
                limits: self.limits,
                active,
                reservation: Cell::new(None),
            }),
        }))
    }
}

impl<'g, 'r, 's, 'b, 'c, 'd> Draft<'g, 'r, 's, 'b, 'c, 'd> {
    pub(crate) fn usage(&self) -> usage::Usage {
        let (records, bytes) = self.0.state.limits;
        usage::Usage::new(
            usage::Bytes::new(bytes.get()),
            usage::Values::new(records.get()),
        )
    }

    pub(crate) fn admit(
        self,
        permit: retain::Reservation<'b, 'c, 'd>,
    ) -> Owner<'g, 'r, 's, 'b, 'c, 'd> {
        self.0.state.reservation.set(Some(permit));
        self.0
    }

    /// Clones are retained privately by the pending API attachment until admission.
    pub(crate) fn try_clone(&self) -> Result<Owner<'g, 'r, 's, 'b, 'c, 'd>, rc::Error> {
        self.0.try_clone()
    }
}

impl<'g, 'r, 's, 'b, 'c, 'd> Owner<'g, 'r, 's, 'b, 'c, 'd> {
    pub(crate) fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self {
            state: self.state.try_clone()?,
        })
    }

    pub(crate) fn record<R: buffer::Cursor>(
        &self,
        position: Position,
        plan: reply::Plan<'_, 'd, R>,
    ) -> Result<(), crate::Error> {
        let Some(mut active) = self.state.active.take() else {
            return Ok(());
        };
        let result = active.record(position, plan, self.state.limits.0);
        self.state.active.set(Some(active));
        result
    }

    pub(crate) fn snapshot(&self) -> group::Ledger<'r, 's> {
        let Some(active) = self.state.active.take() else {
            return group::Ledger::empty();
        };
        let Active { records, bytes } = active;
        drop(self.state.reservation.take());
        if records.get().get_ref().len() == 0 {
            return group::Ledger::empty();
        }
        group::Ledger {
            inner: Some(group::Retained {
                records,
                bytes: bytes.freeze().into_shared(),
            }),
        }
    }

    pub(crate) fn resolve(&self, position: Position) -> Resolution {
        let Some(mut active) = self.state.active.take() else {
            return Resolution::Unchanged;
        };
        let resolved = active.records.get_mut().get_mut().resolve(position);
        self.state.active.set(Some(active));
        resolved
    }
}

impl Active<'_, '_> {
    fn record<'d, R: buffer::Cursor>(
        &mut self,
        position: Position,
        plan: reply::Plan<'_, 'd, R>,
        capacity: NonZero<usize>,
    ) -> Result<(), crate::Error> {
        let records = self.records.get_mut().get_mut();
        let Some(record) = records.vacancy() else {
            return Err(crate::Error::GroupDiagnosticRecords { capacity });
        };
        // Span retains both initialized lengths as u32 in its owned lease.
        let start = self.bytes.len() as u32;
        let append = self
            .bytes
            .try_append(plan.as_slice())
            .map_err(crate::Error::GroupDiagnosticBytes)?;
        append.commit();
        record.commit(group::Record {
            position,
            start,
            end: self.bytes.len() as u32,
        });
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Layout {
    limits: Groups,
    groups: capacity::Count,
    records: array::Plan<group::Record, NonZero<usize>>,
    bytes: pool::Layout,
}

/// The exact admitted group partition and its original uninitialized backing.
pub(crate) struct Backing {
    layout: Layout,
    records: array::Block<MaybeUninit<group::Record>, NonZero<usize>>,
}

impl Backing {
    pub(crate) fn pool(
        &mut self,
    ) -> Result<(recycle::Store<group::Records<'_>>, &Layout), alloc::Error> {
        let records = self.records.as_mut_ptr();
        let width = self.layout.limits.records.get();
        let pool = recycle::Store::try_from_fn(self.layout.groups, |index| {
            // SAFETY: this backing was allocated from its own checked
            // groups * width plan. The pool constructor invokes each admitted
            // index exactly once, so these complete chunks are disjoint and
            // retain the exclusive borrow of the original backing.
            let chunk = unsafe {
                let offset = index as usize * width;
                slice::from_raw_parts_mut(records.add(offset), width)
            };
            group::Records::new(chunk)
        })?;
        Ok((pool, &self.layout))
    }
}

impl Layout {
    pub(crate) const fn limits(self) -> Groups {
        self.limits
    }

    pub(crate) const fn groups(self) -> capacity::Count {
        self.groups
    }

    pub(crate) const fn bytes(self) -> pool::Layout {
        self.bytes
    }

    pub(crate) const fn allocated(self) -> usize {
        self.bytes.bytes()
    }

    pub(crate) fn try_records(self) -> Result<Backing, alloc::Exhausted> {
        Ok(Backing {
            layout: self,
            records: self.records.try_allocate(())?,
        })
    }

    pub(crate) fn try_new(limits: Groups) -> Result<Self, Error> {
        let groups = capacity::Count::try_from(limits.active.get()).map_err(Error::GroupSlots)?;
        let records =
            limits
                .active
                .checked_mul(limits.records)
                .ok_or(Error::GroupRecordsOverflow {
                    groups: limits.active,
                    records: limits.records,
                })?;
        let records = array::Plan::try_new(records).map_err(Error::GroupRecordsLayout)?;
        limits
            .active
            .get()
            .checked_mul(limits.bytes.get())
            .ok_or(Error::GroupBytesOverflow {
                groups: limits.active,
                bytes: limits.bytes,
            })?;
        let bytes =
            pool::Layout::new(limits.active.get(), limits.bytes.get()).map_err(Error::GroupPool)?;
        Ok(Self {
            limits,
            groups,
            records,
            bytes,
        })
    }
}
