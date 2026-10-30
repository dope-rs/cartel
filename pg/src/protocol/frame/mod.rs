use std::ops;

use dope::manifold::client::session::codec::Input;
use o3::buffer::{self, bytes::bounded, hold, hold::scope, resident, storage::range};

use crate::protocol::Wire;

mod codec;
pub use codec::Codec;

pub enum Head<'input, 'd, R: buffer::Cursor> {
    Message(Message<'input, 'd, R>),
    Fatal(crate::Error),
}

/// A decoded message retaining its exact input projection.
pub struct Message<'input, 'd, R: buffer::Cursor> {
    pub typ: u8,
    pub payload: Payload<'input, 'd, R>,
}

/// The original retained range, selected with endpoints already bounded by u32.
#[repr(transparent)]
pub struct Payload<'input, 'd, R: buffer::Cursor>(scope::Deferred<'input, 'd, R>);

/// Immutable retained bytes carrying their original frame's extent admission.
#[repr(transparent)]
pub(crate) struct Retained<'d>(resident::Shared<'d>);

impl<'input, 'd, R: buffer::Cursor> Payload<'input, 'd, R> {
    pub(super) fn plan(
        input: &Input<'_, 'input, 'd, R>,
        range: ops::Range<u32>,
    ) -> Result<Self, range::Error> {
        input
            .try_plan(range.start as usize..range.end as usize)
            .map(Self)
    }

    pub fn as_slice(&self) -> &'input [u8] {
        self.0.as_slice()
    }

    pub(crate) fn wire(&self) -> Wire<'input>
    where
        R: hold::raw::Retain<'d>,
    {
        // SAFETY: Retain preserves the exact immutable range selected by plan.
        // Its original u32 endpoints bound this view's unchanged extent.
        Wire::from_raw(unsafe { bounded::raw::Input::new(self.0.as_slice()) })
    }

    pub(crate) fn into_inner(self) -> scope::Deferred<'input, 'd, R> {
        self.0
    }

    pub(crate) fn try_retain(
        self,
        policy: hold::Policy,
    ) -> Result<Retained<'d>, (Self, hold::Error)>
    where
        R: hold::raw::Retain<'d>,
    {
        self.0
            .bind(policy)
            .try_retain()
            .map(Retained)
            .map_err(|(payload, error)| (Self(payload.defer()), error))
    }
}

impl Retained<'_> {
    pub(crate) fn wire(&self) -> Wire<'_> {
        // SAFETY: retention preserves the same admitted payload's bytes and extent.
        Wire::from_raw(unsafe { bounded::raw::Input::new(self.0.as_slice()) })
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

const _: () = {
    assert!(
        size_of::<Payload<'static, 'static, buffer::storage::Shared>>()
            == size_of::<scope::Deferred<'static, 'static, buffer::storage::Shared>>()
    );
    assert!(size_of::<Retained<'static>>() == size_of::<resident::Shared<'static>>());
};
