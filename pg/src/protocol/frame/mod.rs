use o3::buffer::{self, bytes::bounded, hold, hold::scope};

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

impl<'input, 'd, R: buffer::Cursor> Payload<'input, 'd, R> {
    pub fn as_slice(&self) -> &'input [u8] {
        self.0.as_slice()
    }

    pub(crate) fn wire(&self) -> Wire<'input>
    where
        R: hold::raw::Retain<'d>,
    {
        // SAFETY: Retain preserves the exact immutable range selected by the codec.
        // Its original u32 endpoints bound this view's unchanged extent.
        Wire::from_raw(unsafe { bounded::raw::Input::new(self.0.as_slice()) })
    }

    pub(crate) fn into_inner(self) -> scope::Deferred<'input, 'd, R> {
        self.0
    }
}

const _: () = {
    assert!(
        size_of::<Payload<'static, 'static, buffer::storage::Shared>>()
            == size_of::<scope::Deferred<'static, 'static, buffer::storage::Shared>>()
    );
};
