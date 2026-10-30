use dope::net::link::egress::data;
use o3::buffer::{
    Residency,
    pool::{self, owners, state},
};
pub(crate) use storage::Storage;
use zeroize::Zeroize;

mod storage;

#[repr(transparent)]
#[doc(hidden)]
pub struct Payload(pool::Span<owners::Plain<pool::Width<16>>, pool::Owned, state::Initialized>);

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Residency for Payload {
    fn resident(&self) -> usize {
        self.0.capacity().get() as usize
    }
}

// SAFETY: The exclusive lease retains one immutable, fully initialized 16-byte
// prefix until Drop clears it immediately before returning the pool slot.
unsafe impl<'d> data::raw::Contiguous<'d> for Payload {}

// SAFETY: the lease retains the pool allocation and the exclusive physical slot
// across owner moves. Only destruction clears and returns that exact slot.
unsafe impl<'d> data::raw::StableOwner<'d> for Payload {}

impl Drop for Payload {
    fn drop(&mut self) {
        self.0.as_mut_slice().zeroize();
    }
}

const _: () = assert!(std::mem::size_of::<Payload>() == 2 * std::mem::size_of::<usize>());
