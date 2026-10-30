use crate::protocol::runtime::ready::storage;
use o3::depot;
use o3::depot::queue::links;

pub(super) struct Round<'a, 'd, const ID: u8>(pub(super) &'a storage::Block<'d, ID>);

// SAFETY: each admitted index retains this exact stable round-hook allocation.
unsafe impl<const ID: u8> depot::raw::Hooks for Round<'_, '_, ID> {
    unsafe fn hook_raw(&self, index: links::Index) -> &links::Hook {
        &unsafe { &*self.0.nodes.as_ptr().add(index.get()) }.round
    }
}
