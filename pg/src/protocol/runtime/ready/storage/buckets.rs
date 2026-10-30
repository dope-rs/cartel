use crate::protocol::runtime::ready::storage;
use o3::depot;
use o3::depot::queue::links;

pub(super) struct Buckets<'a, 'd, const ID: u8>(pub(super) &'a storage::Block<'d, ID>);

// SAFETY: every accepted index projects one stable bucket hook.
unsafe impl<const ID: u8> depot::raw::Hooks for Buckets<'_, '_, ID> {
    unsafe fn hook_raw(&self, index: links::Index) -> &links::Hook {
        &unsafe { self.0.nodes().get_unchecked(index.get()) }.bucket
    }
}
