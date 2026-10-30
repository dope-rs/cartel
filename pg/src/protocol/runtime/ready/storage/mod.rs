use block::Block;
use buckets::Buckets;
pub(in crate::protocol::runtime::ready) use entry::Entry;
use header::Header;
pub(in crate::protocol::runtime::ready) use index::Index;
pub(super) use layout::Layout;
use node::Node;
use o3::depot::{self, index::bitmap::entry::Row};
use round::Round;
use std::num::NonZero;

mod layout;

mod block;
mod buckets;
mod entry;
mod header;
mod index;
mod node;
mod round;

/// An admitted row kept only in the allocation which issued it.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
struct Rank(NonZero<u32>);

impl Rank {
    fn new<const ID: u8>(row: Row<&Block<'_, ID>>) -> Self {
        Self(row.position())
    }

    fn row<'a, 'd, const ID: u8>(self, source: &'a Block<'d, ID>) -> Row<&'a Block<'d, ID>> {
        // SAFETY: ranks are issued only by this allocation's admitted rows and
        // are retained privately in its node priorities and index minimum.
        Row::from_source(unsafe { depot::raw::Proof::new((source, self.0)) })
    }
}

const _: () = assert!(std::mem::size_of::<Option<Rank>>() == std::mem::size_of::<u32>());
