use std::{num::NonZero, ptr};

use crate::runtime::{heap, release::Release};

/// One complete physical block retaining the arena whose extent admitted it.
pub(super) struct Segment<'a> {
    pub(super) arena: &'a heap::raw::Arena,
    pub(super) index: heap::raw::Index,
    pub(super) order: heap::raw::Level,
}

/// The actual detached block and exclusive allocator state which can publish it.
pub(super) struct Vacant<'a> {
    source: &'a mut heap::raw::State,
    block: Block<'a>,
}

/// The native-admitted live block and the exact state which accounts for it.
pub(super) struct Occupied<'a> {
    pub(super) source: &'a mut heap::raw::State,
    pub(super) block: Block<'a>,
}

pub(super) struct Block<'a> {
    pub(super) arena: &'a heap::raw::Arena,
    pub(super) index: heap::raw::Index,
    pub(super) order: heap::raw::Order,
}

impl<'a> Segment<'a> {
    pub(super) fn reserve(
        mut self,
        source: &'a mut heap::raw::State,
        order: heap::raw::Order,
    ) -> Vacant<'a> {
        for position in (order.index()..self.order.index()).rev() {
            let level = heap::raw::Level::mask(position as u8);
            self.order = level;
            let width = 1usize.wrapping_shl(level.index() as u32);
            // SAFETY: the upper child's first position is inside this exact retained block.
            // Its one-based position is positive and at most the original arena's unit count.
            let index = unsafe { NonZero::new_unchecked(self.index.0.get() + width) };
            source.link(Self {
                arena: self.arena,
                index: heap::raw::Index(index),
                order: self.order,
            });
        }
        Vacant {
            source,
            block: Block {
                arena: self.arena,
                index: self.index,
                order,
            },
        }
    }
}

impl Vacant<'_> {
    /// This capability owns a detached block absent from the live count.
    /// Each live allocation consumes at least one of this arena's bounded units.
    pub(super) fn publish(self) -> *mut u8 {
        let pointer = self.block.write();
        self.source.allocations += 1;
        pointer
    }

    fn replace(self, old: Block<'_>) -> *mut u8 {
        let pointer = self.block.write();
        // SAFETY: Occupied::grow reserved this disjoint replacement from the same state;
        // both complete allocations retain the original arena, and the new block is larger.
        unsafe {
            ptr::copy_nonoverlapping(old.pointer(), pointer, heap::raw::payload_bytes(old.order))
        };
        self.source.recycle(old.into_segment());
        pointer
    }
}

impl Occupied<'_> {
    /// This capability owns one native-admitted live block in this exact state.
    /// Consuming it removes that counted allocation exactly once.
    pub(super) fn release(self) -> Option<Release> {
        self.source.allocations -= 1;
        self.source.recycle(self.block.into_segment());
        if self.source.is_empty() {
            self.source.release.take()
        } else {
            None
        }
    }

    pub(super) fn grow(self, order: heap::raw::Order) -> *mut u8 {
        let Self { source, block } = self;
        source.reserve(block.arena, order, |next| next.replace(block))
    }
}

impl<'a> Block<'a> {
    fn pointer(&self) -> *mut u8 {
        // SAFETY: the retained complete allocation includes its header before this payload.
        unsafe { self.arena.block(self.index).add(heap::raw::Header::BYTES) }
    }

    fn write(&self) -> *mut u8 {
        self.arena.set_control(self.index, self.order.index() as u8);
        // SAFETY: the vacancy exclusively owns this complete block in its original arena.
        unsafe {
            self.arena
                .block(self.index)
                .cast::<heap::raw::Header>()
                .write(heap::raw::Header::new(
                    heap::raw::Owner::new(self.arena),
                    self.order,
                ))
        };
        self.pointer()
    }

    /// Embeds the admitted request order unchanged in the wider physical domain.
    fn into_segment(self) -> Segment<'a> {
        Segment {
            arena: self.arena,
            index: self.index,
            order: self.order.widen(),
        }
    }
}

/// The unpublished suffix of one arena's complete unit domain.
/// Its nonzero remainder selects a highest bit below the physical level count.
pub(super) struct Partition<'a> {
    arena: &'a heap::raw::Arena,
    offset: usize,
    left: usize,
}

impl<'a> Partition<'a> {
    pub(super) fn new(arena: &'a heap::raw::Arena) -> Self {
        Self {
            arena,
            offset: 0,
            left: arena.units(),
        }
    }
}

impl<'a> Iterator for Partition<'a> {
    type Item = Segment<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let left = NonZero::new(self.left)?;
        let order = heap::raw::Level::mask(left.ilog2() as u8);
        // SAFETY: this unvisited suffix begins inside its original bounded arena.
        let index = heap::raw::Index(unsafe { NonZero::new_unchecked(self.offset + 1) });
        let width = 1usize.wrapping_shl(order.index() as u32);
        self.offset += width;
        self.left -= width;
        Some(Segment {
            arena: self.arena,
            index,
            order,
        })
    }
}
