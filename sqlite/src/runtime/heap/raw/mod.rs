mod block;
mod hooks;
mod local;
mod retained;
mod span;
use std::{cell, cmp, mem, num, ptr};

use heap::layout::Layout;
pub(in crate::runtime) use hooks::{Foreign, Hooks};
pub(crate) use local::Local;
use o3::{base::marker::ThreadBound, depot::array};
pub(crate) use retained::Retained;
pub(crate) use span::Span;

use crate::runtime::{heap, raw::Memory};
const UNIT_BYTES: usize = Layout::UNIT_BYTES;
// Runtime admission bounds physical blocks by MAX_ORDER; Level::mask retains
// the smallest power-of-two domain containing every admitted block order.
const ORDERS: usize = (MAX_ORDER + 1).next_power_of_two();
const FREE: u8 = 1 << 7;
const MAX_BLOCK_BYTES: usize = 1 << 30;
const MAX_ORDER: usize = 25;
pub(in crate::runtime::heap) type Order = array::Index<{ MAX_ORDER + 1 }>;
type Level = array::Index<ORDERS>;

#[repr(C)]
#[derive(Clone, Copy)]
struct Links {
    previous: Option<Index>,
    next: Option<Index>,
}

struct Free<'a> {
    block: block::Segment<'a>,
    links: Links,
}

struct State {
    heads: [Option<Index>; ORDERS],
    allocations: usize,
    release: Option<Memory>,
}

impl State {
    const fn new() -> Self {
        Self {
            heads: [None; ORDERS],
            allocations: 0,
            release: None,
        }
    }

    fn arm(&mut self, release: Memory) -> Option<Memory> {
        if self.is_empty() {
            Some(release)
        } else {
            self.release = Some(release);
            None
        }
    }

    fn initialize(&mut self, arena: &Arena) {
        for block in block::Partition::new(arena) {
            self.link(block);
        }
    }

    const fn is_empty(&self) -> bool {
        self.allocations == 0
    }

    fn reserve<'a>(
        &'a mut self,
        arena: &'a Arena,
        order: Order,
        apply: impl FnOnce(block::Vacant<'a>) -> *mut u8,
    ) -> *mut u8 {
        for position in order.index()..MAX_ORDER + 1 {
            let found = Level::mask(position as u8);
            let Some(index) = *found.get(&self.heads) else {
                continue;
            };
            let free = Free {
                block: block::Segment {
                    arena,
                    index,
                    order: found,
                },
                // SAFETY: this arena's linked head retains the free block and its Links.
                links: unsafe { arena.block(index).cast::<Links>().read() },
            };
            let block = self.unlink(free);
            return apply(block.reserve(self, order));
        }
        ptr::null_mut()
    }

    fn recycle(&mut self, mut block: block::Segment<'_>) {
        while let Some(next) = Level::new(block.order.index() + 1) {
            let width = 1usize.wrapping_shl(block.order.index() as u32);
            let Some(buddy) = Index::new(block.index.get() ^ width, block.arena.units()) else {
                break;
            };
            let Some(free) = Self::free(block.arena, buddy, block.order) else {
                break;
            };
            self.unlink(free);
            block.arena.set_control(block.index, 0);
            block.index = cmp::min(block.index, buddy);
            block.order = next;
        }
        self.link(block);
    }

    fn link(&mut self, block: block::Segment<'_>) {
        let block::Segment {
            arena,
            index,
            order,
        } = block;
        let head = order.get_mut(&mut self.heads);
        let next = *head;
        let links = Links {
            previous: None,
            next,
        };
        unsafe { arena.block(index).cast::<Links>().write(links) };
        if let Some(next) = next {
            unsafe { (*arena.block(next).cast::<Links>()).previous = Some(index) };
        }
        arena.set_control(index, FREE | order.index() as u8);
        *head = Some(index);
    }

    fn free(arena: &Arena, index: Index, order: Level) -> Option<Free<'_>> {
        if arena.control(index) != FREE | order.index() as u8 {
            return None;
        }
        let links = unsafe { arena.block(index).cast::<Links>().read() };
        Some(Free {
            block: block::Segment {
                arena,
                index,
                order,
            },
            links,
        })
    }

    fn unlink<'a>(&mut self, free: Free<'a>) -> block::Segment<'a> {
        let Free { block, links } = free;
        let block::Segment {
            arena,
            index,
            order,
        } = &block;
        if let Some(previous) = links.previous {
            unsafe { (*arena.block(previous).cast::<Links>()).next = links.next };
        } else {
            *order.get_mut(&mut self.heads) = links.next;
        }
        if let Some(next) = links.next {
            unsafe { (*arena.block(next).cast::<Links>()).previous = links.previous };
        }
        arena.set_control(*index, 0);
        block
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct Index(num::NonZero<usize>);

impl Index {
    fn new(index: usize, units: usize) -> Option<Self> {
        if index >= units {
            return None;
        }
        Some(Self(num::NonZero::<usize>::new(index.checked_add(1)?)?))
    }

    const fn get(self) -> usize {
        self.0.get() - 1
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::runtime::heap) struct Owner(ptr::NonNull<Arena>);

impl Owner {
    pub(super) fn new(arena: &Arena) -> Self {
        Self(ptr::NonNull::from(arena))
    }

    fn with_state<T>(self, op: impl FnOnce(&Arena, &mut State) -> T) -> T {
        let arena = unsafe { self.0.as_ref() };
        unsafe { op(arena, &mut *arena.state.get()) }
    }

    pub(super) fn allocate(self, order: Order) -> *mut u8 {
        self.with_state(|arena, state| state.reserve(arena, order, |block| block.publish()))
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Header {
    owner: Option<Owner>,
    metadata: Metadata,
}

#[repr(C)]
#[derive(Clone, Copy)]
union Metadata {
    order: Order,
    bytes: hooks::Len,
}

/// One exact live native allocation retaining its original header and publishing owner.
struct Decoded {
    pointer: ptr::NonNull<Header>,
    owner: Owner,
}

impl Decoded {
    /// # Safety
    /// These fields come together from one unchanged live Core header, returned exclusively on its originating worker.
    /// Its counted block retains the original arena backing independently of the current allocation scope.
    unsafe fn new(pointer: ptr::NonNull<Header>, owner: Owner) -> Self {
        Self { pointer, owner }
    }

    fn order(&self) -> Order {
        // SAFETY: admission retains the unchanged live Core header and its initialized order.
        unsafe { (*self.pointer.as_ptr()).metadata.order }
    }

    fn payload(&self) -> *mut u8 {
        // SAFETY: this retained live header directly precedes its native allocation's payload.
        unsafe { self.pointer.as_ptr().add(1).cast() }
    }

    /// Borrows the original allocator retained by this worker's live native allocation.
    /// Allocator operations invoke no native or user callback while this borrow is live.
    fn with_state<T>(self, apply: impl FnOnce(block::Occupied<'_>) -> T) -> T {
        let order = self.order();
        let Self { pointer, owner } = self;
        owner.with_state(|arena, source| {
            // SAFETY: the native receipt retains the exact header published by this owner.
            // Its block start is base + Index * UNIT_BYTES in this same backing allocation.
            let offset = unsafe {
                pointer
                    .as_ptr()
                    .cast::<u8>()
                    .offset_from_unsigned(arena.base())
            };
            // SAFETY: that original block start lies within this arena's immutable unit domain.
            // Its one-based index is positive and at most the unit count.
            // Native custody also preserves Block::write's order and occupied control until release.
            let index = Index(unsafe { num::NonZero::new_unchecked(offset / UNIT_BYTES + 1) });
            apply(block::Occupied {
                source,
                block: block::Block {
                    arena,
                    index,
                    order,
                },
            })
        })
    }
}

impl Header {
    const BYTES: usize = size_of::<Self>();

    fn new(owner: Owner, order: Order) -> Self {
        Self {
            owner: Some(owner),
            metadata: Metadata { order },
        }
    }

    fn foreign(bytes: hooks::Len) -> Self {
        Self {
            owner: None,
            metadata: Metadata { bytes },
        }
    }
}

pub(in crate::runtime::heap) struct Arena {
    base: ptr::NonNull<u8>,
    units: usize,
    state: cell::UnsafeCell<State>,
    _thread: ThreadBound,
}
impl Arena {
    pub(crate) const BYTES: usize = size_of::<Self>().div_ceil(64) * 64;

    unsafe fn new(base: span::ArenaBase, layout: Layout) -> Self {
        let base = base.pointer();
        let units = layout.units();
        unsafe { base.as_ptr().add(units * UNIT_BYTES).write_bytes(0, units) };
        let arena = Self {
            base,
            units,
            state: cell::UnsafeCell::new(State::new()),
            _thread: ThreadBound::NEW,
        };
        unsafe { (&mut *arena.state.get()).initialize(&arena) };
        arena
    }

    fn capacity(&self) -> usize {
        self.units * UNIT_BYTES
    }

    fn arm(&self, release: Memory) -> Option<Memory> {
        unsafe { (&mut *self.state.get()).arm(release) }
    }

    fn release(header: Decoded) {
        let memory = header.with_state(|block| block.release());
        // The allocator-state borrow ends before its backing is reclaimed.
        drop(memory);
    }
    fn grow(header: Decoded, order: Order) -> *mut u8 {
        header.with_state(|block| block.grow(order))
    }
    fn size(header: Decoded) -> i32 {
        payload_integer(header.order())
    }
    fn units(&self) -> usize {
        self.units
    }
    fn block(&self, index: Index) -> *mut u8 {
        unsafe { self.base().add(index.get() * UNIT_BYTES) }
    }
    fn control(&self, index: Index) -> u8 {
        unsafe { self.base.as_ptr().add(self.capacity() + index.get()).read() }
    }
    fn set_control(&self, index: Index, value: u8) {
        unsafe {
            self.base
                .as_ptr()
                .add(self.capacity() + index.get())
                .write(value)
        };
    }
    fn base(&self) -> *mut u8 {
        self.base.as_ptr()
    }
}
fn payload_bytes(order: Order) -> usize {
    ((UNIT_BYTES / 2) << (order.index() + 1)) - Header::BYTES
}
/// Order admits at most MAX_BLOCK_BYTES, below the signed SQLite limit.
fn payload_integer(order: Order) -> i32 {
    payload_bytes(order) as i32
}
const _: () = {
    assert!(Header::BYTES == 16);
    assert!(align_of::<Header>() == 8);
    assert!(size_of::<Metadata>() == size_of::<u32>());
    assert!(size_of::<Order>() == size_of::<u8>());
    assert!(ORDERS <= u8::MAX as usize);
    assert!(MAX_ORDER < ORDERS);
    assert!(UNIT_BYTES << MAX_ORDER == MAX_BLOCK_BYTES);
    assert!(MAX_ORDER <= u8::MAX as usize);
    assert!(MAX_BLOCK_BYTES <= i32::MAX as usize);
    assert!(i32::MAX as usize / UNIT_BYTES < 1usize << (MAX_ORDER + 1));
    assert!(
        mem::size_of::<State>()
            == mem::size_of::<[Option<Index>; ORDERS]>() + 3 * size_of::<usize>()
    );
    assert!(size_of::<Links>() <= UNIT_BYTES);
};
