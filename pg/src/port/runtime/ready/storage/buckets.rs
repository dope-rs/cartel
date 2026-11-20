use super::{Order, layout};
use crate::port::ready::{Error, Part};
use o3::depot::{
    index::bitmap::{self, Access as _, entry::Row},
    queue::links,
    raw::{Hooks, Proof, Word, Words},
};
use std::{alloc::Layout, cell::Cell, num::NonZero, ptr::NonNull, slice};

type Shape = bitmap::Shape<NonZero<u32>>;

struct Node {
    hook: links::Hook,
    rank: Cell<Option<NonZero<u32>>>,
}

#[derive(Clone, Copy)]
pub struct Offsets {
    nodes: usize,
    heads: usize,
    words: usize,
    shape: Shape,
}

pub struct Buckets {
    nodes: NonNull<Node>,
    heads: NonNull<links::List>,
    words: NonNull<bitmap::Word>,
    shape: Shape,
    minimum: Cell<Option<NonZero<u32>>>,
}

// SAFETY: this policy initializes exactly shape.words() stationary bitmap words.
unsafe impl Words for Buckets {
    type Plane = ();
    type Size = NonZero<u32>;

    fn shape(&self) -> Shape {
        self.shape
    }

    fn words(&self, (): ()) -> &[bitmap::Word] {
        unsafe { slice::from_raw_parts(self.words.as_ptr(), self.shape.words()) }
    }
}

// SAFETY: each allocation index maps to its own fixed hook; mutation is confined to this plane.
unsafe impl Hooks for &Buckets {
    unsafe fn hook_raw(&self, index: links::Index) -> &links::Hook {
        &unsafe { self.nodes.add(index.get()).as_ref() }.hook
    }
}

impl Buckets {
    fn head(&self, rank: NonZero<u32>) -> &links::List {
        // SAFETY: every stored rank comes from a row clamped to this bitmap.
        unsafe { self.heads.add(rank.get() as usize - 1).as_ref() }
    }

    fn first(&self, rank: NonZero<u32>) -> (links::Index, NonZero<u32>) {
        // SAFETY: minimum and bitmap hits refer only to occupied ranks.
        (
            links::List::front_from(unsafe { Proof::new(self.head(rank)) }),
            rank,
        )
    }

    fn next(
        &self,
        index: links::Index,
        rank: NonZero<u32>,
    ) -> Option<(links::Index, NonZero<u32>)> {
        // SAFETY: the caller's current candidate belongs to this rank and plane.
        let node = unsafe { self.nodes.add(index.get()).as_ref() };
        if let Some(next) = node.hook.next_index() {
            return Some((next, rank));
        }
        self.find((), rank.get() as usize)
            .map(|row| self.first(row.position()))
    }

    unsafe fn attach(&self, index: links::Index, row: Row<&Self>) {
        let rank = row.position();
        let list = self.head(rank);
        if list.is_empty() {
            row.update::<true>(());
            if self.minimum.get().is_none_or(|old| rank < old) {
                self.minimum.set(Some(rank));
            }
        }
        let mut hooks = self;
        unsafe { hooks.link_raw(list, index) };
        unsafe { self.nodes.add(index.get()).as_ref() }
            .rank
            .set(Some(rank));
    }
}

impl Order for Buckets {
    const RANKED: bool = true;
    type Layout = Offsets;
    type Rank = NonZero<u32>;
    type End = ();

    fn layout(
        values: Layout,
        slots: usize,
        replies: cartel_core::reply::layout::Layout,
    ) -> Result<(Layout, Offsets), Error> {
        // With K shared orders, the minimum is at most K+3. Reply layout bounds K below u32::MAX/2.
        let buckets = if slots == 1 {
            NonZero::<u32>::MIN
        } else {
            unsafe { NonZero::new_unchecked(replies.order_limit().get() + 3) }
        };
        let shape = Shape::new(buckets);
        let (layout, nodes) = layout::append::<Node>(values, slots, Part::Nodes)?;
        let (layout, heads) = layout::append::<links::List>(layout, shape.bits(), Part::Heads)?;
        let (layout, words) = layout::append::<bitmap::Word>(layout, shape.words(), Part::Words)?;
        Ok((
            layout,
            Offsets {
                nodes,
                heads,
                words,
                shape,
            },
        ))
    }

    unsafe fn install(ptr: NonNull<u8>, slots: usize, offsets: Offsets) -> Self {
        // SAFETY: the original admitted plan supplies all offsets, alignments, and lengths.
        let this = Self {
            nodes: unsafe { ptr.add(offsets.nodes).cast() },
            heads: unsafe { ptr.add(offsets.heads).cast() },
            words: unsafe { ptr.add(offsets.words).cast() },
            shape: offsets.shape,
            minimum: Cell::new(None),
        };
        for i in 0..slots {
            unsafe {
                this.nodes.add(i).write(Node {
                    hook: links::Hook::new(links::Index::from_offset(i)),
                    rank: Cell::new(None),
                })
            };
        }
        for i in 0..this.shape.bits() {
            unsafe { this.heads.add(i).write(links::List::new()) };
        }
        for (i, word) in this.shape.fill(bitmap::Fill::Empty).enumerate() {
            unsafe { this.words.add(i).write(word) };
        }
        this
    }

    fn pick(&self) -> Option<(links::Index, Self::Rank)> {
        Some(self.first(self.minimum.get()?))
    }

    unsafe fn rank(&self, index: links::Index) -> Option<Self::Rank> {
        unsafe { self.nodes.add(index.get()).as_ref() }.rank.get()
    }

    unsafe fn insert(&self, index: links::Index, depth: impl FnOnce() -> usize) {
        unsafe { self.attach(index, Row::clamp(self, depth())) };
    }

    unsafe fn remove(&self, index: links::Index, rank: Self::Rank) {
        // SAFETY: rank was issued with the carried membership in this exact queue.
        let row = Row::from_source(unsafe { Proof::new((self, rank)) });
        let list = self.head(rank);
        let mut hooks = self;
        unsafe { hooks.unlink_raw(list, index) };
        unsafe { self.nodes.add(index.get()).as_ref() }
            .rank
            .set(None);
        if list.is_empty() {
            row.update::<false>(());
            if self.minimum.get() == Some(rank) {
                self.minimum.set(
                    self.find((), rank.get() as usize)
                        .map(|next| next.position()),
                );
            }
        }
    }

    unsafe fn reprioritize(
        &self,
        index: links::Index,
        rank: Self::Rank,
        depth: impl FnOnce() -> usize,
    ) {
        let row = Row::clamp(self, depth());
        if row.position() != rank || self.head(rank).front() != Some(index.get()) {
            unsafe {
                self.remove(index, rank);
                self.attach(index, row);
            }
        }
    }

    unsafe fn remaining(
        &self,
        index: links::Index,
        rank: Self::Rank,
    ) -> (Option<(links::Index, Self::Rank)>, ()) {
        (self.next(index, rank), ())
    }

    unsafe fn following(
        &self,
        index: links::Index,
        rank: Self::Rank,
        _: &(),
    ) -> Option<(links::Index, Self::Rank)> {
        self.next(index, rank)
    }
}

const _: () = assert!(size_of::<Node>() == 3 * size_of::<u32>());
