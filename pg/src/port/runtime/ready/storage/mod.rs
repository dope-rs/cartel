use o3::depot::queue::links;
use std::{alloc::Layout, ptr::NonNull};

pub(crate) use entry::{Entry, Listed};
pub(in crate::port::runtime) use index::Index;
pub(crate) use index::{Remaining, Reservation};

mod buckets;
mod entry;
mod index;
pub(super) mod layout;
pub(crate) mod pool;
mod round;

#[derive(Clone, Copy, Debug)]
pub struct RoundRobin;
/// Selects an eligible connection with the fewest reply orders plus
/// pending Sync boundaries, including the boundary of an open pipeline.
#[derive(Clone, Copy, Debug)]
pub struct LeastInflight;

pub trait Policy: sealed::Seal + Copy + 'static {
    #[doc(hidden)]
    type Order: Order;
}

mod sealed {
    pub trait Seal {}
    impl Seal for super::RoundRobin {}
    impl Seal for super::LeastInflight {}
}

impl Policy for RoundRobin {
    type Order = round::Round;
}

impl Policy for LeastInflight {
    type Order = buckets::Buckets;
}

/// Storage and transitions belonging to one installed policy.
#[doc(hidden)]
pub trait Order: Sized {
    const RANKED: bool;
    type Layout: Copy;
    type Rank: Copy;
    type End;

    fn layout(
        values: Layout,
        slots: usize,
        replies: cartel_core::reply::layout::Layout,
    ) -> Result<(Layout, Self::Layout), crate::port::ready::Error>;

    // The original allocation was admitted with this exact plan and slot count.
    unsafe fn install(ptr: NonNull<u8>, slots: usize, layout: Self::Layout) -> Self;

    fn pick(&self) -> Option<(links::Index, Self::Rank)>;
    // The index and rank are retained by the caller's exclusive eligibility transition.
    unsafe fn rank(&self, index: links::Index) -> Option<Self::Rank>;
    unsafe fn insert(&self, index: links::Index, depth: impl FnOnce() -> usize);
    unsafe fn remove(&self, index: links::Index, rank: Self::Rank);
    unsafe fn reprioritize(
        &self,
        index: links::Index,
        rank: Self::Rank,
        depth: impl FnOnce() -> usize,
    );
    unsafe fn remaining(
        &self,
        index: links::Index,
        rank: Self::Rank,
    ) -> (Option<(links::Index, Self::Rank)>, Self::End);
    unsafe fn following(
        &self,
        index: links::Index,
        rank: Self::Rank,
        end: &Self::End,
    ) -> Option<(links::Index, Self::Rank)>;
}
