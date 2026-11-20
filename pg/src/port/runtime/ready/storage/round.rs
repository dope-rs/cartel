use super::{Order, layout};
use crate::port::ready::{Error, Part};
use o3::depot::{
    queue::links,
    raw::{Hooks, Word},
};
use std::{alloc::Layout, ptr::NonNull};

pub struct Round {
    hooks: NonNull<links::Hook>,
    head: links::Cycle,
}

// SAFETY: this plane is the fixed hook array of its original ready slots.
unsafe impl Hooks for Round {
    unsafe fn hook_raw(&self, index: links::Index) -> &links::Hook {
        unsafe { self.hooks.add(index.get()).as_ref() }
    }
}

impl Order for Round {
    const RANKED: bool = false;
    type Layout = usize;
    type Rank = ();
    type End = links::Index;

    fn layout(
        values: Layout,
        slots: usize,
        _: cartel_core::reply::layout::Layout,
    ) -> Result<(Layout, usize), Error> {
        layout::append::<links::Hook>(values, slots, Part::Nodes)
    }

    unsafe fn install(ptr: NonNull<u8>, slots: usize, offset: usize) -> Self {
        // SAFETY: the selected plan bound and aligned exactly this many hooks.
        let hooks: NonNull<links::Hook> = unsafe { ptr.add(offset).cast() };
        for i in 0..slots {
            unsafe {
                hooks
                    .add(i)
                    .write(links::Hook::new(links::Index::from_offset(i)))
            };
        }
        Self {
            hooks,
            head: links::Cycle::new(),
        }
    }

    fn pick(&self) -> Option<(links::Index, ())> {
        let first = self.head.first()?;
        // SAFETY: this is the observed front of the retained cycle.
        unsafe { self.cycle_step(&self.head, first) };
        Some((first, ()))
    }

    unsafe fn rank(&self, index: links::Index) -> Option<()> {
        index
            .is_linked(unsafe { self.hook_raw(index) })
            .then_some(())
    }

    unsafe fn insert(&self, index: links::Index, _: impl FnOnce() -> usize) {
        unsafe { self.cycle_push(&self.head, index) };
    }

    unsafe fn remove(&self, index: links::Index, (): ()) {
        unsafe { self.cycle_unlink(&self.head, index) };
    }

    unsafe fn reprioritize(&self, _: links::Index, (): (), _: impl FnOnce() -> usize) {}

    unsafe fn remaining(
        &self,
        first: links::Index,
        (): (),
    ) -> (Option<(links::Index, ())>, links::Index) {
        (
            self.head
                .first()
                .filter(|index| *index != first)
                .map(|index| (index, ())),
            first,
        )
    }

    unsafe fn following(
        &self,
        index: links::Index,
        (): (),
        first: &links::Index,
    ) -> Option<(links::Index, ())> {
        let next = unsafe { self.cycle_step(&self.head, index) };
        (next != *first).then_some((next, ()))
    }
}
