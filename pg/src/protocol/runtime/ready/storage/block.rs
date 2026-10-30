use crate::{
    port::pool::slot,
    protocol::runtime::ready::{plan, storage},
};
use dope::core::driver::route::table::conn;
use o3::{alloc, depot};
use o3::{
    alloc::Error,
    depot::{
        index::bitmap::{self, Fill},
        queue::links,
    },
};
use std::{alloc::Layout, marker, mem, num, ptr, slice};

pub(super) struct Block<'d, const ID: u8> {
    pointer: ptr::NonNull<u8>,
    pub(super) nodes: ptr::NonNull<storage::Node>,
    heads: ptr::NonNull<links::List>,
    shape: ptr::NonNull<storage::Header>,
    layout: Layout,
    capacity: conn::Capacity,
    buckets: num::NonZero<u32>,
    value: marker::PhantomData<slot::Slot<'d, ID>>,
}

impl<'d, const ID: u8> Block<'d, ID> {
    pub(super) fn try_new(plan: plan::Plan) -> Result<Self, Error> {
        let block = plan.block();
        let ptr = alloc::raw::LayoutExt::try_allocate::<u8>(block.allocation)?;
        let this = Self {
            pointer: ptr,
            nodes: unsafe { ptr::NonNull::new_unchecked(ptr.as_ptr().add(block.nodes).cast()) },
            heads: unsafe { ptr::NonNull::new_unchecked(ptr.as_ptr().add(block.heads).cast()) },
            shape: unsafe { ptr::NonNull::new_unchecked(ptr.as_ptr().add(block.header).cast()) },
            layout: block.allocation,
            capacity: plan.capacity(),
            buckets: plan.buckets(),
            value: marker::PhantomData,
        };
        this.initialize(block.shape);
        Ok(this)
    }

    pub(super) const fn capacity(&self) -> conn::Capacity {
        self.capacity
    }

    pub(super) const fn bucket_count(&self) -> num::NonZero<u32> {
        self.buckets
    }

    fn initialize(&self, shape: bitmap::Shape<num::NonZero<u32>>) {
        for raw in 0..self.capacity().entries().get() {
            // SAFETY: the validated connection domain lies below the compact sentinel.
            let index = unsafe { <links::Index as depot::raw::Word>::from_offset(raw) };
            unsafe {
                self.nodes
                    .as_ptr()
                    .add(raw)
                    .write(storage::Node::new(index))
            };
        }
        for index in 0..self.bucket_count().get() as usize {
            unsafe { self.heads.as_ptr().add(index).write(links::List::new()) };
        }
        unsafe { self.shape.as_ptr().write(storage::Header::new(shape)) };
        for (index, word) in shape.fill(Fill::Empty).enumerate() {
            unsafe { self.words().as_ptr().add(index).write(word) };
        }
        for index in 0..self.capacity().entries().get() {
            unsafe {
                self.pointer
                    .as_ptr()
                    .cast::<slot::Slot<'d, ID>>()
                    .add(index)
                    .write(slot::Slot::new())
            };
        }
    }

    pub(super) fn values(&self) -> &[slot::Slot<'d, ID>] {
        unsafe {
            slice::from_raw_parts(
                self.pointer.as_ptr().cast(),
                self.capacity().entries().get(),
            )
        }
    }

    pub(super) fn nodes(&self) -> &[storage::Node] {
        unsafe { slice::from_raw_parts(self.nodes.as_ptr(), self.capacity().entries().get()) }
    }

    pub(super) fn heads(&self) -> &[links::List] {
        unsafe { slice::from_raw_parts(self.heads.as_ptr(), self.bucket_count().get() as usize) }
    }

    fn words(&self) -> ptr::NonNull<bitmap::Word> {
        unsafe { self.shape.add(1).cast() }
    }
}

// SAFETY: `Block` owns the exact shape followed by every bitmap word.
unsafe impl<const ID: u8> depot::raw::Words for Block<'_, ID> {
    type Plane = ();
    type Size = num::NonZero<u32>;

    fn shape(&self) -> bitmap::Shape<Self::Size> {
        unsafe { self.shape.as_ref().shape }
    }

    fn words(&self, (): ()) -> &[bitmap::Word] {
        unsafe { slice::from_raw_parts(self.words().as_ptr(), self.shape.as_ref().shape.words()) }
    }
}

impl<const ID: u8> Drop for Block<'_, ID> {
    fn drop(&mut self) {
        if mem::needs_drop::<slot::Slot<'_, ID>>() {
            let values = ptr::slice_from_raw_parts_mut(
                self.pointer.as_ptr().cast::<slot::Slot<'_, ID>>(),
                self.capacity().entries().get(),
            );
            unsafe { values.drop_in_place() };
        }
        unsafe { alloc::raw::LayoutExt::deallocate(self.layout, self.pointer) };
    }
}
