use dope::core::driver;
use dope_fiber::wait::Capacity;
use dope_fiber::{context, wait};
use o3::alloc;
use o3::base::cell::region;
use o3::buffer::{Source, resident::copy};
use std::{cell, num::NonZero, pin};

mod buffer;

pub(crate) struct Queue<'d> {
    buffer: region::Value<region::Identity<'d>, buffer::Buffer<'d>>,
    capacity: usize,
    dropped: cell::Cell<u64>,
    pub(in crate::protocol) waiters: wait::Notifier<'d>,
}

impl<'d> Queue<'d> {
    pub(in crate::protocol) fn try_new(
        driver: driver::Reference<'d>,
        items: NonZero<u32>,
        bytes: usize,
        waiters: Capacity,
        proof: region::Identity<'d>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            buffer: proof.bind(buffer::Buffer::try_new(items, bytes)?),
            capacity: bytes,
            dropped: cell::Cell::new(0),
            waiters: wait::Notifier::new(driver, waiters),
        })
    }

    pub(crate) fn pop(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Option<crate::reply::Notice<'d>> {
        token.borrow_mut(&self.buffer).pop()
    }

    pub(in crate::protocol) fn try_push<'input, R: Source>(
        &self,
        fields: crate::reply::raw::Fields<'input, 'd, R>,
        pid: u32,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), crate::Error> {
        let bytes = fields.retained_len();
        if bytes > self.capacity {
            return self.try_reject();
        }
        let notice = match crate::reply::Notice::try_new(fields, pid) {
            Ok(notice) => notice,
            Err(error) => {
                self.try_reject()?;
                return match error {
                    copy::Error::Budget(_) => Ok(()),
                    copy::Error::Allocation(error) => Err(crate::Error::StorageExhausted(error)),
                };
            }
        };
        token
            .borrow_mut(&self.buffer)
            .try_push(notice, &self.dropped)?;
        self.waiters.notify();
        Ok(())
    }

    fn try_reject(&self) -> Result<(), crate::Error> {
        buffer::Buffer::try_drop(&self.dropped)
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.get()
    }

    pub(crate) fn try_register<'target>(
        &'target self,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'_, 'd>>,
    ) -> Result<(), wait::Error<'d>> {
        self.waiters.try_register(waiter, context)
    }
}
