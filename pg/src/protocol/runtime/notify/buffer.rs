use o3::alloc;
use o3::depot::{self, queue::ring};
use std::{cell::Cell, num::NonZero};

pub(super) struct Buffer<'d> {
    queue: ring::Fifo<crate::reply::Notice<'d>>,
    free: usize,
}

impl<'d> Buffer<'d> {
    pub(super) fn try_new(items: NonZero<u32>, bytes: usize) -> Result<Self, alloc::Error> {
        Ok(Self {
            queue: ring::Fifo::try_with(items, None)?,
            free: bytes,
        })
    }

    pub(super) fn pop(&mut self) -> Option<crate::reply::Notice<'d>> {
        let notice = self.queue.pop_front()?;
        self.free += notice.bytes();
        Some(notice)
    }

    /// The caller admits this notice against the same immutable byte capacity.
    pub(super) fn try_push(
        &mut self,
        notice: crate::reply::Notice<'d>,
        dropped: &Cell<u64>,
    ) -> Result<(), crate::Error> {
        use depot::raw::RingEdit as _;

        let bytes = notice.bytes();
        while bytes > self.free {
            // SAFETY: free plus the resident notices equals the admitted byte
            // capacity. This notice fits that capacity, so a deficit proves
            // this exact exclusively borrowed queue has an initialized front.
            let prior = unsafe { self.queue.front_raw() }.remove();
            self.free += prior.bytes();
            drop(prior);
            Self::try_drop(dropped)?;
        }
        let entry = match self.queue.vacant() {
            Some(entry) => entry,
            None => {
                // SAFETY: this constructor retains a nonzero fixed capacity.
                // A full queue therefore owns an initialized front, whose
                // removal returns the exact vacancy used for publication.
                let (prior, entry) = unsafe { self.queue.front_raw() }.take();
                self.free += prior.bytes();
                drop(prior);
                Self::try_drop(dropped)?;
                entry
            }
        };
        self.free -= bytes;
        entry.push_back(notice);
        Ok(())
    }

    pub(super) fn try_drop(dropped: &Cell<u64>) -> Result<(), crate::Error> {
        let count = dropped
            .get()
            .checked_add(1)
            .ok_or(crate::Error::NotificationDropOverflow)?;
        dropped.set(count);
        Ok(())
    }
}
