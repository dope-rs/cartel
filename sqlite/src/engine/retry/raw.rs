use crate::{config, retry};
use dope_core::driver::schedule::clock::timer;
use retry::lease;
use std::{cell, marker, mem};

/// Retains the exact settings that selected this owner's timer resources.
pub(crate) struct Configured<'d> {
    config: config::Config,
    slot: cell::UnsafeCell<retry::Slot<'d>>,
}

/// Exclusive retry authority; the owner's configuration remains independently shared.
pub(crate) struct Loan<'slot, 'd> {
    source: &'slot Configured<'d>,
    unique: marker::PhantomData<&'slot mut retry::Slot<'d>>,
}

impl<'d> Configured<'d> {
    pub(crate) fn new<E>(
        config: config::Config,
        alarm: impl FnOnce() -> Result<timer::Alarm<'d>, E>,
    ) -> Result<Self, E> {
        let slot = match config.work.alarm() {
            config::Alarm::Omit => retry::Slot::yielding(),
            config::Alarm::Reserve => retry::Slot::timed(alarm()?),
        };
        Ok(Self {
            config,
            slot: cell::UnsafeCell::new(slot),
        })
    }

    pub(crate) const fn config(&self) -> &config::Config {
        &self.config
    }

    /// # Safety
    /// The caller owns the sole installation grant for this source. No earlier loan or
    /// reference to its retry state may remain live. The source must outlive the loan.
    pub(crate) unsafe fn loan(&self) -> Loan<'_, 'd> {
        Loan {
            source: self,
            unique: marker::PhantomData,
        }
    }
}

impl Drop for Configured<'_> {
    fn drop(&mut self) {
        self.slot.get_mut().finish();
    }
}

impl<'slot, 'd> Loan<'slot, 'd> {
    pub(crate) const fn config(&self) -> &'slot config::Config {
        &self.source.config
    }

    pub(crate) fn open(&mut self) -> lease::Lease<'_, 'd, lease::Open> {
        lease::Lease::new(self.reborrow())
    }

    pub(crate) fn cleanup(&mut self) -> lease::Lease<'_, 'd, lease::Cleanup> {
        lease::Lease::new(self.reborrow())
    }

    pub(crate) fn finish(&mut self) {
        self.slot_mut().finish();
    }

    pub(crate) const fn slot(&self) -> &retry::Slot<'d> {
        // SAFETY: the installation grant excludes other loans; this borrow freezes its loan.
        unsafe { &*self.source.slot.get() }
    }

    pub(super) fn slot_mut(&mut self) -> &mut retry::Slot<'d> {
        // SAFETY: this exclusive loan projects only the mutable retry field, never Config.
        unsafe { &mut *self.source.slot.get() }
    }

    fn reborrow(&mut self) -> Loan<'_, 'd> {
        Loan {
            source: self.source,
            unique: marker::PhantomData,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Loan<'static, 'static>>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<Configured<'static>>()
            == mem::size_of::<config::Config>() + mem::size_of::<retry::Slot<'static>>()
    );
};
