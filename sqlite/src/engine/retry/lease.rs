use crate::{config, engine::retry, sys};
use dope_core::driver::{
    schedule::clock,
    schedule::{self, ready::wake},
};
use o3::mem::{self, lease};
use std::{marker, task};

pub(crate) trait Role {
    const RELEASE: Release;

    fn limits(config: &config::Config) -> config::Contention;
}

pub(crate) enum Release {
    Preserve,
    Detach,
    Finish,
}

pub(crate) enum Open {}

pub(crate) enum Cleanup {}

pub(crate) enum Call {}

pub(crate) struct Lease<'slot, 'd, R: Role> {
    slot: mem::Lease<Return<'slot, 'd, R>, lease::Open>,
}

struct Return<'slot, 'd, R>(marker::PhantomData<(retry::raw::Loan<'slot, 'd>, R)>);

#[derive(Clone, Copy)]
pub(crate) struct Ref<'slot, 'd, R> {
    slot: &'slot retry::Slot<'d>,
    role: marker::PhantomData<R>,
}

impl<'slot, 'd, R: Role> Lease<'slot, 'd, R> {
    pub(super) const fn new(slot: retry::raw::Loan<'slot, 'd>) -> Self {
        Self {
            slot: mem::Lease::open(Return(marker::PhantomData), slot),
        }
    }

    pub(crate) const fn config(&self) -> &'slot config::Config {
        self.slot.item().config()
    }

    pub(crate) fn defer(
        &mut self,
        now: clock::Moment<'d>,
        fault: sys::Fault,
    ) -> Result<(), crate::Error> {
        let limits = R::limits(self.config());
        self.slot.item_mut().slot_mut().defer(limits, now, fault)
    }

    pub(crate) fn wait(&self) -> retry::Wait {
        self.slot.item().slot().wait()
    }

    pub(crate) fn poll(
        &mut self,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), crate::Error>> {
        self.slot.item_mut().slot_mut().poll(now, wake)
    }

    pub(crate) fn finish(&mut self) {
        self.slot.item_mut().finish();
    }
}

impl<'slot, 'd> Lease<'slot, 'd, Cleanup> {
    pub(crate) fn into_call(mut self) -> Lease<'slot, 'd, Call> {
        self.finish();
        Lease::new(self.slot.into_parts().1)
    }

    pub(crate) fn poll_park(
        &mut self,
        now: clock::Moment<'d>,
    ) -> task::Poll<Result<(), crate::Error>> {
        self.slot.item_mut().slot_mut().poll_park(now)
    }
}

impl<'slot, 'd, R: Role> mem::Return for Return<'slot, 'd, R> {
    type Item = retry::raw::Loan<'slot, 'd>;

    fn return_item(self, mut slot: Self::Item) {
        match R::RELEASE {
            Release::Preserve => {}
            Release::Detach => slot.slot_mut().detach(),
            Release::Finish => slot.finish(),
        }
    }
}

impl<'slot, 'd, R> Ref<'slot, 'd, R> {
    pub(super) const fn new(slot: &'slot retry::Slot<'d>) -> Self {
        Self {
            slot,
            role: marker::PhantomData,
        }
    }
}

impl<'d> Ref<'_, 'd, Cleanup> {
    pub(crate) fn progress(&self) -> schedule::Progress<'d> {
        self.slot.progress()
    }

    pub(crate) fn park(&self, now: clock::Moment<'d>) -> retry::Park {
        self.slot.park(now)
    }
}

impl Role for Open {
    const RELEASE: Release = Release::Preserve;

    fn limits(config: &config::Config) -> config::Contention {
        config.work.open.contention
    }
}

impl Role for Cleanup {
    const RELEASE: Release = Release::Detach;

    fn limits(config: &config::Config) -> config::Contention {
        config.work.cleanup.contention
    }
}

impl Role for Call {
    const RELEASE: Release = Release::Finish;

    fn limits(config: &config::Config) -> config::Contention {
        config.work.call.contention
    }
}

const _: () = {
    let word = size_of::<usize>();
    assert!(size_of::<Lease<'static, 'static, Open>>() == word);
    assert!(size_of::<Lease<'static, 'static, Cleanup>>() == word);
    assert!(size_of::<Lease<'static, 'static, Call>>() == word);
    assert!(size_of::<Ref<'static, 'static, Cleanup>>() == word);
};
