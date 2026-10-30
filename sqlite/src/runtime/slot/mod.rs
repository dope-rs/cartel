use crate::catalogs;
use held::Held;
pub(crate) use local::{Local, Vacant};
pub(crate) use opening::Opening;
pub(crate) use prepare::Prepare;
pub(crate) use ready::{Ready, Retirement};
use std::{marker, ptr};

mod held;
mod lease;
mod local;
mod opening;
mod prepare;
mod ready;

pub(in crate::runtime::slot) struct Reclaim<'slot, I: catalogs::Catalog> {
    raw: ptr::NonNull<local::Owned<I>>,
    lifetime: marker::PhantomData<&'slot mut local::Owned<I>>,
}

impl<'slot, I: catalogs::Catalog> Reclaim<'slot, I> {
    const fn new(raw: ptr::NonNull<local::Owned<I>>) -> Self {
        Self {
            raw,
            lifetime: marker::PhantomData,
        }
    }
}

impl<I: catalogs::Catalog> Drop for Reclaim<'_, I> {
    fn drop(&mut self) {
        // SAFETY: this pointer targets the pinned local slot for `'slot`.
        // Every borrower is stored before its Lease, so it is dead before the
        // connection is cleared. Its owning Scope remains active until every
        // native statement and the connection have been released.
        unsafe { self.raw.as_mut() }.finish();
    }
}
