use crate::runtime::slot::{self, local::Owned};
use crate::{api::work, catalogs, config, open, retry};
use o3::mem::{self, lease::Open};
use std::{marker, ptr};

use retry::lease;

pub(super) enum Ready {}

pub(super) struct Lease<'slot, 'd, I: catalogs::Catalog, P> {
    inner: mem::Lease<Delivery<'slot, 'd, I>, Open>,
    phase: marker::PhantomData<P>,
}

struct Inner<'slot, 'd, I: catalogs::Catalog> {
    work: &'slot work::Work,
    retry: retry::raw::Loan<'slot, 'd>,
    conn: slot::Reclaim<'slot, I>,
}

/// Delivers after borrowed statements, retry state, connection, and allocator scope are released.
struct Delivery<'slot, 'd, I: catalogs::Catalog>(
    &'slot mut open::Completion,
    marker::PhantomData<Inner<'slot, 'd, I>>,
);

impl<'slot, 'd, I: catalogs::Catalog, P> Lease<'slot, 'd, I, P> {
    pub(super) const fn config(&self) -> &'slot config::Config {
        self.inner.item().retry.config()
    }

    pub(super) const fn work(&self) -> &'slot work::Work {
        self.inner.item().work
    }
}

impl<'slot, 'd, I: catalogs::Catalog> Lease<'slot, 'd, I, Open> {
    pub(super) fn new(
        owned: ptr::NonNull<Owned<I>>,
        retry: retry::raw::Loan<'slot, 'd>,
        work: &'slot work::Work,
        report: &'slot mut open::Completion,
    ) -> Self {
        Self {
            inner: mem::Lease::open(
                Delivery(report, marker::PhantomData),
                Inner {
                    work,
                    retry,
                    conn: slot::Reclaim::new(owned),
                },
            ),
            phase: marker::PhantomData,
        }
    }

    pub(super) fn open(&mut self) -> lease::Lease<'_, 'd, lease::Open> {
        self.inner.item_mut().retry.open()
    }

    pub(super) fn ready(mut self) -> Lease<'slot, 'd, I, Ready> {
        self.inner.item_mut().retry.open().finish();
        Lease {
            inner: self.inner,
            phase: marker::PhantomData,
        }
    }
}

impl<'d, I: catalogs::Catalog> Lease<'_, 'd, I, Ready> {
    pub(super) fn cleanup(&mut self) -> lease::Lease<'_, 'd, lease::Cleanup> {
        self.inner.item_mut().retry.cleanup()
    }

    pub(super) const fn cleanup_ref(&self) -> lease::Ref<'_, 'd> {
        self.inner.item().retry.cleanup_ref()
    }
}

impl<'slot, 'd, I: catalogs::Catalog> mem::Return for Delivery<'slot, 'd, I> {
    type Item = Inner<'slot, 'd, I>;

    fn return_item(self, item: Self::Item) {
        let Inner {
            work,
            mut retry,
            conn,
        } = item;
        retry.finish();
        drop(conn);
        if let Some(errors) = work.take_errors() {
            self.0.complete(errors);
        }
    }
}

const _: () = {
    use std::mem;

    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Delivery<'_, '_, catalogs::Empty>>() == word);
    assert!(mem::size_of::<Inner<'_, '_, catalogs::Empty>>() == word * 3);
    assert!(mem::size_of::<slot::Reclaim<'_, catalogs::Empty>>() == word);
    assert!(mem::size_of::<Lease<'_, '_, catalogs::Empty, Open>>() == word * 4);
    assert!(
        mem::align_of::<Lease<'_, '_, catalogs::Empty, Open>>()
            == mem::align_of::<Inner<'_, '_, catalogs::Empty>>()
    );
    assert!(
        mem::size_of::<Lease<'_, '_, catalogs::Empty, Open>>()
            == mem::size_of::<Lease<'_, '_, catalogs::Empty, Ready>>()
    );
    assert!(
        mem::align_of::<Lease<'_, '_, catalogs::Empty, Open>>()
            == mem::align_of::<Lease<'_, '_, catalogs::Empty, Ready>>()
    );
};
