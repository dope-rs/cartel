use crate::{api, catalogs, retry};
use api::{stmt, work};
use dope_core::driver::schedule::clock;
use dope_fiber::context;
use retry::lease;
use std::{mem, pin, result, task};
use work::cleanup::Activity;

mod drain;
use drain::Drain;

pub(crate) struct Run<'query, 'connection, 'd, I: catalogs::Catalog> {
    engine: &'query mut stmt::Engine<'connection, I>,
    work: &'query work::Work,
    retry: lease::Lease<'query, 'd, lease::Cleanup>,
}

impl<'query, 'connection, 'd, I: catalogs::Catalog> Run<'query, 'connection, 'd, I> {
    pub(crate) const fn new(
        engine: &'query mut stmt::Engine<'connection, I>,
        work: &'query work::Work,
        retry: lease::Lease<'query, 'd, lease::Cleanup>,
    ) -> Self {
        Self {
            engine,
            work,
            retry,
        }
    }

    pub(crate) fn poll(
        &mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<result::Result<(), crate::Error>> {
        self.drain().poll(context)
    }

    pub(crate) fn poll_park(&mut self, now: clock::Moment<'d>) -> task::Poll<Activity> {
        self.drain().poll_park(now)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        &'query mut stmt::Engine<'connection, I>,
        &'query work::Work,
        lease::Lease<'query, 'd, lease::Cleanup>,
    ) {
        (self.engine, self.work, self.retry)
    }

    fn drain(&mut self) -> Drain<'_, 'query, 'connection, 'd> {
        Drain::new(
            self.engine.controls(),
            self.retry.config(),
            self.work,
            &mut self.retry,
        )
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Run<'static, 'static, 'static, catalogs::Empty>>() == 3 * word);
};
