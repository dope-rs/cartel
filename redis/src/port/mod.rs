use std::{cell, mem, pin};

use cartel_core::reply::layout::Position;
use cartel_core::{deadline, memory, reply};
use client::{plan, settings};
use dope::{
    core::driver::{self, route, schedule::app},
    manifold::{
        client::{attempt, conn},
        service::bindings::{self, endpoint, failures::External},
    },
    net::link::pool::change::opening,
    runtime::client::Bound,
};
use dope_fiber::{context, wait};
use o3::base::cell::region;
use parser::{limit, storage};

use crate::{
    client,
    client::settings::init,
    protocol::{self, parser},
    request,
    request::encode,
};

mod failures;
mod requests;
pub(crate) mod schedule;

struct Signal<'d> {
    waiters: wait::Queue<'d>,
    notice: cell::Cell<Notice>,
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Notice {
    Idle,
    Pending,
}

impl<'d> Signal<'d> {
    fn notify(&self) {
        self.notice.set(Notice::Pending);
    }

    fn flush(&self, work: app::Work<'_, 'd>) {
        if matches!(self.notice.replace(Notice::Idle), Notice::Idle) {
            return;
        }
        let notice = match self.waiters.wake_one(work) {
            wait::WakeStatus::Complete if self.waiters.len() == wait::Length::ZERO => Notice::Idle,
            wait::WakeStatus::Complete | wait::WakeStatus::Pending => Notice::Pending,
        };
        self.notice.set(notice);
    }

    fn is_pending(&self) -> bool {
        matches!(self.notice.get(), Notice::Pending)
    }
}

const _: [(); 1] = [(); mem::size_of::<Notice>()];

pub(super) struct Activity<'b, 'd, const ID: u8> {
    dispatch: Signal<'d>,
    leases: Signal<'d>,
    failures: region::Value<region::Identity<'d>, failures::Failures<'b, 'd, ID>>,
    terminal: cell::Cell<Option<Stop<'d, ID>>>,
}

pub(super) enum Stop<'d, const ID: u8> {
    Failed(client::Failure<'d>),
    Exhausted {
        key: attempt::Id<'d, route::KeyTag<ID>>,
        cause: opening::Exhaustion,
    },
}

const _: () = {
    assert!(mem::size_of::<Stop<'static, 17>>() == mem::size_of::<client::Failure<'static>>());
    assert!(
        mem::size_of::<Option<Stop<'static, 17>>>()
            == mem::size_of::<Option<client::Failure<'static>>>()
    );
};

impl<'b, 'd, const ID: u8> Activity<'b, 'd, ID> {
    pub(super) fn try_dispatch<'target, 'poll>(
        &'target self,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        ctx: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> Result<(), wait::Error<'d>> {
        self.dispatch.waiters.try_register(waiter, ctx)
    }

    pub(super) fn try_lease<'target, 'poll>(
        &'target self,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        ctx: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> Result<(), wait::Error<'d>> {
        self.leases.waiters.try_register(waiter, ctx)
    }

    /// The terminal projection restores its owner before any callback or drop.
    pub(super) fn fatal_cause(
        &self,
        region: &region::Token<region::Identity<'d>>,
    ) -> Option<crate::Error<'d, ID>> {
        if let Some(cause) = region.borrow(&self.failures).cause() {
            return Some(cause);
        }
        let terminal = self.terminal.take();
        let cause = terminal.as_ref().map(|cause| match cause {
            Stop::Failed(cause) => client::Failure::from(cause).into_error(),
            Stop::Exhausted { key, cause } => crate::Error::ConnectionExhausted {
                key: *key,
                cause: *cause,
            },
        });
        self.terminal.set(terminal);
        cause
    }

    pub(super) fn clear_fatal(
        &self,
        position: Position<'b, 'd, route::KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        let previous = region.borrow_mut(&self.failures).clear(position);
        drop(previous);
    }

    pub(super) fn record_fatal(
        &self,
        position: Position<'b, 'd, route::KeyTag<ID>>,
        err: client::Failure<'d>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        let previous = region.borrow_mut(&self.failures).record(position, err);
        drop(previous);
    }

    pub(super) fn record_stop(&self, err: Stop<'d, ID>) {
        let previous = self.terminal.take();
        match previous {
            Some(previous) => {
                self.terminal.set(Some(previous));
                drop(err);
            }
            None => self.terminal.set(Some(err)),
        }
    }

    pub(super) fn wake_send(&self) {
        self.dispatch.notify();
    }

    pub(super) fn wake_lease(&self) {
        self.leases.notify();
    }

    pub(super) fn release(&self, _: reply::Released<'_>) {
        self.wake_all();
    }

    pub(super) fn wake_all(&self) {
        self.wake_send();
        self.wake_lease();
    }

    pub(super) fn flush(&self, work: app::Work<'_, 'd>) {
        self.dispatch.flush(work);
        self.leases.flush(work);
    }

    pub(super) fn is_pending(&self) -> bool {
        self.dispatch.is_pending() || self.leases.is_pending()
    }
}

pub(super) struct Limits {
    pub(super) max_frame: limit::Bound<limit::Frame>,
    pub(super) frame_values: limit::Bound<limit::Value>,
}

pub struct Port<'s, 'storage, 'b, 'c, 'd, const ID: u8 = 0> {
    pub(super) schedule: schedule::Schedule<'s, 'storage, 'b, 'c, 'd, ID>,
    pub(super) activity: Activity<'b, 'd, ID>,
    pub(super) deadline: deadline::Source<'d>,
    pub(super) limits: Limits,
    pub(super) parser: &'s storage::Storage<'d>,
    requests: requests::Requests,
}

impl<'s, 'storage, 'b, 'c, 'd, const ID: u8> Port<'s, 'storage, 'b, 'c, 'd, ID> {
    pub(super) fn is_active(&self) -> bool {
        self.schedule.routes.is_active()
    }

    pub(super) fn codec(&self) -> protocol::Codec<'s, 'd> {
        protocol::Codec::new(
            self.limits.max_frame,
            self.limits.frame_values,
            self.parser,
            memory::RETENTION,
        )
    }

    pub fn bind<'a>(
        &'a self,
        plan: init::Plan,
    ) -> bindings::Bound<
        'a,
        'd,
        Self,
        client::Logic<'s, 'd>,
        route::KeyTag<ID>,
        { client::CONNECTOR_CONNECTION_LIMIT },
    > {
        bindings::Bound::bind(self, client::Logic::new(self, plan))
    }

    pub fn bind_with<'a, F>(
        &'a self,
        plan: init::Plan,
        failure: F,
    ) -> bindings::Bound<
        'a,
        'd,
        Self,
        client::Logic<'s, 'd>,
        route::KeyTag<ID>,
        { client::CONNECTOR_CONNECTION_LIMIT },
        External<F>,
    > {
        bindings::Bound::bind_with(self, client::Logic::new(self, plan), failure)
    }

    /// Runs the consumer while parser and reply credit storage remain alive.
    pub fn try_with<C, R>(
        config: settings::Config,
        state: &mut C,
        mut driver: impl for<'a> FnMut(&'a mut C) -> driver::Context<'a, 'd>,
        run: impl for<'a, 'loan, 'e, 'f> FnOnce(Port<'a, 'loan, 'e, 'f, 'd, ID>, &mut C) -> R,
    ) -> Result<R, plan::Error> {
        let parser = storage::Storage::try_new(config.parser())?;
        let proof = driver(state).region_token().proof();
        reply::Arena::try_with(config.arena(), proof, |replies| {
            let port = {
                let mut driver = driver(state);
                let capacity = config.waiters();
                let send = wait::Queue::new(driver.driver_ref(), capacity);
                let lease = wait::Queue::new(driver.driver_ref(), capacity);
                let requests = requests::Requests::try_new(config)?;
                let failures = failures::Failures::try_new(replies.domain())?;
                Port {
                    schedule: schedule::Schedule::try_new(config, driver.region_token(), replies)?,
                    activity: Activity {
                        dispatch: Signal {
                            waiters: send,
                            notice: cell::Cell::new(Notice::Idle),
                        },
                        leases: Signal {
                            waiters: lease,
                            notice: cell::Cell::new(Notice::Idle),
                        },
                        failures: driver.region_token().bind(failures),
                        terminal: cell::Cell::new(None),
                    },
                    deadline: deadline::Source::new(driver.driver_ref().timer(), config.window),
                    limits: Limits {
                        max_frame: config.max_frame(),
                        frame_values: config.frame_values(),
                    },
                    parser: &parser,
                    requests,
                }
            };
            Ok(run(port, state))
        })?
    }

    pub(super) fn fail(
        &self,
        token: conn::Id<'d, route::KeyTag<ID>>,
        err: client::Failure<'d>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        if let Some(position) = self
            .schedule
            .fail(token, client::Failure::from(&err), region)
        {
            self.activity.record_fatal(position, err, region);
            self.activity.wake_all();
        }
    }

    pub(super) fn encode(
        &self,
        encode: impl FnOnce(encode::Encoder) -> Result<encode::Complete, request::Error>,
    ) -> Result<encode::Complete, request::Error> {
        let frame = self.requests.frame()?;
        encode(encode::Encoder::new(frame))
    }

    pub(super) fn encode_sized(
        &self,
        length: encode::Length,
        encode: impl FnOnce(encode::Writer) -> Result<encode::Complete, request::Error>,
    ) -> Result<encode::Complete, request::Error> {
        encode(self.requests.sized(length)?)
    }
}

impl<'life, 's, 'storage, 'b, 'c, 'd: 'life, const ID: u8>
    endpoint::Provider<'life, 'd, route::KeyTag<ID>, { client::CONNECTOR_CONNECTION_LIMIT }>
    for Port<'s, 'storage, 'b, 'c, 'd, ID>
where
    Self: 'life,
{
    type Client<'app, B: ?Sized + 'app>
        = client::Redis<'app, 's, 'b, 'c, 'd, B, ID>
    where
        'life: 'app,
        'd: 'app;

    fn provide<'app, B: ?Sized + 'app>(
        &'life self,
        bound: Bound<'app, 'd, B>,
    ) -> Self::Client<'app, B>
    where
        'life: 'app,
        'd: 'app,
    {
        client::Redis {
            port: bound.bind(self),
        }
    }
}
