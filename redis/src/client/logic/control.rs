use cartel_core::{frame, reply};
use dope::{
    core::driver::{
        route,
        schedule::{self, app::Work},
    },
    manifold::client::{
        app::{self, request},
        close, conn, session,
    },
};
use o3::base::{cell::region, time::Positive};

use crate::{
    Port, client,
    port::Stop,
    protocol::{self, parser},
};

#[repr(transparent)]
pub(super) struct Control<'a, 's, 'b, 'c, 'd, const ID: u8> {
    pub(super) port: &'a Port<'s, 'a, 'b, 'c, 'd, ID>,
}

impl<'s, 'b, 'c, 'd, const ID: u8> Control<'_, 's, 'b, 'c, 'd, ID> {
    pub(super) fn register(
        self,
        entry: &conn::Activation<'_, 'd, route::KeyTag<ID>>,
    ) -> app::Admission {
        match self.port.schedule.activate(entry) {
            Ok(()) => app::Admission::Attached,
            Err(reason) => {
                self.stop(Stop::Failed(client::Failure::from_close(reason.reason())));
                app::Admission::Rejected(reason)
            }
        }
    }

    pub(super) fn stop(self, cause: Stop<'d, ID>) {
        self.port.activity.record_stop(cause);
        self.port.activity.wake_all();
    }

    pub(super) fn drain(
        self,
        reqs: &mut request::Requests<'_, '_, '_, 'd, frame::Buffer, route::KeyTag<ID>>,
    ) {
        let token = reqs.connection();
        let Err(cause) = self.port.schedule.drain(reqs) else {
            return;
        };
        reqs.close(close::Directive::cut_retry(cause.close_reason()));
        self.port.fail(token, cause, reqs.region());
    }

    pub(super) fn blocked(
        self,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        state: &mut parser::Parser<'s, 'd>,
    ) -> session::Blocking {
        if let Some(target) = self.port.schedule.routes.target(conn) {
            match state.builder.access.wait(conn, target) {
                Ok(()) => session::Blocking::Registered,
                Err(cause) => {
                    self.stop(Stop::Failed(client::Failure::ParserWait(cause)));
                    session::Blocking::Unsupported
                }
            }
        } else {
            session::Blocking::Unsupported
        }
    }

    pub(super) fn fence(
        self,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        use reply::order::Depth;

        match self.port.schedule.responses(conn) {
            Some(reps) if reps.depth(region) != Depth::ZERO => app::Fence::Pending,
            Some(_) | None => app::Fence::Ready,
        }
    }

    pub(super) fn park(self, work: Work<'_, 'd>, region: &mut region::Token<region::Identity<'d>>) {
        if let Some(released) = self.port.schedule.maintain(work, region) {
            self.port.activity.release(released);
        }
        self.port.activity.flush(work);
    }

    pub(super) fn progress(self) -> schedule::Progress<'d> {
        use reply::Work;
        use schedule::Progress;

        match self.port.schedule.work() {
            Work::Pending => Progress::Runnable,
            Work::Idle if self.port.activity.is_pending() => Progress::Runnable,
            Work::Idle => Progress::Quiescent,
        }
    }

    pub(super) fn inbound(
        self,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        state: &protocol::ConnState,
        base: Positive,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        use protocol::Phase;
        use reply::order::Depth;

        match state.phase() {
            Phase::Init(_) => app::Inbound::Awaiting(base),
            Phase::Offline | Phase::Ready => match self.port.schedule.responses(conn) {
                Some(reps) if reps.depth(region) != Depth::ZERO => app::Inbound::Awaiting(base),
                Some(_) | None => app::Inbound::Quiescent,
            },
        }
    }
}

const _: () = assert!(
    size_of::<Control<'static, 'static, 'static, 'static, 'static, 0>>() == size_of::<usize>()
);
