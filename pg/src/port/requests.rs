use std::mem;

use cartel_core::{frame, queue, reply};
use dope::{
    core::driver::{route::KeyTag, schedule::app},
    manifold::client::{app::request, close, conn},
    net::link::egress::{self, data},
};
use o3::{
    base::cell::region,
    buffer::{Residency, pool},
    mem::credit::fair,
};
use port::{pool::slot, replies};
use reply::{order, recv::attach};

use crate::reply::Pressure;
use crate::{error, port, port::Disposition, protocol::encode, schema};

pub(crate) enum Rejection {
    Pressure(Pressure),
    Failed(error::Error),
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum Boundary {
    Close,
    Begin,
    Commit,
    Group,
    Open,
    External,
}

const _: () = {
    assert!(mem::size_of::<Boundary>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Boundary>() == mem::align_of::<bool>());
};

pub(crate) use crate::protocol::runtime::ready::{ReplyContext, Source};

pub(crate) struct Access<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8> {
    port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
}

impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Access<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) const fn new(port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>) -> Self {
        Self { port }
    }

    fn frame<T>(
        span: Option<pool::Span>,
        kind: frame::Sensitivity,
        write: impl FnOnce(&mut encode::Writer) -> Result<T, encode::Error>,
    ) -> Result<(Complete<'d>, T), error::Error> {
        let span = span.ok_or(error::Error::RequestCapacity)?;
        let (frame, value) =
            encode::Writer::encode(span, kind, write).map_err(error::Error::Request)?;
        let frame = egress::NonEmpty::try_new(Frame(Body::Owned(frame)))
            .map_err(error::Error::RequestLoad)?;
        Ok((frame, value))
    }

    pub(crate) fn encode(
        &self,
        span: Option<pool::Span>,
        write: impl FnOnce(&mut encode::Writer) -> Result<(), encode::Error>,
    ) -> Result<Complete<'d>, error::Error> {
        Self::frame(span, frame::Sensitivity::Plain, write).map(|(frame, ())| frame)
    }

    pub(crate) fn control(
        &self,
        write: impl FnOnce(&mut encode::Writer) -> Result<(), encode::Error>,
    ) -> Result<Complete<'d>, error::Error> {
        Self::frame(
            self.port.controls.acquire(),
            frame::Sensitivity::Plain,
            write,
        )
        .map(|(frame, ())| frame)
    }

    pub(crate) fn secret<T>(
        &self,
        write: impl FnOnce(&mut encode::Writer) -> Result<T, encode::Error>,
    ) -> Result<(Complete<'d>, T), error::Error> {
        Self::frame(
            self.port.controls.acquire(),
            frame::Sensitivity::Secret,
            write,
        )
    }

    fn request_lane(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> queue::Lane<'life, 'd, egress::NonEmpty<Frame<'d>>> {
        self.port
            .request_queue
            .lane_for(self.port.lane(token.lane()))
    }

    pub(crate) fn try_with<'a, J, K, R>(
        &self,
        context: ReplyContext<'_, 'a, 'd, ID>,
        input: J,
        prepare: impl FnOnce(
            &mut region::Token<region::Identity<'d>>,
            reply::Lane<
                'life,
                'b,
                'c,
                'd,
                replies::Row<'d>,
                replies::ReplyFailure<'d>,
                replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
            >,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
            J,
        ) -> Result<(order::Depth, K), (attach::Error, J)>,
        build: impl FnOnce(K, conn::Id<'d, KeyTag<ID>>) -> (Complete<'d>, R),
    ) -> Result<R, (Rejection, Source<'a, 'd, ID>, J)> {
        let (ready, source, boundary, region) = context.into_parts();
        let token = ready.connection();
        let permit = match ready.try_reply(boundary) {
            Ok(permit) => permit,
            Err(error) => return Err((Rejection::Failed(error), source, input)),
        };
        let replies = self.port.lane(token.lane());
        let conn = permit.slot();
        let batch = permit.batch();
        let result = self.port.request_queue.try_with(
            replies,
            region,
            replies::Tag::new(batch, boundary),
            (source, input),
            |region, lane, tag, (source, input)| {
                let (depth, input) = match prepare(region, lane, tag, input) {
                    Ok(prepared) => prepared,
                    Err((cause, input)) => return Err((cause, (source, input))),
                };
                let (bytes, done) = build(input, token);
                if !permit.commit() {
                    self.port.shared.wake_ready();
                }
                if let Source::Shared(listed) = source {
                    listed.reprioritize(depth.get() + conn.barriers());
                }
                Ok((bytes, done))
            },
        );
        match result {
            Ok(done) => {
                conn.wake();
                Ok(done)
            }
            Err((queue::Failure::Reply(cause), (source, input))) => {
                let cause = match cause {
                    attach::Error::Full { len, capacity } => {
                        Rejection::Pressure(Pressure::Full { len, capacity })
                    }
                    attach::Error::Capacity {
                        lane,
                        cause:
                            fair::Cause::Exhausted {
                                axis,
                                required,
                                available,
                            },
                    } => Rejection::Pressure(Pressure::Capacity {
                        lane,
                        axis,
                        required,
                        available,
                    }),
                    cause => Rejection::Failed(error::Error::Reply(cause)),
                };
                Err((cause, source, input))
            }
            Err((queue::Failure::Queue(cause), (source, input))) => {
                let queued = self.port.request_queue.lane_for(replies).weight(region);
                let cause = match cause {
                    queue::Rejection::Capacity(fair::Cause::Exhausted {
                        required,
                        available,
                        ..
                    }) => Rejection::Pressure(Pressure::Queue {
                        queued,
                        required,
                        available,
                    }),
                    cause => Rejection::Failed(error::Error::RequestQueue { queued, cause }),
                };
                Err((cause, source, input))
            }
        }
    }

    pub(crate) fn try_reply<'a, J, R>(
        &self,
        input: J,
        reply: &mut impl attach::Attach<
            'life,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
        context: ReplyContext<'_, 'a, 'd, ID>,
        build: impl FnOnce(J, conn::Id<'d, KeyTag<ID>>) -> (Complete<'d>, R),
    ) -> Result<R, (Rejection, Source<'a, 'd, ID>, J)> {
        self.try_with(
            context,
            input,
            |token, lane, tag, input| match reply.try_ordered(token, lane, tag) {
                Ok(depth) => Ok((depth, input)),
                Err(cause) => Err((cause, input)),
            },
            build,
        )
    }

    pub(crate) fn try_enqueue(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        bytes: Complete<'d>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), error::Error> {
        let Some(conn) = self.port.conn(token) else {
            return Err(error::Error::Closed);
        };
        let ready = conn.ready(token)?;
        let requests = self.request_lane(token);
        let queued = requests.weight(region);
        requests
            .try_push(region, bytes)
            .map_err(|queue::Rejected { cause, .. }| error::Error::RequestQueue {
                queued,
                cause,
            })?;
        ready.wake();
        Ok(())
    }

    pub(crate) fn drain(
        &self,
        requests: &mut request::Requests<'_, '_, '_, 'd, Frame<'d>, KeyTag<ID>>,
    ) {
        let token = requests.connection();
        let Some(conn) = self.port.conn(token) else {
            return;
        };
        match conn.egress() {
            slot::Egress::Open => {}
            slot::Egress::Close(close) => {
                requests.close(close);
                return;
            }
        }
        let lane = self.request_lane(token);
        let queued = lane.len(requests.region());
        let result = lane.drain(requests);
        if lane.len(requests.region()) != queued {
            self.port.shared.signals.egress.notify();
        }
        let Err(cause) = result else {
            return;
        };
        let error = error::Error::RequestReserve(cause);
        let failure = replies::Failure::from_error(error);
        let close = close::Directive::cut_retry(failure.close_reason());
        self.port
            .shared
            .wake_close(token, failure, Disposition::Reconnect);
        requests.close(close);
    }

    pub(crate) fn try_sync(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), error::Error> {
        let bytes = egress::NonEmpty::try_new(Frame::from(encode::COPY_DONE_SYNC.as_slice()))
            .map_err(error::Error::RequestLoad)?;
        let Some(conn) = self.port.conn(token) else {
            return Err(error::Error::Closed);
        };
        let sync = conn.ready(token)?.try_finish()?;
        let requests = self.request_lane(token);
        let queued = requests.weight(region);
        let unchanged = requests
            .try_with(region, bytes, |_, bytes| Ok((bytes, sync.commit())))
            .map_err(|(cause, _)| error::Error::RequestQueue { queued, cause })?;
        if !unchanged {
            self.port.shared.wake_ready();
        }
        conn.wake();
        Ok(())
    }

    pub(crate) fn retire(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        work: app::Work<'_, 'd>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> egress::Cleared {
        if self.port.conn(token).is_none() {
            return egress::Cleared::Done;
        }
        let requests = self.request_lane(token);
        let queued = requests.len(region);
        let result = requests.clear(work, region, drop);
        if requests.len(region) != queued {
            self.port.shared.signals.egress.notify();
        }
        match result {
            queue::Drain::Empty => egress::Cleared::Done,
            queue::Drain::Exhausted => egress::Cleared::Retry,
        }
    }
}

enum Body<'d> {
    Owned(frame::Buffer),
    Borrowed(&'d [u8]),
}

pub(crate) type Complete<'d> = egress::NonEmpty<Frame<'d>>;

#[repr(transparent)]
#[doc(hidden)]
pub struct Frame<'d>(Body<'d>);

impl<'d> From<&'d [u8]> for Frame<'d> {
    fn from(value: &'d [u8]) -> Self {
        Self(Body::Borrowed(value))
    }
}

impl AsRef<[u8]> for Frame<'_> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Body::Owned(frame) => frame.as_ref(),
            Body::Borrowed(bytes) => bytes,
        }
    }
}

impl Residency for Frame<'_> {
    fn resident(&self) -> usize {
        match &self.0 {
            Body::Owned(frame) => frame.resident(),
            Body::Borrowed(_) => 0,
        }
    }
}

// SAFETY: each variant fixes a contiguous slice and its exact retained charge
// while borrowed.
unsafe impl<'d> data::raw::Contiguous<'d> for Frame<'d> {}

const _: () = {
    assert!(mem::size_of::<Frame<'static>>() == mem::size_of::<frame::Buffer>());
    assert!(mem::align_of::<Frame<'static>>() == mem::align_of::<frame::Buffer>());
    assert!(
        mem::size_of::<Complete<'static>>()
            == mem::size_of::<Frame<'static>>() + mem::size_of::<egress::Geometry>()
    );
    assert!(mem::size_of::<Option<Complete<'static>>>() == mem::size_of::<Complete<'static>>());
};
