use std::mem;

use cartel_core::{frame, handoff, queue, reply};
use dope::{
    core::driver::{route::KeyTag, schedule::app},
    manifold::client::{app::request, close, conn},
    net::link::egress::{self, data},
};
use o3::{
    base::cell::region,
    buffer::{Residency, pool},
};
use port::{pool::slot, replies};
use reply::{order, recv::attach};

use crate::{error, port, port::Disposition, protocol::encode, schema};

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

pub(crate) struct ReplyContext<'a, 'd, const ID: u8> {
    token: conn::Id<'d, KeyTag<ID>>,
    boundary: Boundary,
    region: &'a mut region::Token<region::Identity<'d>>,
}

impl<'a, 'd, const ID: u8> ReplyContext<'a, 'd, ID> {
    pub(crate) const fn new(
        token: conn::Id<'d, KeyTag<ID>>,
        boundary: Boundary,
        region: &'a mut region::Token<region::Identity<'d>>,
    ) -> Self {
        Self {
            token,
            boundary,
            region,
        }
    }
}

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
        pool: &pool::Pool,
        kind: frame::Sensitivity,
        write: impl FnOnce(&mut encode::Writer) -> Result<T, encode::Error>,
    ) -> Result<(Complete<'d>, T), error::Error> {
        let cursor = pool.cursor().ok_or(error::Error::RequestCapacity)?;
        let (frame, value) =
            encode::Writer::encode(cursor, kind, write).map_err(error::Error::Request)?;
        Ok((Complete::try_new(frame)?, value))
    }

    pub(crate) fn encode(
        &self,
        write: impl FnOnce(&mut encode::Writer) -> Result<(), encode::Error>,
    ) -> Result<Complete<'d>, error::Error> {
        Self::frame(&self.port.requests, frame::Sensitivity::Plain, write).map(|(frame, ())| frame)
    }

    pub(crate) fn control(
        &self,
        write: impl FnOnce(&mut encode::Writer) -> Result<(), encode::Error>,
    ) -> Result<Complete<'d>, error::Error> {
        Self::frame(&self.port.controls, frame::Sensitivity::Plain, write).map(|(frame, ())| frame)
    }

    pub(crate) fn secret<T>(
        &self,
        write: impl FnOnce(&mut encode::Writer) -> Result<T, encode::Error>,
    ) -> Result<(Complete<'d>, T), error::Error> {
        Self::frame(&self.port.controls, frame::Sensitivity::Secret, write)
    }

    fn request_lane(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> queue::Lane<'life, 'd, egress::NonEmpty<Frame<'d>>> {
        self.port
            .request_queue
            .lane_for(self.port.lane(token.lane()))
    }

    pub(crate) fn try_with<J, K, R>(
        &self,
        context: ReplyContext<'_, 'd, ID>,
        weight: usize,
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
        build: impl FnOnce(K) -> (Complete<'d>, R),
    ) -> Result<R, (error::Error, J)> {
        let ReplyContext {
            token,
            boundary,
            region,
        } = context;
        let permit = match self
            .port
            .conn(token)
            .ok_or(error::Error::Closed)
            .and_then(|conn| conn.ready(token)?.try_reply(boundary))
        {
            Ok(permit) => permit,
            Err(error) => return Err((error, input)),
        };
        let replies = self.port.lane(token.lane());
        let cause = match handoff::Lane::new(&self.port.request_queue, replies)
            .try_prepare(region, weight)
        {
            Ok(lane) => {
                let conn = permit.slot();
                let (batch, boundary) = permit.parts();
                let tag = replies::Tag::new(batch, boundary);
                let result = lane.try_with(tag, input, prepare, |input, depth| {
                    let (bytes, done) = build(input);
                    if !permit.commit() {
                        self.port.shared.wake_ready();
                    }
                    self.port.shared.reindex(token, depth);
                    (bytes.into_frame(), done)
                });
                return match result {
                    Ok(done) => {
                        conn.wake();
                        Ok(done)
                    }
                    Err((cause, input)) => Err((error::Error::Reply(cause), input)),
                };
            }
            Err(cause) => cause,
        };
        Err((
            error::Error::RequestQueue {
                queued: self.port.request_queue.lane_for(replies).weight(region),
                cause,
            },
            input,
        ))
    }

    pub(crate) fn try_reply<S, R>(
        &self,
        bytes: Complete<'d>,
        state: S,
        reply: &mut impl attach::Attach<
            'life,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
        context: ReplyContext<'_, 'd, ID>,
        seal: impl FnOnce(S) -> R,
    ) -> Result<R, (error::Error, Complete<'d>, S)> {
        self.try_with(
            context,
            bytes.as_ref().len(),
            (bytes, state),
            |token, lane, tag, input| match reply.try_ordered(token, lane, tag) {
                Ok(depth) => Ok((depth, input)),
                Err(cause) => Err((cause, input)),
            },
            |(bytes, state)| (bytes, seal(state)),
        )
        .map_err(|(error, (bytes, state))| (error, bytes, state))
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
        let request = match requests.try_reserve(region, bytes.as_ref().len()) {
            Ok(request) => request,
            Err(cause) => return Err(error::Error::RequestQueue { queued, cause }),
        };
        request.publish(bytes.into_frame());
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
        let Err(cause) = lane.drain(requests) else {
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
        let request = match requests.try_reserve(region, encode::COPY_DONE_SYNC.len()) {
            Ok(request) => request,
            Err(cause) => return Err(error::Error::RequestQueue { queued, cause }),
        };
        let unchanged = sync.commit();
        request.publish(bytes);
        if !unchanged {
            self.port.shared.wake_ready();
        }
        conn.wake();
        Ok(())
    }

    pub(crate) fn try_flush<E>(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
        send: impl FnOnce(&mut region::Token<region::Identity<'d>>) -> Result<(), E>,
    ) -> Result<(), E> {
        let Some(conn) = self.port.conn(token) else {
            return Ok(());
        };
        let sync = match conn.flush(token) {
            slot::Flush::Idle => return Ok(()),
            slot::Flush::Pending(sync) => sync,
        };
        let requests = self.request_lane(token);
        if !requests.is_empty(region) {
            return Ok(());
        }
        send(region)?;
        if !sync.commit() {
            self.port.shared.wake_ready();
        }
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
        match requests.clear(work, region, drop) {
            queue::Drain::Empty => egress::Cleared::Done,
            queue::Drain::Exhausted => egress::Cleared::Retry,
        }
    }
}

enum Body<'d> {
    Owned(frame::Buffer),
    Borrowed(&'d [u8]),
    Inline(data::Inline<16>),
}

pub(crate) struct Complete<'d>(egress::NonEmpty<Frame<'d>>);

#[repr(transparent)]
#[doc(hidden)]
pub struct Frame<'d>(Body<'d>);

impl<'d> Complete<'d> {
    fn try_new(frame: frame::Buffer) -> Result<Self, error::Error> {
        let frame = Frame(Body::Owned(frame));
        Ok(Self(
            egress::NonEmpty::try_new(frame).map_err(error::Error::RequestLoad)?,
        ))
    }

    pub(crate) fn into_frame(self) -> egress::NonEmpty<Frame<'d>> {
        self.0
    }
}

impl<'d> From<&'d [u8]> for Frame<'d> {
    fn from(value: &'d [u8]) -> Self {
        Self(Body::Borrowed(value))
    }
}

impl<'d> From<data::Inline<16>> for Frame<'d> {
    fn from(value: data::Inline<16>) -> Self {
        Self(Body::Inline(value))
    }
}

impl AsRef<[u8]> for Complete<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_inner().as_ref()
    }
}

impl AsRef<[u8]> for Frame<'_> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Body::Owned(frame) => frame.as_ref(),
            Body::Borrowed(bytes) => bytes,
            Body::Inline(bytes) => bytes.as_ref(),
        }
    }
}

impl Residency for Frame<'_> {
    fn resident(&self) -> usize {
        match &self.0 {
            Body::Owned(frame) => frame.resident(),
            Body::Borrowed(_) => 0,
            Body::Inline(bytes) => bytes.resident(),
        }
    }
}

// SAFETY: each variant fixes a contiguous slice and its exact retained charge
// while borrowed. Moving an inline body is allowed only after that borrow ends.
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
