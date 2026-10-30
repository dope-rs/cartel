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
use port::{
    pool::slot::{self, Change},
    replies,
};
use reply::{order, recv::attach};

use crate::{error, port, port::Disposition, protocol::encode, schema};

#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum Boundary {
    Close,
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

    fn frame(&self) -> Result<pool::Cursor, error::Error> {
        self.port
            .requests
            .cursor()
            .ok_or(error::Error::RequestCapacity)
    }

    pub(crate) fn encode(
        &self,
        write: impl FnOnce(&mut encode::Writer) -> Result<(), encode::Error>,
    ) -> Result<Complete<'d>, error::Error> {
        let (frame, ()) = encode::Writer::encode(self.frame()?, frame::Sensitivity::Plain, write)
            .map_err(error::Error::Request)?;
        Complete::try_new(frame)
    }

    pub(crate) fn secret<T>(
        &self,
        write: impl FnOnce(&mut encode::Writer) -> Result<T, encode::Error>,
    ) -> Result<(Complete<'d>, T), error::Error> {
        let (frame, value) =
            encode::Writer::encode(self.frame()?, frame::Sensitivity::Secret, write)
                .map_err(error::Error::Request)?;
        Ok((Complete::try_new(frame)?, value))
    }

    fn request_lane(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> queue::Lane<'life, 'd, egress::NonEmpty<Frame<'d>>> {
        self.port
            .request_queue
            .lane_for(self.port.lane(token.lane()))
    }

    pub(crate) fn prepare<'turn>(
        &self,
        context: ReplyContext<'turn, 'd, ID>,
        weight: usize,
    ) -> Result<Prepared<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>, error::Error> {
        let ReplyContext {
            token,
            boundary,
            region,
        } = context;
        let conn = self.port.conn(token).ok_or(error::Error::Closed)?;
        let ready = conn.ready(token)?;
        let requests = self.request_lane(token);
        let queued = requests.weight(region);
        let permit = ready.try_reply(boundary)?;
        let replies = self.port.lane(token.lane());
        let lane = handoff::Lane::new(&self.port.request_queue, replies)
            .try_prepare(region, weight)
            .map_err(|cause| error::Error::RequestQueue { queued, cause })?;
        Ok(Prepared {
            lane,
            commit: Commit {
                port: self.port,
                token,
                permit,
            },
            conn,
            boundary,
        })
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
        let prepared = match self.prepare(context, bytes.as_ref().len()) {
            Ok(prepared) => prepared,
            Err(error) => return Err((error, bytes, state)),
        };
        prepared.try_reply(bytes, state, reply, seal)
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
        bytes: Complete<'d>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), error::Error> {
        let Some(conn) = self.port.conn(token) else {
            return Err(error::Error::Closed);
        };
        let sync = conn.ready(token)?.try_finish()?;
        let requests = self.request_lane(token);
        let queued = requests.weight(region);
        let request = match requests.try_reserve(region, bytes.as_ref().len()) {
            Ok(request) => request,
            Err(cause) => return Err(error::Error::RequestQueue { queued, cause }),
        };
        let eligible = sync.commit();
        request.publish(bytes.into_frame());
        if matches!(eligible, Change::Available) {
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
        if matches!(sync.commit(), Change::Available) {
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

/// A request whose connection, Sync transition, and queue vacancy are established.
/// No reply order is installed until `try_reply` publishes the same frame.
pub(crate) struct Prepared<
    'turn,
    'life,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'life,
    I: schema::QuerySet,
    const ID: u8,
> {
    lane: handoff::Prepared<
        'turn,
        'life,
        'life,
        'b,
        'c,
        'd,
        egress::NonEmpty<Frame<'d>>,
        replies::Row<'d>,
        replies::ReplyFailure<'d>,
        replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
    >,
    commit: Commit<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    conn: &'life slot::Slot<'d, ID>,
    boundary: Boundary,
}

struct Commit<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8> {
    port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    token: conn::Id<'d, KeyTag<ID>>,
    permit: slot::Permit<'life>,
}

impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Commit<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    fn finish<J, R, F: FnOnce(J) -> (Complete<'d>, R)>(
        (this, input, build): (Self, J, F),
        depth: order::Depth,
    ) -> (egress::NonEmpty<Frame<'d>>, R) {
        let (bytes, done) = build(input);
        if matches!(this.permit.commit(), Change::Available) {
            this.port.shared.wake_ready();
        }
        this.port.shared.reindex(this.token, depth);
        (bytes.into_frame(), done)
    }
}

fn seal<'d, S, R, F: FnOnce(S) -> R>(
    (bytes, state, seal): (Complete<'d>, S, F),
) -> (Complete<'d>, R) {
    (bytes, seal(state))
}

impl<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Prepared<'turn, 'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(crate) fn try_reply<S, R>(
        self,
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
        finish: impl FnOnce(S) -> R,
    ) -> Result<R, (error::Error, Complete<'d>, S)> {
        self.try_publish(reply, (bytes, state, finish), seal)
            .map_err(|(error, (bytes, state, _))| (error, bytes, state))
    }

    pub(crate) fn try_publish<J, R>(
        self,
        reply: &mut impl attach::Attach<
            'life,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
        input: J,
        build: impl FnOnce(J) -> (Complete<'d>, R),
    ) -> Result<R, (error::Error, J)> {
        self.try_with(
            input,
            |token, lane, tag, input| match reply.try_ordered(token, lane, tag) {
                Ok(depth) => Ok((depth, input)),
                Err(cause) => Err((cause, input)),
            },
            build,
        )
    }

    pub(crate) fn try_with<J, K, R>(
        self,
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
        let tag = replies::Tag::new(self.commit.permit.batch(), self.boundary);
        let result = self.lane.try_with(
            tag,
            (self.commit, input, build),
            |token, lane, tag, (commit, input, build)| match prepare(token, lane, tag, input) {
                Ok((depth, output)) => Ok((depth, (commit, output, build))),
                Err((cause, input)) => Err((cause, (commit, input, build))),
            },
            Commit::finish,
        );
        match result {
            Ok(done) => {
                self.conn.wake();
                Ok(done)
            }
            Err((cause, (_, input, _))) => Err((error::Error::Reply(cause), input)),
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
