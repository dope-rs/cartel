use cartel_redis::protocol::ConnState;
use dope::{
    core::{Errno, driver, driver::route::KeyTag},
    manifold::client::{
        app, close, conn,
        session::{self, app::drain::Ready, turns},
    },
    net::link::egress,
};
use o3::base::cell::region;

struct Foreign<N>(N);

impl<'d, N: session::Session<'d, ConnState = ConnState>> session::Session<'d> for Foreign<N> {
    type Codec = N::Codec;
    type Connect = N::Connect;
    type ConnState = ConnState;
    type Failure = N::Failure;
    type Requests = N::Requests;
    type Send = N::Send;

    fn codec(&self) -> &Self::Codec {
        self.0.codec()
    }
}

impl<'d, N: session::Retirement<'d, ConnState = ConnState>> session::Retirement<'d> for Foreign<N> {
    type Drain = Ready;

    fn io_fail<'e>(
        &mut self,
        cause: Errno,
        ctx: &mut turns::Context<'_, 'e, 'd, ConnState, Self::Send>,
    ) {
        self.0.io_fail(cause, ctx);
    }

    fn quarantine(
        &self,
        token: conn::Id<'d, KeyTag<0>>,
        state: &mut ConnState,
        reason: close::Reason,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        self.0.quarantine(token, state, reason, regions);
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, ctx: turns::Retired<'_, 'd, ConnState, KeyTag<0>, Self>) {
        self.0.disconnect(ctx);
    }
}

fn erase<'d, N>(
    ctx: turns::Retired<'_, 'd, ConnState, KeyTag<0>, Foreign<N>>,
) -> turns::Drained<'d, KeyTag<0>, N> {
    let reason = ctx.reason();
    let proof: turns::Drained<'d, KeyTag<0>, N> = ctx.into_drained();
    assert_eq!(proof.reason(), reason);
    proof
}

fn native<'w, 'd: 'w, A: app::Lifecycle<'w, 'd>, B>(
    target: &mut A,
    proof: conn::Retired<'d, KeyTag<0>, B>,
    state: &mut A::Conn,
    driver: &mut driver::Context<'_, 'd>,
) {
    let proof: conn::Retired<'d, KeyTag<0>, A> = proof;
    target.drained(proof, state, driver);
}

fn main() {}
