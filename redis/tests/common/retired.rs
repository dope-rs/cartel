use cartel_redis::protocol::ConnState;
use dope::{
    core::driver::route::KeyTag,
    manifold::client::{
        close,
        session::{self, turns},
    },
};
use o3::base::cell::region;

fn accept<'d, N: session::Retirement<'d, ConnState = ConnState>>(
    target: &mut N,
    ctx: turns::Retired<'_, 'd, ConnState, KeyTag<0>, N>,
    regions: &mut (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ),
) {
    let token = ctx.connection();
    target.quarantine(token, ctx.state, close::Reason::Protocol, regions);
    target.disconnect(ctx);
}

fn retain<'d, N>(
    ctx: turns::Retired<'_, 'd, ConnState, KeyTag<0>, N>,
) -> turns::Drained<'d, KeyTag<0>, N> {
    let reason = ctx.reason();
    let proof = ctx.into_drained();
    assert_eq!(proof.reason(), reason);
    proof
}

fn main() {}
