use cartel_pg::port::config;
use o3::buffer::resident::staging;
use o3::{base::cell::region, buffer::resident};
use std::pin::Pin;

pub fn try_frame(frame: usize) -> Result<config::Config, config::Error> {
    config::Config::new(config::Capacities {
        connections: 1,
        inflight: 1,
        admission_waiters: 1,
        requests: config::Request {
            frames: 1,
            frame_bytes: 64,
        },
        responses: config::Response {
            items: 1,
            bytes: 64,
            frame_bytes: frame,
        },
        notifications: config::Notification {
            items: 1,
            bytes: 64,
        },
    })
}

use cartel_pg::protocol::{Codec, frame::Head};
use dope::{
    manifold::client::session::codec, manifold::client::session::codec::Codec as _,
    manifold::client::session::codec::Input, manifold::client::session::codec::Parse,
    manifold::client::session::codec::Stateless as _,
    manifold::client::session::codec::staging::Policy,
};
use o3::{buffer::storage::Shared, buffer::write::consume::scope};

pub fn try_parse(
    codec: &Codec,
    ingress: &Shared,
    inspect: impl for<'id, 'input, 'd> FnOnce(Parse<'id, Head<'input, 'd, Shared>>),
) -> Result<(), Error> {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(0, token.proof())
            .map_err(Error::Budget)?;
        let plan = (<<Codec as codec::Codec>::Staging as Policy>::plan(codec.staging()))
            .map_err(Error::Staging)?;
        let limit = <<Codec as codec::Codec>::Staging as Policy>::limit(plan);
        Input::with(ingress, &budget, limit, |input| {
            let parsed = (codec.parse(input)).map_err(Error::Source)?;
            inspect(parsed);
            Ok(())
        })
    })
}

#[derive(Debug)]
pub enum Error {
    Budget(resident::Reuse),
    Staging(staging::Error),
    Source(scope::Error),
}
