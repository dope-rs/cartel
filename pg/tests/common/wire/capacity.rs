use cartel_pg::{
    port::config,
    protocol::{
        Codec,
        frame::{Complete, Item, Parsed},
    },
};
use dope::manifold::client::session::codec::{self, Codec as _, Input, staging::Policy};
use o3::{
    base::cell::region,
    buffer::{resident, storage::Shared},
};
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

pub fn parse<T>(
    codec: &Codec,
    ingress: &Shared,
    project: impl for<'id, 'input, 'd> FnOnce(Complete<'id, 'input, 'd, Shared>) -> Item<'id, T>,
    inspect: impl for<'id> FnOnce(Parsed<'id, Result<T, cartel_pg::Error>>),
) {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(0, token.proof())
            .expect("fixture budget");
        let plan = (<<Codec as codec::Codec>::Staging as Policy>::plan(codec.staging()))
            .expect("fixture staging");
        let limit = <<Codec as codec::Codec>::Staging as Policy>::limit(plan);
        Input::with(ingress, &budget, limit, |input| {
            inspect(codec.parse(
                input,
                project,
                |_, error| Err(error),
                |project, frame| {
                    let (value, end) = project(frame)?;
                    Ok((Ok(value), end))
                },
            ))
        })
    })
}
