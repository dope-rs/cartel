use cartel_redis::{
    client::settings, client::settings::Capacities, client::settings::Config, protocol,
    protocol::parser, protocol::parser::capacity, protocol::parser::storage::Storage,
};
use dope::{
    manifold::client::session::codec,
    manifold::client::session::codec::Codec as _,
    manifold::client::session::codec::Stateful as _,
    manifold::client::session::codec::staging::{Limit, Policy},
    manifold::client::session::codec::storage::Slot,
    manifold::client::session::response::Direct,
};
use o3::{
    alloc, base::cell::region, buffer::hold, buffer::resident, buffer::resident::staging,
    buffer::storage, buffer::storage::Shared, buffer::write::consume::scope, mem::rc,
};
use std::pin::Pin;

pub fn with_state<'storage, 'g>(
    codec: &protocol::Codec<'storage, 'g>,
    op: impl FnOnce(parser::Parser<'storage, 'g>),
) -> Result<(), Error> {
    try_with_budget(|budget| op(codec.state(budget))).map_err(Error::Budget)
}

pub fn config(bytes: usize, values: usize) -> Result<Config, settings::Error> {
    Config::new(Capacities {
        connections: 1,
        admission_waiters: 1,
        inflight: 1,
        request_frame_bytes: 1,
        response_bytes: bytes,
        response_values: values,
        response_frame_bytes: bytes,
        response_frame_values: values,
    })
}

/// Builds the exact codec over `store`, sharing retained frames with their source.
pub fn new_codec<'storage, 'g>(
    limits: Config,
    store: &'storage Storage<'g>,
) -> protocol::Codec<'storage, 'g> {
    protocol::Codec::new(
        limits.max_frame(),
        limits.frame_values(),
        store,
        hold::Policy::share(),
    )
}

pub fn with_reply(
    bytes: &[u8],
    cap: usize,
    count: usize,
    use_: impl for<'storage, 'd, 'g> FnOnce(Result<protocol::Decoded<'storage, 'd, 'g>, scope::Error>),
) -> Result<(), Error> {
    let frame = Shared::try_copy(bytes).map_err(Error::Frame)?;
    let capacity = capacity::Capacity::try_new(1, count, count).map_err(Error::Capacity)?;
    let store = Storage::try_new(capacity).map_err(Error::Storage)?;
    let limits = config(cap, count).map_err(Error::Config)?;
    let codec = new_codec(limits, &store);
    try_with_budget(|budget| {
        let mut state = codec.state(budget);
        let mut emitted = false;
        let (rest, step) = parse_chunk(&codec, &mut state, &frame, |decoded| {
            emitted = true;
            use_(decoded);
        })?;
        let consumed = step.has_consumed();
        let actual = bytes.len().checked_sub(rest.len()).ok_or(Error::Extent)?;
        let expected = bytes.len().min(cap);
        if !emitted || !consumed || actual != expected {
            return Err(Error::Progress {
                emitted,
                consumed,
                actual,
                expected,
            });
        }
        Ok(())
    })
    .map_err(Error::Budget)?
}

pub fn parse_chunk<'storage, 'g>(
    codec: &protocol::Codec<'storage, 'g>,
    state: &mut parser::Parser<'storage, 'g>,
    frame: &Shared,
    use_: impl for<'d> FnOnce(Result<protocol::Decoded<'storage, 'd, 'g>, scope::Error>),
) -> Result<(Shared, codec::Step<Direct>), Error> {
    let mut source = frame
        .try_get(..)
        .map_err(Error::Share)?
        .ok_or(Error::Extent)?;
    try_with_budget(|budget| {
        let limit = staging_limit(codec)?;
        let step: codec::Step<Direct> = codec.decode(&mut source, state, &budget, limit, |_| {
            codec::Admission::Ready(|decoded, _: &mut parser::Parser<'storage, 'g>| use_(decoded))
        });
        Ok((source, step))
    })
    .map_err(Error::Budget)?
}

pub fn finish_chunk<'storage, 'g>(
    codec: &protocol::Codec<'storage, 'g>,
    state: parser::Parser<'storage, 'g>,
    frame: &Shared,
    use_: impl for<'d> FnOnce(Result<protocol::Decoded<'storage, 'd, 'g>, scope::Error>),
) -> Result<codec::Finished<(Shared, parser::Parser<'storage, 'g>), Direct>, Error> {
    let source = frame
        .try_get(..)
        .map_err(Error::Share)?
        .ok_or(Error::Extent)?;
    try_with_budget(|budget| {
        let limit = staging_limit(codec)?;
        let (input, result) = Slot::new((source, state)).run(|input| {
            codec.finish(input, &budget, limit, |_| {
                codec::Admission::Ready(|decoded, _: &mut parser::Parser<'storage, 'g>| {
                    use_(decoded)
                })
            })
        });
        Ok((input.into_parts(), result))
    })
    .map_err(Error::Budget)?
}

pub fn staging_limit(codec: &protocol::Codec<'_, '_>) -> Result<Limit, Error> {
    let plan =
        <<protocol::Codec<'_, '_> as codec::Codec>::Staging as Policy>::plan(codec.staging())
            .map_err(Error::Staging)?;
    Ok(<<protocol::Codec<'_, '_> as codec::Codec>::Staging as Policy>::limit(plan))
}

#[derive(Debug)]
pub enum Error {
    Budget(resident::Reuse),
    Frame(storage::AllocError),
    Share(rc::Error),
    Extent,
    Capacity(capacity::Error),
    Storage(alloc::Error),
    Config(settings::Error),
    Staging(staging::Error),
    Progress {
        emitted: bool,
        consumed: bool,
        actual: usize,
        expected: usize,
    },
}

pub fn try_with_budget<R>(
    op: impl for<'d> FnOnce(resident::Budget<'d>) -> R,
) -> Result<R, resident::Reuse> {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        Ok(op(domain.get_mut().try_budget(0, token.proof())?))
    })
}
