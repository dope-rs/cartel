use dope::manifold::Manifold;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::runtime::Session as RuntimeSession;
use dope_net::Transport;
use dope_net::wire::Wire;

use crate::{Connect, Redis, Store};

/// Attaches a Redis client and its connector resource to a runtime session.
#[inline(always)]
pub fn attach<'scope, 'd: 'scope, const ID: u8, E>(
    session: &mut RuntimeSession<'scope, 'd, Store<'d>>,
    topology: impl Dialer<E::Transport> + 'd,
) -> std::io::Result<(Redis<'d>, impl Manifold<'d> + 'd)>
where
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
    <E::Wire as Wire>::InitConfig: Default,
{
    let redis = session.storage().redis();
    let connector = {
        let mut driver = session.driver_access();
        redis.connect::<ID, _, E>(Connect { topology }, &mut driver)?
    };
    Ok((redis, connector))
}

/// Attaches a Redis client with core-local wire runtime configuration.
///
/// `wire` is consumed by this connector's wire runtime. In a thread-per-core
/// application, call this once per runtime session with that core's config.
#[inline(always)]
pub fn attach_configured<'scope, 'd: 'scope, const ID: u8, E>(
    session: &mut RuntimeSession<'scope, 'd, Store<'d>>,
    topology: impl Dialer<E::Transport> + 'd,
    wire: <E::Wire as Wire>::InitConfig,
) -> std::io::Result<(Redis<'d>, impl Manifold<'d> + 'd)>
where
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    let redis = session.storage().redis();
    let connector = {
        let mut driver = session.driver_access();
        redis.connect_configured::<ID, _, E>(Connect { topology }, wire, &mut driver)?
    };
    Ok((redis, connector))
}
