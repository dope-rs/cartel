use dope::manifold::Manifold;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::runtime::Session as RuntimeSession;
use dope_net::Transport;
use dope_net::wire::Wire;

use crate::{Client, Port, QuerySet};

/// Attaches a PostgreSQL client and its connector resource to a runtime session.
#[inline(always)]
pub fn attach<'scope, 'd: 'scope, const ID: u8, E, I>(
    session: &mut RuntimeSession<'scope, 'd, Port<'d, I>>,
    upstreams: impl Dialer<E::Transport> + 'd,
) -> std::io::Result<(Client<'d, I>, impl Manifold<'d> + 'd)>
where
    E: Env + 'd,
    E::Transport: Transport,
    <E::Wire as Wire>::InitConfig: Default,
    I: QuerySet,
{
    let port = session.storage();
    let client = port.client();
    let connector = {
        let mut driver = session.driver_access();
        port.connect::<ID, _, E>(upstreams, &mut driver)?
    };
    Ok((client, connector))
}

/// Attaches a PostgreSQL client with core-local wire runtime configuration.
///
/// `wire` is consumed by this connector's wire runtime. In a thread-per-core
/// application, call this once per runtime session with that core's config.
#[inline(always)]
pub fn attach_configured<'scope, 'd: 'scope, const ID: u8, E, I>(
    session: &mut RuntimeSession<'scope, 'd, Port<'d, I>>,
    upstreams: impl Dialer<E::Transport> + 'd,
    wire: <E::Wire as Wire>::InitConfig,
) -> std::io::Result<(Client<'d, I>, impl Manifold<'d> + 'd)>
where
    E: Env + 'd,
    E::Transport: Transport,
    I: QuerySet,
{
    let port = session.storage();
    let client = port.client();
    let connector = {
        let mut driver = session.driver_access();
        port.connect_configured::<ID, _, E>(upstreams, wire, &mut driver)?
    };
    Ok((client, connector))
}
