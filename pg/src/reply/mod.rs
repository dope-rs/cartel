use crate::api;
use cartel_core::reply::recv::attach;
use dope::{
    core::driver::route::KeyTag,
    manifold::client::{attempt, conn},
    net::link::pool::change::opening,
};
use dope_fiber::wait;

pub(crate) use code::Class;
pub use code::Code;
pub use context::Context;
pub use notice::Notice;
pub(crate) use plan::{Plan, Scope};

mod code;
mod context;
pub mod group;
mod notice;
pub mod owned;
mod plan;
pub(crate) mod raw;
pub mod server;

pub enum Error<'d, const ID: u8 = 0> {
    Driver(crate::Error),
    Wait(wait::Error<'d>),
    Admission {
        conn: conn::Id<'d, KeyTag<ID>>,
        cause: attach::Error,
        error: wait::Error<'d>,
    },
    Open(Open<'d, ID>),
    Pinned(api::Target<'d, ID>),
    Server(server::Diagnostic<'d>),
}

/// Exact reply failures borrowed without moving retained wake or response owners.
#[derive(Debug)]
pub enum Kind<'a, 'd, const ID: u8 = 0> {
    Driver(&'a crate::Error),
    Wait(wait::Cause),
    Admission {
        conn: &'a conn::Id<'d, KeyTag<ID>>,
        cause: &'a attach::Error,
        error: wait::Cause,
    },
    Open(&'a Open<'d, ID>),
    Pinned(&'a api::Target<'d, ID>),
    Server(server::Fields<'a>),
}

impl<'d, const ID: u8> Error<'d, ID> {
    pub fn kind(&self) -> Kind<'_, 'd, ID> {
        match self {
            Self::Driver(error) => Kind::Driver(error),
            Self::Wait(error) => Kind::Wait(error.cause()),
            Self::Admission { conn, cause, error } => Kind::Admission {
                conn,
                cause,
                error: error.cause(),
            },
            Self::Open(error) => Kind::Open(error),
            Self::Pinned(target) => Kind::Pinned(target),
            Self::Server(error) => Kind::Server(error.fields()),
        }
    }

    pub fn as_driver(&self) -> Option<&crate::Error> {
        match self {
            Self::Driver(error) => Some(error),
            Self::Wait(_)
            | Self::Admission { .. }
            | Self::Open(_)
            | Self::Pinned(_)
            | Self::Server(_) => None,
        }
    }

    pub fn target(&self) -> Option<api::Target<'d, ID>> {
        match self {
            Self::Pinned(target) => Some(*target),
            Self::Driver(_)
            | Self::Wait(_)
            | Self::Admission { .. }
            | Self::Open(_)
            | Self::Server(_) => None,
        }
    }

    pub fn as_server<'a>(&'a self) -> Option<&'a server::Diagnostic<'d>> {
        match self {
            Self::Server(error) => Some(error),
            Self::Driver(_)
            | Self::Wait(_)
            | Self::Admission { .. }
            | Self::Open(_)
            | Self::Pinned(_) => None,
        }
    }

    pub fn try_own(self) -> Result<owned::Error, (Self, owned::Cause)> {
        match self {
            Self::Driver(error) => Ok(owned::Error::Driver(error)),
            Self::Wait(error) => Err((Self::Wait(error), owned::Cause::Bound)),
            Self::Admission { conn, cause, error } => {
                Err((Self::Admission { conn, cause, error }, owned::Cause::Bound))
            }
            Self::Open(cause) => Err((Self::Open(cause), owned::Cause::Bound)),
            Self::Pinned(target) => Err((Self::Pinned(target), owned::Cause::Bound)),
            Self::Server(error) => match error.try_own() {
                Ok(error) => Ok(owned::Error::Server(error)),
                Err((error, cause)) => Err((Self::Server(error), owned::Cause::Allocation(cause))),
            },
        }
    }
}

impl<const ID: u8> From<crate::Error> for Error<'_, ID> {
    fn from(error: crate::Error) -> Self {
        Self::Driver(error)
    }
}

/// One permanently failed connection attempt with its branded identity.
#[derive(Clone, Copy, Debug)]
pub enum Open<'d, const ID: u8> {
    Transport {
        key: attempt::Id<'d, KeyTag<ID>>,
        cause: attempt::ConnectFault,
    },
    Exhaustion {
        key: attempt::Id<'d, KeyTag<ID>>,
        cause: opening::Exhaustion,
    },
}

const _: () = {
    assert!(size_of::<Open<'static, 0>>() <= 4 * size_of::<usize>());
    assert!(align_of::<Open<'static, 0>>() == align_of::<usize>());
};
