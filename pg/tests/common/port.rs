use cartel_core::reply::recv::attach;
use cartel_pg::{api, port::config, reply, reply::server, schema, schema::groups};
use dope::{core::driver::route::KeyTag, manifold::client::conn};
use dope_fiber::wait;
use std::num::NonZero;

#[derive(Debug)]
pub enum Reply<'a, 'd> {
    Driver(&'a cartel_pg::Error),
    Wait {
        cause: wait::Cause,
        wake: Option<(NonZero<u16>, u32)>,
    },
    Admission {
        conn: &'a conn::Id<'d, KeyTag<0>>,
        cause: &'a attach::Error,
        error: wait::Cause,
        wake: Option<(NonZero<u16>, u32)>,
    },
    Open(&'a reply::Open<'d, 0>),
    Pinned(&'a api::Target<'d, 0>),
    Server(server::Fields<'a>),
}

impl<'a, 'd> From<&'a reply::Error<'d>> for Reply<'a, 'd> {
    fn from(error: &'a reply::Error<'d>) -> Self {
        let wake = match error {
            reply::Error::Wait(error) | reply::Error::Admission { error, .. } => match error {
                wait::Error::QueueFull { wake, .. } | wait::Error::SlotFull(wake) => {
                    Some((wake.key().slot(), wake.key().generation().get()))
                }
                wait::Error::Foreign | wait::Error::Index { .. } => None,
            },
            _ => None,
        };
        match error {
            reply::Error::Driver(cause) => Self::Driver(cause),
            reply::Error::Wait(error) => Self::Wait {
                cause: error.cause(),
                wake,
            },
            reply::Error::Admission { conn, cause, error } => Self::Admission {
                conn,
                cause,
                error: error.cause(),
                wake,
            },
            reply::Error::Open(cause) => Self::Open(cause),
            reply::Error::Pinned(target) => Self::Pinned(target),
            reply::Error::Server(cause) => Self::Server(cause.fields()),
        }
    }
}

pub struct Queries;

impl schema::QuerySet for Queries {
    type Groups = groups::End;

    const PREPARE: schema::Prepare<'_> = {
        const LEN: usize = schema::Prepare::fixed_len::<Queries>();
        static WIRE: schema::Wire<'_, LEN> = schema::Prepare::fixed::<Queries, LEN>();
        WIRE.prepare()
    };
}

impl Queries {
    pub fn try_config() -> Result<cartel_pg::Config, cartel_pg::ConfigError> {
        cartel_pg::Config::new(
            String::from("user"),
            String::from("password"),
            String::from("database"),
        )
    }

    pub fn try_cancel() -> Result<config::Config, config::Error> {
        let capacity = config::Capacities {
            connections: 1,
            inflight: 2,
            admission_waiters: 2,
            requests: config::Request {
                frames: 2,
                frame_bytes: 512,
            },
            responses: config::Response {
                items: 2,
                bytes: 256,
                frame_bytes: 128,
            },
            notifications: config::Notification {
                items: 1,
                bytes: 128,
            },
        };
        config::Config::new(capacity)
    }

    pub fn try_groups(
        records: NonZero<usize>,
        bytes: NonZero<usize>,
    ) -> Result<config::Config, config::Error> {
        let config = config::Config::new(config::Capacities {
            connections: 1,
            inflight: 8,
            admission_waiters: 2,
            requests: config::Request {
                frames: 8,
                frame_bytes: 1024,
            },
            responses: config::Response {
                items: 16,
                bytes: 4096,
                frame_bytes: 1024,
            },
            notifications: config::Notification {
                items: 1,
                bytes: 128,
            },
        })?;
        config.with_groups(config::Groups {
            active: NonZero::<usize>::MIN,
            records,
            bytes,
        })
    }

    pub fn try_reply() -> Result<config::Config, config::Error> {
        config::Config::new(config::Capacities {
            connections: 1,
            inflight: 2,
            admission_waiters: 2,
            requests: config::Request {
                frames: 2,
                frame_bytes: 512,
            },
            responses: config::Response {
                items: 2,
                bytes: 2_048,
                frame_bytes: 1_024,
            },
            notifications: config::Notification {
                items: 1,
                bytes: 128,
            },
        })
    }
}
