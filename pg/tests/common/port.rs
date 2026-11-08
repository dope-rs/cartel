use cartel_pg::{port::config, reply, schema, schema::groups};
use dope_fiber::wait;
use std::num::NonZero;

#[derive(Debug)]
pub struct Reply<'a, 'd> {
    pub kind: reply::Kind<'a, 'd>,
    pub wake: Option<(NonZero<u32>, u32)>,
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
        Self {
            kind: error.kind(),
            wake,
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
