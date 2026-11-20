use std::num::NonZero;

use cartel_pg::port::config;
use o3::base::time::Positive;

use crate::common::external::runtime::Error;

pub struct Limits {
    pub connections: NonZero<usize>,
    pub inflight: Option<NonZero<usize>>,
    pub window: Option<Positive>,
    pub groups: Option<config::Groups>,
}

impl Default for Limits {
    fn default() -> Self {
        Self::new(NonZero::<usize>::MIN)
    }
}

impl Limits {
    pub const fn new(max: NonZero<usize>) -> Self {
        Self {
            connections: max,
            inflight: None,
            window: None,
            groups: None,
        }
    }

    pub fn try_config(self) -> Result<config::Config, Error> {
        let frames = self
            .connections
            .checked_mul(NonZero::<usize>::MIN.saturating_add(15))
            .ok_or(Error::Frames(self.connections))?;
        let caps = config::Capacities {
            connections: self.connections.get(),
            inflight: self.inflight.unwrap_or(frames).get(),
            admission_waiters: frames.get(),
            requests: config::Request {
                frames: frames.get(),
                frame_bytes: 4 * 1024,
            },
            responses: config::Response {
                items: 65_536,
                bytes: 256 * 1024 * 1024,
                frame_bytes: 16 * 1024 * 1024,
            },
            notifications: config::Notification {
                items: 1024,
                bytes: 4 * 1024 * 1024,
            },
        };
        let settings = match self.window {
            Some(window) => config::Config::with_window(caps, window),
            None => config::Config::new(caps),
        }
        .map_err(Error::Config)?;
        match self.groups {
            Some(groups) => settings.with_groups(groups).map_err(Error::Config),
            None => Ok(settings),
        }
    }
}
