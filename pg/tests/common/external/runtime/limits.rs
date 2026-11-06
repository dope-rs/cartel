use std::num::NonZero;

use cartel_pg::port::config;
use o3::base::time::Positive;

use crate::common::external::runtime::Error;

pub struct Limits {
    connections: NonZero<usize>,
    inflight: Option<NonZero<usize>>,
    window: Option<Positive>,
    groups: Option<config::Groups>,
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

    pub(super) const fn inflight(mut self, inflight: NonZero<usize>) -> Self {
        self.inflight = Some(inflight);
        self
    }

    pub(super) const fn window(mut self, window: Positive) -> Self {
        self.window = Some(window);
        self
    }

    pub(super) const fn groups(mut self, groups: Option<config::Groups>) -> Self {
        self.groups = groups;
        self
    }

    pub fn try_config<R>(self) -> Result<config::Config, Error<R>> {
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
