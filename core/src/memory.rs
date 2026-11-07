use dope::manifold::client::session::ingress::{self, Source as _};
use o3::{
    buffer::{
        hold::{Policy, limits},
        resident::staging,
    },
    depot::capacity,
};

pub const RETENTION: Policy = Policy::new(4 * 1024, 2);

pub fn try_staging(frame: usize) -> Result<staging::Plan, staging::Error> {
    staging::Plan::new(frame.min(16 * 1024), frame)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    admitted_bytes: limits::Bound,
    ingress_bytes: usize,
    retained_bytes: limits::Bound,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Ingress {
    pub active: usize,
    pub retained: limits::Bound,
    pub owners: capacity::Count,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Retention(limits::Error),
    Ingress {
        connections: usize,
        frame_bytes: usize,
    },
    RetainedPending(limits::Error),
    RetainedExact(limits::Error),
    Staging(staging::Error),
}

impl Layout {
    pub fn try_framed(
        policy: Policy,
        bytes: usize,
        conns: usize,
        frame: usize,
        exact: usize,
    ) -> Result<Self, Error> {
        let admit = policy.try_limit(bytes).map_err(Error::Retention)?;
        let input = conns.checked_mul(frame).ok_or(Error::Ingress {
            connections: conns,
            frame_bytes: frame,
        })?;
        let retained = admit.try_add(input).map_err(Error::RetainedPending)?;
        let retained = retained.try_add(exact).map_err(Error::RetainedExact)?;
        Ok(Self {
            admitted_bytes: admit,
            ingress_bytes: input,
            retained_bytes: retained,
        })
    }

    pub const fn admitted(self) -> limits::Bound {
        self.admitted_bytes
    }

    pub const fn pending(self) -> usize {
        self.ingress_bytes
    }

    pub const fn ingress(self, owners: capacity::Count) -> Ingress {
        Ingress {
            active: self.ingress_bytes,
            retained: self.retained_bytes,
            owners,
        }
    }
}

impl Ingress {
    pub fn try_resident(self, conns: usize, frame: usize) -> Result<limits::Bound, Error> {
        let plan = try_staging(frame).map_err(Error::Staging)?;
        let shared = ingress::Local::new(self.owners)
            .shared(ingress::Bytes::from_limit(self.retained), plan.warm());
        let layout = staging::Layout::new(conns, plan, shared.get(), self.retained.bytes())
            .map_err(Error::Staging)?;
        Ok(limits::Bound::new(layout.resident().get()))
    }
}

const _: () = assert!(core::mem::size_of::<Layout>() == 3 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Ingress>() == 3 * core::mem::size_of::<usize>());
