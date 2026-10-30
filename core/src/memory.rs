use o3::{
    buffer::hold::{Policy, limits},
    depot::capacity,
};

pub const RETENTION: Policy = Policy::new(4 * 1024, 2);

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    policy: Policy,
    admitted_bytes: limits::Bound,
    ingress_bytes: usize,
    retained_bytes: limits::Bound,
    resident_bytes: limits::Bound,
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
    ResidentIngress(limits::Error),
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
        let resident = retained.try_add(input).map_err(Error::ResidentIngress)?;
        Ok(Self {
            policy,
            admitted_bytes: admit,
            ingress_bytes: input,
            retained_bytes: retained,
            resident_bytes: resident,
        })
    }

    pub const fn policy(self) -> Policy {
        self.policy
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

    pub const fn resident(self) -> limits::Bound {
        self.resident_bytes
    }
}

const _: () = assert!(core::mem::size_of::<Layout>() == 6 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Ingress>() == 3 * core::mem::size_of::<usize>());
