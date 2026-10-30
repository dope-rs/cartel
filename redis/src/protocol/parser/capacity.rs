use std::mem;

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Capacity {
    nodes: u32,
    connections: usize,
    lanes: usize,
}

const SHARED_LANES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Lanes { connections: usize, values: usize },
    Total { lanes: usize, retained: usize },
    Above { actual: usize, maximum: usize },
}

impl Capacity {
    pub fn try_new(conns: usize, frame: usize, retained: usize) -> Result<Self, Error> {
        Self::try_with(conns, conns.min(SHARED_LANES), frame, retained)
    }

    pub(crate) fn try_with(
        conns: usize,
        active: usize,
        frame: usize,
        retained: usize,
    ) -> Result<Self, Error> {
        let lanes = active.checked_mul(frame).ok_or(Error::Lanes {
            connections: active,
            values: frame,
        })?;
        let total = lanes
            .checked_add(retained)
            .ok_or(Error::Total { lanes, retained })?;
        let maximum = u32::MAX as usize;
        if total > maximum {
            return Err(Error::Above {
                actual: total,
                maximum,
            });
        }
        Ok(Self {
            nodes: total as u32,
            connections: conns,
            lanes: active,
        })
    }

    pub(crate) const fn get(self) -> usize {
        self.nodes as usize
    }

    pub(crate) const fn connections(self) -> usize {
        self.connections
    }

    pub(crate) const fn lanes(self) -> usize {
        self.lanes
    }
}

const _: [(); 24] = [(); mem::size_of::<Capacity>()];
