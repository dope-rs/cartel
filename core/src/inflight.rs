use crate::error::Error;

pub struct Inflight {
    pub max: usize,
    pub total: usize,
}

const DEFAULT_MAX_INFLIGHT: usize = 8192;

impl Default for Inflight {
    fn default() -> Self {
        Self {
            max: DEFAULT_MAX_INFLIGHT,
            total: 0,
        }
    }
}

impl Inflight {
    pub fn check(&self) -> Result<(), Error> {
        if self.total >= self.max {
            return Err(Error::Backpressure {
                inflight: self.total,
                queued: 0,
                cap: self.max,
            });
        }
        Ok(())
    }

    pub fn inc(&mut self) {
        self.total = self.total.saturating_add(1);
    }

    pub fn dec(&mut self) {
        self.total = self.total.saturating_sub(1);
    }

    pub fn dec_n(&mut self, n: usize) {
        self.total = self.total.saturating_sub(n);
    }

    pub fn set_max(&mut self, max: usize) {
        self.max = max;
    }
}
