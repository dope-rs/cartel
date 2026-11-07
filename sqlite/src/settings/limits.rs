use std::{num, time};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub parameters: Parameters,
    pub scan: Scan,
    /// Maximum live SQLite savepoint frames in one transaction.
    pub savepoints: usize,
    pub program: Program,
    pub work: Work,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Work {
    /// One aggregate bound for configuration, initialization, and statement preparation.
    /// Charges a final quantum per preparation and execution to bound partial intervals.
    pub open: Budget,
    /// One bound for a query or transaction-control call, including busy retries.
    pub call: Budget,
    /// One bound for deferred rollback and savepoint cleanup.
    pub cleanup: Progress,
}

impl Work {
    pub const fn uniform(budget: Budget) -> Self {
        Self {
            open: budget,
            call: budget,
            cleanup: budget.progress,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// Aggregate SQLite virtual-machine work for the whole operation.
    pub progress: Progress,
    /// Cooperative lock-contention policy for the whole operation.
    pub busy: Busy,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Parameters {
    pub items: usize,
    pub bytes: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Scan {
    pub rows: usize,
    pub row_bytes: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Progress(num::NonZero<u64>);

impl Progress {
    pub const fn new(quanta: num::NonZero<u64>) -> Self {
        Self(quanta)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Program(num::NonZero<u32>);

impl Program {
    pub const fn new(ops: num::NonZero<u32>) -> Self {
        Self(ops)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }

    pub(crate) fn sqlite_ops(self) -> num::NonZero<i32> {
        self.0
            .min(num::NonZero::<i32>::MAX.cast_unsigned())
            .cast_signed()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Busy {
    /// Maximum elapsed time from the first busy result.
    pub window: time::Duration,
    /// Retry delay: zero yields once; positive reserves and reuses one driver timer slot.
    pub interval: time::Duration,
    /// Maximum number of attempts after the first busy result.
    pub retries: num::NonZero<u32>,
}
