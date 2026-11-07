use crate::{options, runtime, sys};
use std::{mem, time};

#[derive(Debug)]
pub enum Error {
    Catalog(crate::Error),
    ParameterItemsZero,
    ParameterItems {
        actual: usize,
    },
    ParameterBytesZero,
    ScanRowsZero,
    RowBytesZero,
    RowBytes {
        actual: usize,
    },
    CollectRows {
        actual: usize,
        maximum: usize,
    },
    CollectBytes {
        actual: usize,
        required: usize,
    },
    StatementBytes {
        actual: usize,
        maximum: usize,
    },
    MainCacheBytes {
        actual: usize,
    },
    MainCacheInteger {
        actual: usize,
    },
    TemporaryCacheBytes {
        actual: usize,
    },
    TemporaryCacheInteger {
        actual: usize,
    },
    Contention {
        operation: Operation,
        interval: time::Duration,
        window: time::Duration,
    },
    Savepoints {
        actual: usize,
        maximum: usize,
    },
    Runtime(runtime::Error),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Operation {
    Open,
    Call,
}

pub struct Target<'sql> {
    location: Location,
    options: options::OpenOptions,
    commands: sys::Commands<'sql>,
}

const _: () = {
    assert!(!mem::needs_drop::<Error>());
    assert!(mem::size_of::<Error>() <= 5 * mem::size_of::<usize>());
};

pub(crate) enum Location {
    File(sys::Filename),
    Memory,
}

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    File,
    Memory,
}

impl Location {
    pub(crate) const fn kind(&self) -> Kind {
        match self {
            Self::File(_) => Kind::File,
            Self::Memory => Kind::Memory,
        }
    }
}

impl<'sql> Target<'sql> {
    pub(crate) fn new(
        _: runtime::Runtime,
        location: Location,
        options: options::OpenOptions,
    ) -> Self {
        Self {
            location,
            options,
            commands: sys::Commands::EMPTY,
        }
    }

    /// Runs parameterless, columnless commands before catalog preparation.
    /// Each runs in autocommit mode; later failure preserves earlier changes.
    /// Attaching validates only SQL text and does not open the database.
    pub fn commands(mut self, sql: &'sql [&'sql str]) -> Result<Self, crate::Error> {
        self.commands = sys::Commands::try_new(sql)?;
        Ok(self)
    }

    pub(crate) const fn sql_bytes(&self) -> usize {
        self.commands.max_bytes()
    }

    pub(crate) fn into_parts(self) -> (Location, options::OpenOptions, sys::Commands<'sql>) {
        (self.location, self.options, self.commands)
    }
}
