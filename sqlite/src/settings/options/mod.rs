use crate::{database, runtime, sys};
use std::{mem, num, path};

mod opening;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Journal {
    Rollback,
    Wal(Wal),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Wal {
    checkpoint: Checkpoint,
    retained_bytes: Option<u64>,
}

impl Wal {
    pub const fn new() -> Self {
        Self {
            checkpoint: Checkpoint::Auto,
            retained_bytes: None,
        }
    }

    pub const fn checkpoint(&mut self, mode: Checkpoint) -> &mut Self {
        self.checkpoint = mode;
        self
    }

    pub const fn retain(&mut self, bytes: Option<u64>) -> &mut Self {
        self.retained_bytes = bytes;
        self
    }
}

impl Default for Wal {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Checkpoint {
    Auto,
    Pages(num::NonZero<u32>),
    Manual,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Synchronous {
    Off,
    Normal,
    Full,
    Extra,
}

impl Synchronous {
    const fn value(self) -> i32 {
        match self {
            Self::Off => 0,
            Self::Normal => 1,
            Self::Full => 2,
            Self::Extra => 3,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
    Create,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    access: Access,
    journal: Journal,
    synchronous: Synchronous,
    mapped_bytes: u64,
    path_bytes: usize,
}

impl OpenOptions {
    const PATH_BYTES: usize = 4096;

    pub const fn new() -> Self {
        Self {
            access: Access::Create,
            journal: Journal::Rollback,
            synchronous: Synchronous::Full,
            mapped_bytes: 0,
            path_bytes: Self::PATH_BYTES,
        }
    }

    pub const fn access(&mut self, access: Access) -> &mut Self {
        self.access = access;
        self
    }

    pub const fn journal(&mut self, journal: Journal) -> &mut Self {
        self.journal = journal;
        self
    }

    pub const fn synchronous(&mut self, mode: Synchronous) -> &mut Self {
        self.synchronous = mode;
        self
    }

    pub const fn mapped_bytes(&mut self, bytes: u64) -> &mut Self {
        self.mapped_bytes = bytes;
        self
    }

    pub const fn path_bytes(&mut self, bytes: usize) -> &mut Self {
        self.path_bytes = bytes;
        self
    }

    pub fn open<'sql>(
        &self,
        runtime: runtime::Runtime,
        path: impl AsRef<path::Path>,
    ) -> Result<database::Target<'sql>, crate::Error> {
        let path = path.as_ref();
        let path = sys::Filename::new(path, self.path_bytes)?;
        Ok(database::Target::new(
            runtime,
            database::Location::File(path),
            *self,
        ))
    }

    pub fn open_memory<'sql>(
        &self,
        runtime: runtime::Runtime,
    ) -> Result<database::Target<'sql>, crate::Error> {
        match self.journal {
            Journal::Rollback => {}
            Journal::Wal(_) => return Err(crate::Error::MemoryJournal),
        }
        Ok(database::Target::new(
            runtime,
            database::Location::Memory,
            *self,
        ))
    }

    pub(crate) const fn flags(self) -> i32 {
        match self.access {
            Access::Read => libsqlite3_sys::SQLITE_OPEN_READONLY,
            Access::Write => libsqlite3_sys::SQLITE_OPEN_READWRITE,
            Access::Create => {
                libsqlite3_sys::SQLITE_OPEN_READWRITE | libsqlite3_sys::SQLITE_OPEN_CREATE
            }
        }
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

const _: () = {
    assert!(mem::size_of::<Access>() == 1);
    assert!(mem::size_of::<OpenOptions>() == 6 * mem::size_of::<usize>());
};
