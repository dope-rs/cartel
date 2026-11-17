use o3::alloc::{self, BoxSliceExt};
use o3::buffer::storage;
use std::convert::Infallible;
use zeroize::Zeroizing;

use crate::client::ops::scripts::Invocation;

type Secret = Zeroizing<Box<[u8]>>;

fn try_secret(value: &[u8]) -> Result<Secret, alloc::Error> {
    Ok(Zeroizing::new(Box::try_copy(value)?))
}

pub(crate) enum Auth {
    Password(Secret),
    User {
        username: storage::Shared,
        password: Secret,
    },
}

#[derive(Debug)]
pub enum Error {
    Username(storage::AllocError),
    Password(alloc::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Step {
    Authenticate,
    Select,
    SetClientName,
    Eval,
}

pub(crate) enum Command<'a, I> {
    Authenticate(&'a Auth),
    Select(u32),
    Name(&'a [u8]),
    Eval(&'a I),
}

impl<I> Command<'_, I> {
    pub(crate) const fn step(&self) -> Step {
        match self {
            Self::Authenticate(_) => Step::Authenticate,
            Self::Select(_) => Step::Select,
            Self::Name(_) => Step::SetClientName,
            Self::Eval(_) => Step::Eval,
        }
    }
}

/// State replayed on every physical Redis connection before pool admission.
pub struct Plan<I: Unpin = Infallible, const EXPECTED: i64 = 0, const COUNT: usize = 0> {
    auth: Option<Auth>,
    database: Option<u32>,
    client_name: Option<storage::Shared>,
    eval: [I; COUNT],
}

impl Default for Plan {
    fn default() -> Self {
        Self::new()
    }
}

impl Plan {
    pub const fn new() -> Self {
        Self {
            auth: None,
            database: None,
            client_name: None,
            eval: [],
        }
    }

    /// Requires this EVAL integer reply before each physical connection is admitted.
    pub fn eval<const EXPECTED: i64, I: Invocation + Unpin>(
        self,
        invocation: I,
    ) -> Plan<I, EXPECTED, 1> {
        Plan {
            auth: self.auth,
            database: self.database,
            client_name: self.client_name,
            eval: [invocation],
        }
    }
}

impl<I: Unpin, const EXPECTED: i64, const COUNT: usize> Plan<I, EXPECTED, COUNT> {
    pub fn password(mut self, password: impl AsRef<[u8]>) -> Result<Self, alloc::Error> {
        self.auth = Some(Auth::Password(try_secret(password.as_ref())?));
        Ok(self)
    }

    pub fn user(
        mut self,
        username: impl AsRef<[u8]>,
        password: impl AsRef<[u8]>,
    ) -> Result<Self, Error> {
        self.auth = Some(Auth::User {
            username: storage::Shared::try_copy(username.as_ref()).map_err(Error::Username)?,
            password: try_secret(password.as_ref()).map_err(Error::Password)?,
        });
        Ok(self)
    }

    pub const fn select(mut self, database: u32) -> Self {
        self.database = Some(database);
        self
    }

    pub fn name(mut self, name: impl AsRef<[u8]>) -> Result<Self, storage::AllocError> {
        self.client_name = Some(storage::Shared::try_copy(name.as_ref())?);
        Ok(self)
    }

    pub const fn database(&self) -> Option<u32> {
        self.database
    }

    pub fn client_name(&self) -> Option<&[u8]> {
        self.client_name.as_ref().map(AsRef::as_ref)
    }

    pub(crate) fn first(&self) -> Option<Command<'_, I>> {
        self.auth
            .as_ref()
            .map(Command::Authenticate)
            .or_else(|| self.next(Step::Authenticate))
    }

    fn after_select(&self) -> Option<Command<'_, I>> {
        self.client_name()
            .map(Command::Name)
            .or_else(|| self.eval.first().map(Command::Eval))
    }

    pub(crate) fn next(&self, step: Step) -> Option<Command<'_, I>> {
        match step {
            Step::Authenticate => match self.database {
                Some(database) => Some(Command::Select(database)),
                None => self.after_select(),
            },
            Step::Select => self.after_select(),
            Step::SetClientName => self.eval.first().map(Command::Eval),
            Step::Eval => None,
        }
    }
}
