use std::ops::Deref;

use o3::alloc::{self, VecExt};
use o3::buffer::storage;
use zeroize::Zeroizing;

pub(crate) struct Secret(Zeroizing<Vec<u8>>);

impl Secret {
    fn try_copy(value: &[u8]) -> Result<Self, alloc::Error> {
        let mut bytes = Vec::try_new(value.len())?;
        bytes.extend_from_slice(value);
        Ok(Self(Zeroizing::new(bytes)))
    }
}

impl Deref for Secret {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
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
}

#[derive(Clone, Copy)]
pub(crate) enum Command<'a> {
    Authenticate(&'a Auth),
    Select(u32),
    Name(&'a [u8]),
}

impl Command<'_> {
    pub(crate) const fn step(self) -> Step {
        match self {
            Self::Authenticate(_) => Step::Authenticate,
            Self::Select(_) => Step::Select,
            Self::Name(_) => Step::SetClientName,
        }
    }
}

/// State replayed on every physical Redis connection before pool admission.
#[derive(Default)]
pub struct Plan {
    auth: Option<Auth>,
    database: Option<u32>,
    client_name: Option<storage::Shared>,
}

impl Plan {
    pub const fn new() -> Self {
        Self {
            auth: None,
            database: None,
            client_name: None,
        }
    }

    pub fn password(mut self, password: impl AsRef<[u8]>) -> Result<Self, alloc::Error> {
        self.auth = Some(Auth::Password(Secret::try_copy(password.as_ref())?));
        Ok(self)
    }

    pub fn user(
        mut self,
        username: impl AsRef<[u8]>,
        password: impl AsRef<[u8]>,
    ) -> Result<Self, Error> {
        self.auth = Some(Auth::User {
            username: storage::Shared::try_copy(username.as_ref()).map_err(Error::Username)?,
            password: Secret::try_copy(password.as_ref()).map_err(Error::Password)?,
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

    pub(crate) fn first(&self) -> Option<Command<'_>> {
        if let Some(auth) = self.auth.as_ref() {
            return Some(Command::Authenticate(auth));
        }
        if let Some(database) = self.database {
            return Some(Command::Select(database));
        }
        self.client_name().map(Command::Name)
    }

    pub(crate) fn next(&self, step: Step) -> Option<Command<'_>> {
        match step {
            Step::Authenticate => match self.database {
                Some(database) => Some(Command::Select(database)),
                None => self.client_name().map(Command::Name),
            },
            Step::Select => self.client_name().map(Command::Name),
            Step::SetClientName => None,
        }
    }
}
