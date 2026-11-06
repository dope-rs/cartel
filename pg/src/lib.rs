mod error;
pub use error::Error;

pub mod api;
pub mod dsl;
pub mod port;
pub mod protocol;
pub mod reply;
pub mod schema;
pub mod scram;

use o3::{alloc, base::marker::ThreadBound};
use std::{mem, ops};

pub use cartel_core::deadline::RequestPhase;
#[doc(hidden)]
pub use cartel_core::sql;
pub use cartel_gen::{PgRow, PgTable, pg_instance, query_group};
#[doc(hidden)]
pub use dope_fiber;
pub use dope_fiber::abi::Fiber;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Timestamp(pub i64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Date(pub i32);

type TextStorage = (String, ThreadBound);

fn take_text(source: &mut String) -> TextStorage {
    if source.is_empty() {
        *source = String::new();
    }
    (mem::take(source), ThreadBound::NEW)
}

#[derive(PartialEq, Eq)]
pub struct Ltree(TextStorage);

impl TryFrom<&mut String> for Ltree {
    type Error = alloc::Exhausted;

    fn try_from(s: &mut String) -> Result<Self, Self::Error> {
        Ok(Self(take_text(s)))
    }
}

impl ops::Deref for Ltree {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.0.as_str()
    }
}

impl AsRef<str> for Ltree {
    fn as_ref(&self) -> &str {
        self.0.0.as_str()
    }
}

impl PartialEq<str> for Ltree {
    fn eq(&self, other: &str) -> bool {
        self.0.0 == other
    }
}

impl PartialEq<&str> for Ltree {
    fn eq(&self, other: &&str) -> bool {
        self.0.0 == *other
    }
}

/// An owned PostgreSQL identifier used by operations that retain its name.
pub struct Identifier(TextStorage);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    Empty,
    Nul { position: usize },
    Storage(alloc::Exhausted),
}

impl TryFrom<&mut String> for Identifier {
    type Error = ParseError;

    fn try_from(value: &mut String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(ParseError::Empty);
        }
        if let Some(position) = value.as_bytes().iter().position(|byte| *byte == 0) {
            return Err(ParseError::Nul { position });
        }
        Ok(Self(take_text(value)))
    }
}

impl ops::Deref for Identifier {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.0.as_str()
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        self.0.0.as_str()
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Text(TextStorage);

impl TryFrom<&mut String> for Text {
    type Error = alloc::Exhausted;

    fn try_from(s: &mut String) -> Result<Self, Self::Error> {
        Ok(Self(take_text(s)))
    }
}

impl ops::Deref for Text {
    type Target = str;
    fn deref(&self) -> &str {
        self.0.0.as_str()
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        self.0.0.as_str()
    }
}

impl PartialEq<str> for Text {
    fn eq(&self, other: &str) -> bool {
        self.0.0 == other
    }
}

impl PartialEq<&str> for Text {
    fn eq(&self, other: &&str) -> bool {
        self.0.0 == *other
    }
}

pub struct Jsonb(TextStorage);

impl Jsonb {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.0.as_bytes()
    }
}

impl TryFrom<&mut String> for Jsonb {
    type Error = alloc::Exhausted;

    fn try_from(s: &mut String) -> Result<Self, Self::Error> {
        Ok(Self(take_text(s)))
    }
}

impl AsRef<str> for Jsonb {
    fn as_ref(&self) -> &str {
        self.0.0.as_str()
    }
}

impl ops::Deref for Jsonb {
    type Target = str;
    fn deref(&self) -> &str {
        self.0.0.as_str()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    pub const NIL: Self = Self([0u8; 16]);

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Credentials cannot be formatted as a whole.
///
pub struct Config {
    user: String,
    password: Password,
    database: String,
    search_path: Option<Identifier>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigField {
    User,
    Password,
    Database,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    Nul { field: ConfigField, position: usize },
    PasswordCapacity,
    Allocation(alloc::Error),
}

pub(crate) use scram::Password;

impl Config {
    pub fn new(user: String, password: String, database: String) -> Result<Self, ConfigError> {
        let password = Password::new(password);
        Self::without_nul(ConfigField::User, &user)?;
        Self::without_nul(ConfigField::Password, password.as_str())?;
        Self::without_nul(ConfigField::Database, &database)?;
        Ok(Self {
            user,
            password: password.try_prepare()?,
            database,
            search_path: None,
        })
    }

    pub fn search_path(mut self, schema: Identifier) -> Self {
        self.search_path = Some(schema);
        self
    }

    pub(crate) fn user(&self) -> &str {
        &self.user
    }

    pub(crate) fn password(&self) -> &Password {
        &self.password
    }

    pub(crate) fn database(&self) -> &str {
        &self.database
    }

    pub(crate) fn schema(&self) -> Option<&str> {
        self.search_path.as_deref()
    }

    fn without_nul(field: ConfigField, value: &str) -> Result<(), ConfigError> {
        match value.as_bytes().iter().position(|byte| *byte == 0) {
            Some(position) => Err(ConfigError::Nul { field, position }),
            None => Ok(()),
        }
    }
}

const _: () = assert!(mem::size_of::<Password>() == mem::size_of::<String>());
