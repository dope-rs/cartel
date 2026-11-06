use cartel_pg::Config;
use dope::core::Errno;
use std::{env, net::AddrParseError, net::SocketAddr, num::ParseIntError};

pub const NAMES: [&str; 6] = [
    "CARTEL_PG_TEST",
    "PG_HOST",
    "PG_PORT",
    "PG_USER",
    "PG_PASSWORD",
    "PG_DATABASE",
];

#[derive(Debug)]
pub enum Error {
    Disabled,
    Entropy(Errno),
    Variable(env::VarError),
    Port(ParseIntError),
    Address(AddrParseError),
    Config(cartel_pg::ConfigError),
}

pub struct Env {
    address: SocketAddr,
    user: String,
    password: zeroize::Zeroizing<String>,
    database: String,
}

impl Env {
    pub fn try_load(values: [Option<std::ffi::OsString>; 6]) -> Result<Self, Error> {
        let [enabled, host, port, user, password, database] = values;
        if enabled.is_none() {
            return Err(Error::Disabled);
        }
        let host = Self::read(host, "127.0.0.1")?;
        let port = Self::read(port, "5432")?
            .parse::<u16>()
            .map_err(Error::Port)?;
        let address = format!("{host}:{port}").parse().map_err(Error::Address)?;
        Ok(Self {
            address,
            user: Self::read(user, "bench")?,
            password: zeroize::Zeroizing::new(Self::read(password, "bench")?),
            database: Self::read(database, "bench")?,
        })
    }
    pub const fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn try_config(&self) -> Result<Config, Error> {
        Config::new(
            self.user.clone(),
            self.password.as_str().to_owned(),
            self.database.clone(),
        )
        .map_err(Error::Config)
    }
    fn read(value: Option<std::ffi::OsString>, default: &str) -> Result<String, Error> {
        match value {
            Some(value) => value
                .into_string()
                .map_err(env::VarError::NotUnicode)
                .map_err(Error::Variable),
            None => Ok(default.to_owned()),
        }
    }
}
