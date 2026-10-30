use crate::reply::owned;

pub enum Error {
    Driver(crate::Error),
    Server(owned::Server),
}

impl Error {
    pub fn as_driver(&self) -> Option<&crate::Error> {
        match self {
            Self::Driver(error) => Some(error),
            Self::Server(_) => None,
        }
    }

    pub fn as_server(&self) -> Option<&owned::Server> {
        match self {
            Self::Server(error) => Some(error),
            Self::Driver(_) => None,
        }
    }
}
