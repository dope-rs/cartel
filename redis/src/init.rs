use o3::{
    buffer::{hold, resident},
    mem::rc,
};

use crate::client::settings::init::Step;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Rejected,
    Unexpected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Retention {
    Retain(hold::Error),
    Share(rc::Error),
}

/// The initialization step and its rejected or unexpected response.
#[derive(Debug)]
pub struct Failure<B> {
    pub step: Step,
    pub kind: Kind,
    /// The configured policy selects sharing or copying; failures keep the exact cause.
    pub frame: Result<B, Retention>,
}

impl<B: AsRef<[u8]>> Failure<B> {
    pub fn as_ref(&self) -> Failure<&[u8]> {
        Failure {
            step: self.step,
            kind: self.kind,
            frame: match &self.frame {
                Ok(frame) => Ok(frame.as_ref()),
                Err(error) => Err(*error),
            },
        }
    }
}

impl<'d> Failure<resident::Shared<'d>> {
    pub(crate) fn observe(&self) -> Self {
        Self {
            step: self.step,
            kind: self.kind,
            frame: match &self.frame {
                Ok(frame) => frame.try_clone().map_err(Retention::Share),
                Err(error) => Err(*error),
            },
        }
    }
}
