use std::mem;

#[derive(Debug)]
pub enum Error {
    /// The supplied completion still owns errors from an earlier installation.
    Unobserved,
    /// This local storage's database target has already been consumed.
    AlreadyStarted,
    Step(crate::Error),
}

impl From<crate::Error> for Error {
    fn from(error: crate::Error) -> Self {
        Self::Step(error)
    }
}

const _: () = {
    assert!(!mem::needs_drop::<Error>());
    assert!(mem::size_of::<Error>() <= 3 * mem::size_of::<usize>());
};
