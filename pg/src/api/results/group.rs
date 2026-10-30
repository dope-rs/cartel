//! Outcomes of an explicitly synchronized group of PostgreSQL commands.

use crate::reply::server;

/// What was established for one command before the group stopped.
///
/// A completed command does not establish that its transaction committed. The
/// group's synchronization result and diagnostics must also be considered.
#[derive(Debug)]
pub enum Outcome<T, E> {
    Completed(T),
    Failed(E),
    /// No command completion was observed before this exact connection or
    /// capacity failure was published to its response order.
    Interrupted(E),
    /// The group's diagnostics contain the exact server error for this command.
    Server,
    /// ReadyForQuery proved that PostgreSQL skipped this command after an error.
    Skipped,
    /// No command completion or protocol-proven skip was observed.
    Unobserved,
}

/// Whether the group's terminating ReadyForQuery was observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Sync {
    Complete,
    Unobserved,
}

/// The protocol event to which a retained server diagnostic belongs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Position {
    Command(usize),
    Sync,
    Recovery,
    Connection,
    Unknown,
}

/// A view borrowed from the group's retained diagnostic storage.
#[derive(Debug)]
pub struct Diagnostic<'a> {
    pub position: Position,
    pub fields: server::Fields<'a>,
}

/// Retained diagnostic storage associated with one query capability.
pub trait Diagnostics {
    type Iter<'a>: Iterator<Item = Diagnostic<'a>>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_>;
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
