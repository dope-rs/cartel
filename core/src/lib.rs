mod credits;
pub mod error;
pub mod fatal_slot;
pub mod inflight;
pub mod queue;
pub mod reply;

pub use error::Error;
pub use fatal_slot::FatalSlot;
pub use inflight::Inflight;
pub use queue::{BoundedQueue, QueuePool, QueuePoolRef};
pub use reply::{
    Arena, ArenaFactory, ArenaPool, ArenaPoolRef, Extract, FrontKind, ItemPool, ItemPoolRef,
    LaneBudget, LaneBudgetRef, Limits, Registrable, Reply, ReplyStream, Slot,
};

#[doc(hidden)]
pub mod __private {
    pub use dope_gen::Dispatcher;
    pub use pin_project::pin_project;
}
