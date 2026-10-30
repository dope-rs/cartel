pub mod database;
pub mod dsl;
mod engine;
mod error;
pub mod open;
#[doc(hidden)]
pub mod row;
pub mod runtime;
mod settings;

use engine::{api, catalogs, conn, cursor, retry, sys};
use settings::{config, limits, memory, options};

#[doc(hidden)]
pub use api::{Bound, IntoQuery, Limit, Offset};
pub use api::{
    batches::Rows,
    exec::Query,
    tx::{Savepoint, Transaction},
};
pub use cartel_core::deadline::RequestPhase;
#[doc(hidden)]
pub use cartel_core::sql;
pub use cartel_gen::{Table, catalog, query};
pub use catalogs::{Catalog, Shape};
#[doc(hidden)]
pub use catalogs::{Entry, HasQuery, Statement, TypedQuery};
pub use conn::Connection;
pub use conn::Installed;
pub use cursor::Cursor;
pub use dope_fiber::abi::Fiber;
pub use error::Error;
pub use limits::{Budget, Busy, Limits, Parameters, Program, Progress, Scan, Work};
pub use memory::{Cache, Lookaside, Memory, Reservation, Results, Statements, Targets};
pub use options::{Access, Checkpoint, Journal, OpenOptions, Synchronous, Wal};
pub use sys::{Action, Code};
