use crate::client::settings::{Calc, Field, Op};
use cartel_core::{memory, reply::layout};
use o3::{
    buffer::{hold::limits, pool},
    depot::capacity,
    depot::queue::pool::fair,
};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Error {
    Zero(Field),
    Below {
        field: Field,
        actual: usize,
        minimum: usize,
    },
    Above {
        field: Field,
        actual: usize,
        maximum: usize,
    },
    NotGreater {
        field: Field,
        actual: usize,
        base: usize,
    },
    Overflow {
        calc: Calc,
        op: Op,
        left: usize,
        right: usize,
    },
    Limit {
        calc: Calc,
        error: limits::Error,
    },
    RequestPool(pool::LayoutError),
    BulkPool(pool::LayoutError),
    RequestQueue(fair::Error),
    Owners(capacity::Error),
    Retention(memory::Error),
    ResponseLayout(layout::Error),
}
