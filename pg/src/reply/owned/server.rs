use o3::buffer::storage;
use std::mem;

pub struct Server {
    pub(in crate::reply) payload: storage::Shared,
}

impl Server {
    pub(in crate::reply) const fn new(payload: storage::Shared) -> Self {
        Self { payload }
    }
}

const _: () = assert!(mem::size_of::<Server>() == mem::size_of::<storage::Shared>());
