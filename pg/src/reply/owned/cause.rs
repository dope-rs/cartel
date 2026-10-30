use o3::buffer::storage;

#[derive(Debug)]
pub enum Cause {
    Allocation(storage::AllocError),
    Bound,
}
