/// Drops borrowed SQLite wrappers before releasing their local resource lease.
pub(super) struct Held<T, L> {
    pub(super) value: T,
    pub(super) lease: L,
}

impl<T, L> Held<T, L> {
    pub(super) const fn new(value: T, lease: L) -> Self {
        Self { value, lease }
    }
}
