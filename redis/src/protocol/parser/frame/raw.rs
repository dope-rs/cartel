use crate::protocol::parser::frame::Selection;
use o3::buffer::bytes::scope;
use std::ops;

// SAFETY: only a completed tape paired with its exact immutable frame mints
// these selections. Their invariant scope preserves the source and coordinate
// origin; retained frames preserve the admitted ordered bounds and extent.
unsafe impl<'id> scope::raw::Select<'id> for Selection<'id> {
    fn bounds(self) -> ops::Range<usize> {
        self.span.range()
    }
}
