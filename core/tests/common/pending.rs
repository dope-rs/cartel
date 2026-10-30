use cartel_core::{reply, reply::recv};
use std::convert::Infallible;

pub(crate) struct Pending;

impl<'s, 'b, 'c, 'd> recv::Extract<'s, 'b, 'c, 'd, u8> for Pending {
    type Output = Infallible;

    type Context = ();

    fn extract(
        _: &Self::Context,
        _: &mut reply::Slot<'_, 's, 'b, 'c, 'd, u8>,
    ) -> Option<Infallible> {
        None
    }
}
