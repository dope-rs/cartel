use cartel_core::reply::recv;

pub(crate) struct Pending;

impl<'s, 'b, 'c, 'd> recv::Spec<'s, 'b, 'c, 'd, ()> for Pending {
    type Context = ();
    type Mode = ();
}
