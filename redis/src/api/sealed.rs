use crate::client::{self, conn};

pub trait Sealed {}

impl<B: ?Sized, const ID: u8> Sealed for client::Redis<'_, '_, '_, '_, '_, B, ID> {}
impl<B: ?Sized, const ID: u8> Sealed for conn::Connection<'_, '_, '_, '_, '_, B, ID> {}
