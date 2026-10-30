use crate::api::stmt;
use std::result;

mod atoms;
mod bound;

pub use bound::{Bound, Limit, Offset};

pub trait Bind: Sized {
    const COUNT: usize;

    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error>;
}

impl Bind for () {
    const COUNT: usize = 0;

    fn bind<'value>(
        &'value self,
        _binder: &mut stmt::Binder<'_, 'value>,
        _index: i32,
    ) -> result::Result<(), crate::Error> {
        Ok(())
    }
}

impl<H: atoms::Value, T: Bind> Bind for (H, T) {
    const COUNT: usize = T::COUNT + 1;

    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        self.0.bind(binder, index)?;
        if T::COUNT == 0 {
            Ok(())
        } else {
            self.1.bind(binder, index + 1)
        }
    }
}
