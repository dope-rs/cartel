use cartel_sqlite::Table;
use std::ops;

type __Slot2<T> = T;

#[derive(Table)]
#[table_name("select")]
pub struct Keyword<T = i64, const N: usize = 0> {
    pub(crate) r#type: __Slot2<<Self as ops::Deref>::Target>,
}

impl<T, const N: usize> ops::Deref for Keyword<T, N> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.r#type
    }
}
