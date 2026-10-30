use crate::dsl::query;

pub trait Source {
    type Row;
}

impl<T> Source for query::Filter<T> {
    type Row = T;
}
