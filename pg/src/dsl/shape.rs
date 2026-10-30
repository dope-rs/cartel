use crate::dsl::query;

pub trait SourceRow {
    type Row;
}
impl<T> SourceRow for query::Filter<T> {
    type Row = T;
}
impl<T> SourceRow for query::Cte<T> {
    type Row = T;
}
impl<R> SourceRow for query::Select<R> {
    type Row = R;
}

pub trait ColSlice {
    type Elem;
}
impl<S: ColSlice + ?Sized> ColSlice for &S {
    type Elem = S::Elem;
}
impl<A> ColSlice for [A] {
    type Elem = A;
}
impl<A> ColSlice for Vec<A> {
    type Elem = A;
}
