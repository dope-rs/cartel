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

pub fn element<S: ColSlice>(scope: &crate::dsl::Scope, _: &S) -> S::Elem {
    match scope.0 {}
}

pub trait Batch {
    type Columns<'data>;
}

pub trait BatchColumn: crate::schema::Field {
    type Item<'data>;
}
