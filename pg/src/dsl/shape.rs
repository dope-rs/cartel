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
