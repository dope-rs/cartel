pub trait Sealed {}

impl Sealed for bool {}
impl Sealed for i32 {}
impl Sealed for i64 {}
impl Sealed for String {}
impl Sealed for crate::Jsonb {}
impl<A: Sealed> Sealed for (A, ()) {}
impl<A: Sealed, B: Sealed> Sealed for (A, B) {}
