pub struct Expr<T>(
    std::convert::Infallible,
    std::marker::PhantomData<fn(T) -> T>,
);

impl<T> Expr<T> {
    pub fn value(self) -> T {
        match self.0 {}
    }
}
