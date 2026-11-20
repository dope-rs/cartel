pub struct Stream<T>(
    std::convert::Infallible,
    std::marker::PhantomData<fn(T) -> T>,
);
