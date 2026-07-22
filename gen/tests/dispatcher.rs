use std::pin::Pin;

use dope::DriverContext;
use dope::manifold::Manifold;

struct Dummy;

impl<'d> Manifold<'d> for Dummy {
    fn pre_park(self: Pin<&mut Self>, _driver: &mut DriverContext<'_, 'd>) {}
}

#[cartel_gen::dispatcher]
struct Custom {
    #[manifold]
    inner: Dummy,
}

impl Custom {
    fn new() -> Self {
        Self::__cartel_dispatcher_new(Dummy)
    }
}

#[cartel_gen::dispatcher(new)]
struct Generated<'d, M>
where
    M: Manifold<'d>,
{
    #[manifold]
    manifold: M,
    #[cfg(any())]
    omitted: u8,
}

#[cartel_gen::dispatcher(new)]
struct CollidingBrand<'d, M>
where
    M: Manifold<'d>,
{
    #[manifold]
    manifold: M,
    __cartel_dispatcher_brand: u8,
}

#[test]
fn constructor_generation_is_opt_in() {
    let _ = Custom::new();
    let _ = Generated::new(Dummy);
    let _ = CollidingBrand::new(Dummy, 7);
}
