use std::pin;

use dope_fiber::abi::{Fiber, PollFn, Task};

pub fn release_after_poll<'a, 'd: 'a, F, H>(
    mut fiber: pin::Pin<Box<Task<F>>>,
    held: H,
) -> impl Fiber<'d, Output = F::Output> + 'a
where
    F: Fiber<'d> + 'a,
    H: 'a,
{
    let mut held = Some(held);
    PollFn::admitted(move |admit| {
        let (outcome, _) = admit.poll(fiber.as_mut());
        if outcome.is_pending() && held.is_some() {
            drop(held.take());
        }
        outcome
    })
}
