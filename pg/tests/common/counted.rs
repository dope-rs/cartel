use dope_fiber::{
    abi::{Fiber, Task},
    context::PollCall,
};
use std::{cell::Cell, task::Poll};

#[pin_project::pin_project]
pub struct Counted<'a, F> {
    #[pin]
    fiber: Task<F>,
    polls: &'a Cell<usize>,
}

impl<'a, F> Counted<'a, F> {
    pub fn new(fiber: F, polls: &'a Cell<usize>) -> Self {
        Self {
            fiber: Task::new(fiber),
            polls,
        }
    }
}

impl<'d, F: Fiber<'d>> Fiber<'d> for Counted<'_, F> {
    type Output = F::Output;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        let this = this.project();
        this.polls.set(this.polls.get() + 1);
        admit.poll(this.fiber).0
    }
}
