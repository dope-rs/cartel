use std::{mem, pin, task};

use dope::core::driver::schedule::clock::timer;
use dope_fiber::context;
use o3::base::time::Positive;

/// The bounded operation phase in which an absolute request deadline elapsed.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum RequestPhase {
    Admission,
    Response,
    Settlement,
    Lease,
    Contention,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Elapsed {
    phase: RequestPhase,
}

impl Elapsed {
    pub const fn new(phase: RequestPhase) -> Self {
        Self { phase }
    }

    pub const fn phase(self) -> RequestPhase {
        self.phase
    }
}

/// Copyable authority to create deadlines tied to one exact driver lifetime.
#[derive(Clone, Copy)]
pub struct Source<'d> {
    timer: timer::Timer<'d>,
    window: Positive,
}

impl<'d> Source<'d> {
    pub const fn new(timer: timer::Timer<'d>, window: Positive) -> Self {
        Self { timer, window }
    }

    #[inline]
    pub fn guard(self) -> Result<Guard<'d>, timer::Exhausted> {
        Ok(Guard {
            state: GuardState::Live {
                alarm: self.timer.try_alarm()?,
                window: Some(self.window),
            },
        })
    }

    pub const fn into_parts(self) -> (timer::Timer<'d>, Positive) {
        (self.timer, self.window)
    }
}

/// One first-poll-anchored absolute deadline. Phase changes never rearm it.
pub struct Guard<'d> {
    state: GuardState<'d>,
}

enum GuardState<'d> {
    Live {
        alarm: timer::Alarm<'d>,
        window: Option<Positive>,
    },
    Elapsed,
    Complete,
}

impl<'d> Guard<'d> {
    pub fn poll_expired(
        self: pin::Pin<&mut Self>,
        mut ctx: pin::Pin<&mut context::Context<'_, 'd>>,
        phase: RequestPhase,
    ) -> task::Poll<Elapsed> {
        let state = &mut self.get_mut().state;
        let (alarm, window) = match state {
            GuardState::Live { alarm, window } => (alarm, window),
            GuardState::Elapsed => return task::Poll::Ready(Elapsed::new(phase)),
            GuardState::Complete => return task::Poll::Pending,
        };
        let wake = ctx.as_ref().completion();
        let now = ctx.as_mut().driver().driver_ref().clock().now();
        let expired = match window.take() {
            Some(duration) => alarm.arm_and_poll(now.clamp_add(duration), now, wake),
            None => alarm.poll(now, wake),
        };
        if expired.is_ready() {
            *state = GuardState::Elapsed;
            task::Poll::Ready(Elapsed::new(phase))
        } else {
            task::Poll::Pending
        }
    }

    pub fn complete(&mut self) {
        self.state = GuardState::Complete;
    }
}

const _: () = {
    const LIVE: usize = mem::size_of::<timer::Alarm<'static>>() + mem::size_of::<Positive>();

    assert!(mem::size_of::<Guard<'static>>() == LIVE);
    assert!(mem::size_of::<Option<Guard<'static>>>() == LIVE);
    assert!(mem::size_of::<Result<Guard<'static>, timer::Exhausted>>() == LIVE);
};
