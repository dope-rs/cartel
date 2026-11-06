use crate::{config, sys};
use dope_core::driver::{
    schedule::clock::{self, timer},
    schedule::{self, ready::wake},
};
use std::{mem, task};

#[repr(transparent)]
pub(crate) struct Slot<'d>(Mode<'d>);

enum Mode<'d> {
    /// The configuration retained by the loan admitted all three delays as yields.
    Yield(State<'d>),
    Timer(Timer<'d>),
}

struct Timer<'d> {
    alarm: timer::Alarm<'d>,
    phase: Phase<'d>,
}

#[derive(Clone, Copy)]
enum State<'d> {
    Idle,
    Ready(Budget<'d>),
    Yield(Budget<'d>),
    Yielded(Budget<'d>),
}

#[derive(Clone, Copy)]
enum Phase<'d> {
    Run(State<'d>),
    New(Budget<'d>, clock::Deadline<'d>),
    Sleep(Budget<'d>, clock::Deadline<'d>),
}

#[derive(Clone, Copy)]
struct Budget<'d> {
    deadline: clock::Deadline<'d>,
    fault: sys::Fault,
    remaining: u32,
}

enum Quota {
    Available,
    Exhausted,
}

enum Attempt<'d> {
    Ready(Budget<'d>),
    Pending,
}

pub(crate) enum Wait {
    Ready,
    Pending,
}

pub(crate) enum Park {
    Ready,
    Delayed,
}

impl<'d> Slot<'d> {
    pub(crate) const fn yielding() -> Self {
        Self(Mode::Yield(State::Idle))
    }

    pub(crate) const fn timed(alarm: timer::Alarm<'d>) -> Self {
        Self(Mode::Timer(Timer {
            alarm,
            phase: Phase::Run(State::Idle),
        }))
    }

    pub(super) fn defer(
        &mut self,
        limits: config::Contention,
        now: clock::Moment<'d>,
        fault: sys::Fault,
    ) -> Result<(), crate::Error> {
        match &mut self.0 {
            Mode::Yield(state) => state.defer(limits, now, fault).map(|attempt| {
                if let Attempt::Ready(budget) = attempt {
                    *state = State::Yield(budget);
                }
            }),
            Mode::Timer(timer) => timer.defer(limits, now, fault),
        }
    }

    pub(super) const fn wait(&self) -> Wait {
        match &self.0 {
            Mode::Yield(state) => state.wait(),
            Mode::Timer(timer) => timer.wait(),
        }
    }

    pub(super) fn poll(
        &mut self,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), crate::Error>> {
        match &mut self.0 {
            Mode::Yield(state) => state.poll(now, wake),
            Mode::Timer(timer) => timer.poll(now, wake),
        }
    }

    pub(crate) fn park(&self, now: clock::Moment<'d>) -> Park {
        match &self.0 {
            Mode::Yield(_) => Park::Ready,
            Mode::Timer(timer) => timer.park(now),
        }
    }

    pub(super) fn poll_park(
        &mut self,
        now: clock::Moment<'d>,
    ) -> task::Poll<Result<(), crate::Error>> {
        match &mut self.0 {
            Mode::Yield(state) => state.poll_park(now),
            Mode::Timer(timer) => timer.poll_park(now),
        }
    }

    pub(crate) fn progress(&self) -> schedule::Progress<'d> {
        match &self.0 {
            Mode::Yield(_) => schedule::Progress::Runnable,
            Mode::Timer(timer) => timer.progress(),
        }
    }

    pub(crate) fn finish(&mut self) {
        match &mut self.0 {
            Mode::Yield(state) => state.finish(),
            Mode::Timer(timer) => timer.finish(),
        }
    }

    pub(super) fn detach(&mut self) {
        match &mut self.0 {
            Mode::Yield(state) => state.detach(),
            Mode::Timer(timer) => timer.detach(),
        }
    }
}

impl<'d> State<'d> {
    fn defer(
        &mut self,
        limits: config::Contention,
        now: clock::Moment<'d>,
        fault: sys::Fault,
    ) -> Result<Attempt<'d>, crate::Error> {
        let state = mem::replace(self, Self::Idle);
        let mut budget = match state {
            Self::Idle => Budget::new(limits, now, fault),
            Self::Ready(mut budget) => {
                budget.fault = fault;
                budget
            }
            state @ (Self::Yield(_) | Self::Yielded(_)) => {
                *self = state;
                return Ok(Attempt::Pending);
            }
        };
        if matches!(budget.take(), Quota::Exhausted) {
            return Err(fault.busy());
        }
        if budget.deadline.is_due(now) {
            return Err(fault.timeout());
        }
        Ok(Attempt::Ready(budget))
    }

    const fn wait(&self) -> Wait {
        match self {
            Self::Idle | Self::Ready(_) => Wait::Ready,
            Self::Yield(_) | Self::Yielded(_) => Wait::Pending,
        }
    }

    fn poll(
        &mut self,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), crate::Error>> {
        let budget = match *self {
            Self::Idle | Self::Ready(_) => return task::Poll::Ready(Ok(())),
            Self::Yield(budget) | Self::Yielded(budget) => budget,
        };
        if budget.deadline.is_due(now) {
            *self = Self::Idle;
            return task::Poll::Ready(Err(budget.fault.timeout()));
        }
        match *self {
            Self::Yield(_) => {
                *self = Self::Yielded(budget);
                wake.wake();
                task::Poll::Pending
            }
            Self::Yielded(_) => {
                *self = Self::Ready(budget);
                task::Poll::Ready(Ok(()))
            }
            Self::Idle | Self::Ready(_) => task::Poll::Ready(Ok(())),
        }
    }

    fn poll_park(&mut self, now: clock::Moment<'d>) -> task::Poll<Result<(), crate::Error>> {
        let budget = match *self {
            Self::Idle => return task::Poll::Ready(Ok(())),
            Self::Ready(budget) | Self::Yield(budget) | Self::Yielded(budget) => budget,
        };
        if budget.deadline.is_due(now) {
            *self = Self::Idle;
            return task::Poll::Ready(Err(budget.fault.timeout()));
        }
        *self = Self::Ready(budget);
        task::Poll::Ready(Ok(()))
    }

    fn finish(&mut self) {
        *self = Self::Idle;
    }

    fn detach(&mut self) {
        if let Self::Yielded(budget) = *self {
            *self = Self::Yield(budget);
        }
    }
}

impl<'d> Timer<'d> {
    fn defer(
        &mut self,
        limits: config::Contention,
        now: clock::Moment<'d>,
        fault: sys::Fault,
    ) -> Result<(), crate::Error> {
        let Phase::Run(state) = &mut self.phase else {
            return Ok(());
        };
        let Attempt::Ready(budget) = state.defer(limits, now, fault)? else {
            return Ok(());
        };
        self.phase = match limits.delay() {
            config::Delay::Yield => Phase::Run(State::Yield(budget)),
            config::Delay::Sleep(interval) => {
                let wake_at = now.clamp_add(interval).min(budget.deadline);
                Phase::New(budget, wake_at)
            }
        };
        Ok(())
    }

    const fn wait(&self) -> Wait {
        match self.phase {
            Phase::Run(state) => state.wait(),
            Phase::New(_, _) | Phase::Sleep(_, _) => Wait::Pending,
        }
    }

    fn poll(
        &mut self,
        now: clock::Moment<'d>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), crate::Error>> {
        let (budget, wake_at) = match &mut self.phase {
            Phase::Run(state) => return state.poll(now, wake),
            Phase::New(budget, wake_at) | Phase::Sleep(budget, wake_at) => (*budget, *wake_at),
        };
        if budget.deadline.is_due(now) {
            self.finish();
            return task::Poll::Ready(Err(budget.fault.timeout()));
        }
        match self.phase {
            Phase::New(_, _) => {
                self.alarm.arm(wake_at, wake);
                self.phase = Phase::Sleep(budget, wake_at);
                task::Poll::Pending
            }
            Phase::Sleep(_, _) if self.alarm.poll(now, wake).is_pending() => task::Poll::Pending,
            Phase::Sleep(_, _) => {
                self.phase = Phase::Run(State::Ready(budget));
                task::Poll::Ready(Ok(()))
            }
            Phase::Run(_) => task::Poll::Ready(Ok(())),
        }
    }

    fn park(&self, now: clock::Moment<'d>) -> Park {
        match self.phase {
            Phase::Run(_) => Park::Ready,
            Phase::New(_, wake_at) | Phase::Sleep(_, wake_at) if wake_at.is_due(now) => Park::Ready,
            Phase::New(_, _) | Phase::Sleep(_, _) => Park::Delayed,
        }
    }

    fn poll_park(&mut self, now: clock::Moment<'d>) -> task::Poll<Result<(), crate::Error>> {
        let (budget, wake_at) = match &mut self.phase {
            Phase::Run(state) => return state.poll_park(now),
            Phase::New(budget, wake_at) | Phase::Sleep(budget, wake_at) => (*budget, *wake_at),
        };
        if budget.deadline.is_due(now) {
            self.finish();
            return task::Poll::Ready(Err(budget.fault.timeout()));
        }
        if !wake_at.is_due(now) {
            return task::Poll::Pending;
        }
        if matches!(self.phase, Phase::Sleep(_, _)) {
            self.alarm.clear();
        }
        self.phase = Phase::Run(State::Ready(budget));
        task::Poll::Ready(Ok(()))
    }

    fn progress(&self) -> schedule::Progress<'d> {
        match self.phase {
            Phase::Run(_) => schedule::Progress::Runnable,
            Phase::New(_, wake_at) | Phase::Sleep(_, wake_at) => schedule::Progress::until(wake_at),
        }
    }

    fn finish(&mut self) {
        self.alarm.clear();
        self.phase = Phase::Run(State::Idle);
    }

    fn detach(&mut self) {
        match self.phase {
            Phase::Run(ref mut state) => state.detach(),
            Phase::Sleep(budget, wake_at) => {
                self.alarm.clear();
                self.phase = Phase::New(budget, wake_at);
            }
            Phase::New(_, _) => {}
        }
    }
}

impl<'d> Budget<'d> {
    fn new(limits: config::Contention, now: clock::Moment<'d>, fault: sys::Fault) -> Self {
        Self {
            deadline: now.clamp_add(limits.window),
            fault,
            remaining: limits.retries.get(),
        }
    }

    fn take(&mut self) -> Quota {
        let Some(left) = self.remaining.checked_sub(1) else {
            return Quota::Exhausted;
        };
        self.remaining = left;
        Quota::Available
    }
}

const _: () = {
    assert!(
        mem::size_of::<Option<timer::Alarm<'static>>>() == mem::size_of::<timer::Alarm<'static>>()
    );
    assert!(
        mem::size_of::<Phase<'static>>()
            <= 2 * mem::size_of::<clock::Deadline<'static>>()
                + mem::size_of::<u32>()
                + mem::size_of::<sys::Fault>()
                + 2 * mem::align_of::<clock::Deadline<'static>>()
    );
    assert!(
        mem::size_of::<Slot<'static>>()
            == mem::size_of::<timer::Alarm<'static>>() + mem::size_of::<Phase<'static>>()
    );
    assert!(mem::align_of::<Slot<'static>>() == mem::align_of::<timer::Alarm<'static>>());
};
