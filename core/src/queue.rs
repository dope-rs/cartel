use o3::cell::{RegionCell, RegionToken};
use o3::collections::LinkedArena;
use o3::mem::FairCredits;

struct State<T> {
    items: LinkedArena<(T, usize)>,
    credits: FairCredits,
    weights: Box<[usize]>,
}

impl<T> State<T> {
    fn with_capacity(capacity: usize, lanes: usize) -> Self {
        assert!(lanes > 0, "queue arena lane capacity must be positive");
        assert!(capacity >= lanes, "queue capacity must cover every lane");
        Self {
            items: LinkedArena::with_capacity(capacity, lanes),
            credits: FairCredits::with_reserve(capacity, lanes, 1),
            weights: vec![0; lanes].into_boxed_slice(),
        }
    }

    fn has_capacity(&self, lane: usize) -> bool {
        self.weights.get(lane).is_some()
            && !self.items.is_full()
            && self.credits.can_acquire(lane, 1)
    }

    fn try_push(&mut self, lane: usize, item: T, weight: usize) -> Result<(), T> {
        if self.weights.get(lane).is_none() || !self.credits.try_acquire(lane, 1) {
            return Err(item);
        }
        match self.items.push_back(lane, (item, weight)) {
            Ok(()) => {
                self.weights[lane] = self.weights[lane].saturating_add(weight);
                Ok(())
            }
            Err((item, _)) => {
                self.credits.release(lane, 1);
                Err(item)
            }
        }
    }

    fn pop_front(&mut self, lane: usize) -> Option<(T, usize)> {
        self.weights.get(lane)?;
        let (item, weight) = self.items.pop_front(lane)?;
        self.weights[lane] = self.weights[lane].saturating_sub(weight);
        self.credits.release(lane, 1);
        Some((item, weight))
    }

    fn restore_front(&mut self, lane: usize, item: T, weight: usize) {
        assert!(
            self.credits.try_acquire(lane, 1),
            "popped queue credit must remain available"
        );
        self.items
            .push_front(lane, (item, weight))
            .unwrap_or_else(|_| unreachable!("popped queue node must remain available"));
        self.weights[lane] = self.weights[lane].saturating_add(weight);
    }
}

/// Region-owned fixed storage shared fairly by a fixed number of FIFO lanes.
pub struct QueueArena<'d, T> {
    state: RegionCell<'d, State<T>>,
    lanes: usize,
}

/// A zero-cost typed view of one [`QueueArena`] lane.
pub struct QueueLane<'a, 'd, T> {
    arena: &'a QueueArena<'d, T>,
    lane: usize,
}

impl<T> Copy for QueueLane<'_, '_, T> {}

impl<T> Clone for QueueLane<'_, '_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, T: Unpin> QueueArena<'d, T> {
    pub fn with_capacity(capacity: usize, lanes: usize) -> Self {
        Self {
            state: RegionCell::new(State::with_capacity(capacity, lanes)),
            lanes,
        }
    }

    pub fn lane(&self, lane: usize) -> QueueLane<'_, 'd, T> {
        assert!(lane < self.lanes, "queue arena lane out of range");
        QueueLane { arena: self, lane }
    }
}

impl<'d, T: Unpin> QueueLane<'_, 'd, T> {
    pub const fn index(self) -> usize {
        self.lane
    }

    pub fn len(self, token: &RegionToken<'d>) -> usize {
        self.arena.state.borrow(token).items.lane_len(self.lane)
    }

    pub fn is_empty(self, token: &RegionToken<'d>) -> bool {
        self.len(token) == 0
    }

    pub fn weight(self, token: &RegionToken<'d>) -> usize {
        self.arena.state.borrow(token).weights[self.lane]
    }

    pub fn has_capacity(self, token: &RegionToken<'d>) -> bool {
        self.arena.state.borrow(token).has_capacity(self.lane)
    }

    pub fn try_push(self, token: &mut RegionToken<'d>, item: T, weight: usize) -> Result<(), T> {
        self.arena
            .state
            .borrow_mut(token)
            .try_push(self.lane, item, weight)
    }

    pub fn push_reserved(self, token: &mut RegionToken<'d>, item: T, weight: usize) {
        assert!(
            self.try_push(token, item, weight).is_ok(),
            "queue reservation must be checked before insertion"
        );
    }

    pub fn pop_front(self, token: &mut RegionToken<'d>) -> Option<T> {
        self.arena
            .state
            .borrow_mut(token)
            .pop_front(self.lane)
            .map(|(item, _)| item)
    }

    pub fn drain(self, token: &mut RegionToken<'d>, mut push: impl FnMut(T) -> Result<(), T>) {
        while let Some((item, weight)) = self.arena.state.borrow_mut(token).pop_front(self.lane) {
            if let Err(item) = push(item) {
                self.arena
                    .state
                    .borrow_mut(token)
                    .restore_front(self.lane, item, weight);
                break;
            }
        }
    }

    pub fn clear(self, token: &mut RegionToken<'d>) {
        while let Some(item) = self.pop_front(token) {
            drop(item);
        }
    }
}
