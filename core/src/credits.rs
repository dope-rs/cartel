pub struct FairCredits {
    capacity: usize,
    available: usize,
    protected: usize,
    held: Box<[usize]>,
    reserve: Box<[usize]>,
}

impl FairCredits {
    pub fn with_reserve(capacity: usize, lanes: usize, reserve: usize) -> Self {
        assert!(lanes > 0, "credit lane capacity must be positive");
        let protected = reserve
            .checked_mul(lanes)
            .filter(|&protected| protected <= capacity)
            .expect("credit reserve exceeds capacity");
        Self {
            capacity,
            available: capacity,
            protected,
            held: vec![0; lanes].into_boxed_slice(),
            reserve: vec![reserve; lanes].into_boxed_slice(),
        }
    }

    pub fn balanced(capacity: usize, lanes: usize) -> Self {
        assert!(lanes > 0, "credit lane capacity must be positive");
        let reserve = if lanes == 1 {
            capacity
        } else {
            capacity / lanes / 2
        };
        Self::with_reserve(capacity, lanes, reserve)
    }

    pub fn used(&self) -> usize {
        self.capacity - self.available
    }

    pub fn lanes(&self) -> usize {
        self.held.len()
    }

    pub fn held(&self, lane: usize) -> Option<usize> {
        self.held.get(lane).copied()
    }

    pub fn reserve(&self, lane: usize) -> Option<usize> {
        self.reserve.get(lane).copied()
    }

    pub fn shared_available(&self) -> usize {
        self.available.saturating_sub(self.protected)
    }

    pub fn can_acquire(&self, lane: usize, amount: usize) -> bool {
        let Some((&held, &reserve)) = self.held.get(lane).zip(self.reserve.get(lane)) else {
            return false;
        };
        if amount > self.available {
            return false;
        }
        let own = reserve.saturating_sub(held).min(amount);
        amount - own <= self.shared_available()
    }

    pub fn try_acquire(&mut self, lane: usize, amount: usize) -> bool {
        if !self.can_acquire(lane, amount) {
            return false;
        }
        self.acquire_reserved(lane, amount);
        true
    }

    pub(crate) fn acquire_reserved(&mut self, lane: usize, amount: usize) {
        assert!(
            self.can_acquire(lane, amount),
            "credit reservation must be checked before acquisition"
        );
        let held = self.held[lane];
        self.held[lane] += amount;
        self.available -= amount;
        self.protected -= self.reserve[lane]
            .saturating_sub(held)
            .saturating_sub(self.reserve[lane].saturating_sub(self.held[lane]));
    }

    pub fn release(&mut self, lane: usize, amount: usize) {
        let held = self.held[lane];
        debug_assert!(held >= amount);
        self.held[lane] -= amount;
        self.available += amount;
        self.protected += self.reserve[lane]
            .saturating_sub(self.held[lane])
            .saturating_sub(self.reserve[lane].saturating_sub(held));
    }
}
