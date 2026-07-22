use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use dope::DriverContext;
use dope::driver::ready::CompletionWaker;
use dope::runtime::StorageFactory;
use dope_fiber::{Context, Fiber};
use o3::cell::{RegionCell, RegionToken};
use o3::collections::{CellQueue, LinkedArena, RoundRobinSet, Slab, SlabKey};
use o3::mem::FairCredits;

struct Item<C> {
    value: C,
    bytes: usize,
    credits: usize,
}

struct SlotState<C> {
    first: Option<Item<C>>,
    bytes: usize,
    credits: usize,
    completed: bool,
    overflow: bool,
}

impl<C> SlotState<C> {
    const fn new() -> Self {
        Self {
            first: None,
            bytes: 0,
            credits: 0,
            completed: false,
            overflow: false,
        }
    }

    fn reset(&mut self) {
        debug_assert!(self.first.is_none());
        debug_assert_eq!(self.bytes, 0);
        debug_assert_eq!(self.credits, 0);
        self.completed = false;
        self.overflow = false;
    }
}

/// Mutable view of one reply slot.
///
/// The view deliberately does not expose the arena permission. An extractor
/// can consume values but cannot reenter the arena while it is borrowed.
pub struct Slot<'a, C> {
    state: &'a mut SlotState<C>,
    items: &'a mut LinkedArena<Item<C>>,
    item_lane: usize,
    resources: &'a mut FairCredits<2>,
    lane: usize,
}

impl<C> Slot<'_, C> {
    pub fn completed(&self) -> bool {
        self.state.completed
    }

    pub fn overflowed(&self) -> bool {
        self.state.overflow
    }

    pub fn take_overflow(&mut self) -> bool {
        let overflow = std::mem::replace(&mut self.state.overflow, false);
        self.state.completed |= overflow;
        overflow
    }

    pub fn is_empty(&self) -> bool {
        self.state.first.is_none()
    }

    pub fn len(&self) -> usize {
        usize::from(self.state.first.is_some()) + self.items.lane_len(self.item_lane)
    }

    pub fn pop(&mut self) -> Option<C> {
        let head = self.state.first.take()?;
        if let Some(item) = self.items.pop_front(self.item_lane) {
            self.state.first = Some(item);
        }
        self.state.bytes -= head.bytes;
        self.state.credits -= head.credits;
        self.resources
            .release_all(self.lane, [head.credits, head.bytes]);
        Some(head.value)
    }
}

enum EntryTag {}

type EntryKey = SlabKey<EntryTag>;

#[derive(Clone, Copy)]
enum OrderItem {
    Slot(EntryKey),
    Boundary,
}

#[derive(Clone, Copy)]
enum Front {
    Empty,
    Boundary,
    Slot(EntryKey),
    Detached,
}

struct Entry<'d, C> {
    slot: SlotState<C>,
    lane: usize,
    live: bool,
    ordered: bool,
    waker: Option<CompletionWaker<'d>>,
}

struct Lane {
    live: usize,
    accepting: bool,
    limits: Limits,
}

impl Lane {
    fn new(limits: Limits) -> Self {
        Self {
            live: 0,
            accepting: true,
            limits,
        }
    }
}

struct ArenaState<'d, C> {
    lanes: Box<[Lane]>,
    slots: Slab<Entry<'d, C>, EntryTag>,
    items: LinkedArena<Item<C>>,
    resources: FairCredits<2>,
    order: LinkedArena<OrderItem>,
    entry_credits: FairCredits,
    order_credits: FairCredits,
    active: RoundRobinSet,
    reserved: RoundRobinSet,
}

impl<'d, C> ArenaState<'d, C> {
    fn new(config: ArenaConfig) -> Self {
        let ArenaConfig {
            lanes,
            entries,
            items,
            bytes,
            credits,
            limits,
        } = config;
        assert!(lanes > 0, "reply arena lane capacity must be positive");
        assert!(entries >= lanes, "reply entries must cover every lane");
        assert!(
            entries <= u32::MAX as usize / 2,
            "reply order capacity overflow"
        );
        let order_capacity = entries * 2;
        let entry_credits = FairCredits::with_reserve(entries, lanes, 1);
        let lane_states = (0..lanes).map(|_| Lane::new(limits)).collect();
        Self {
            lanes: lane_states,
            slots: Slab::with_capacity(entries),
            items: LinkedArena::with_capacity(items, entries),
            resources: FairCredits::from_capacities([credits, bytes], lanes),
            order: LinkedArena::with_capacity(order_capacity, lanes),
            entry_credits,
            order_credits: FairCredits::with_reserve(order_capacity, lanes, 2),
            active: RoundRobinSet::with_capacity(lanes),
            reserved: RoundRobinSet::with_capacity(lanes),
        }
    }

    fn can_reserve(&self, lane: usize, order: usize, entry: bool) -> bool {
        (!entry || (!self.slots.is_full() && self.entry_credits.can_acquire(lane, 1)))
            && self.order_credits.can_acquire(lane, order)
    }

    fn reserve_order(&mut self, lane: usize, count: usize, entry: bool) -> bool {
        if !self.order_credits.try_acquire(lane, count) {
            return false;
        }
        if entry {
            if !self.entry_credits.try_acquire(lane, 1) {
                self.order_credits.release(lane, count);
                return false;
            }
            if self.entry_credits.held_by(lane) == self.entry_credits.reserved_for(lane) {
                self.unlink_reserved(lane);
            }
        }
        true
    }

    fn append_order(&mut self, lane: usize, item: OrderItem) {
        self.order
            .push_back(lane, item)
            .unwrap_or_else(|_| unreachable!("reply order capacity was reserved"));
    }

    fn register(&mut self, lane: usize, trailing_boundary: bool) -> Option<EntryKey> {
        if !self.lanes.get(lane)?.accepting {
            return None;
        }
        let count = 1 + usize::from(trailing_boundary);
        self.reserve_order(lane, count, true).then_some(())?;
        let key = match self.slots.insert(Entry {
            slot: SlotState::new(),
            lane,
            live: true,
            ordered: true,
            waker: None,
        }) {
            Ok(key) => key,
            Err(_) => {
                self.order_credits.release(lane, count);
                self.release_entry_credit(lane);
                return None;
            }
        };
        self.lanes[lane].live += 1;
        self.append_order(lane, OrderItem::Slot(key));
        if trailing_boundary {
            self.append_order(lane, OrderItem::Boundary);
        }
        Some(key)
    }

    fn release_entry(&mut self, key: EntryKey) {
        let Some(entry) = self.slots.remove(key) else {
            return;
        };
        let lane = entry.lane;
        debug_assert!(self.items.lane_is_empty(key.index() as usize));
        self.release_entry_credit(lane);
    }

    fn release_entry_credit(&mut self, lane: usize) {
        let was_reserved =
            self.entry_credits.held_by(lane) == self.entry_credits.reserved_for(lane);
        self.entry_credits.release(lane, 1);
        if was_reserved && self.active.contains(lane) {
            self.link_reserved(lane);
        }
    }

    fn begin_retire(&mut self, retired: Retired) -> bool {
        let Some(entry) = self.slots.get_mut(retired.key) else {
            return false;
        };
        if !entry.live {
            return false;
        }
        entry.live = false;
        entry.slot.completed = false;
        entry.slot.overflow = false;
        entry.waker = None;
        self.lanes[entry.lane].live -= 1;
        true
    }

    fn pop_slot(&mut self, retired: Retired) -> Option<C> {
        let Self {
            slots,
            items,
            resources,
            ..
        } = self;
        let entry = slots.get_mut(retired.key)?;
        if entry.live {
            return None;
        }
        Slot {
            state: &mut entry.slot,
            items,
            item_lane: retired.key.index() as usize,
            resources,
            lane: entry.lane,
        }
        .pop()
    }

    fn finish_retire(&mut self, retired: Retired) {
        let Some(entry) = self.slots.get_mut(retired.key) else {
            return;
        };
        if entry.live {
            return;
        }
        entry.slot.reset();
        if !entry.ordered {
            self.release_entry(retired.key);
        }
    }

    fn front(&self, lane: usize) -> Front {
        let Some(item) = self.order.front(lane) else {
            return Front::Empty;
        };
        match *item {
            OrderItem::Boundary => Front::Boundary,
            OrderItem::Slot(key) => {
                if self.slots.get(key).is_some_and(|entry| entry.live) {
                    Front::Slot(key)
                } else {
                    Front::Detached
                }
            }
        }
    }

    fn front_kind(&self, lane: usize) -> FrontKind {
        match self.front(lane) {
            Front::Empty => FrontKind::Empty,
            Front::Boundary => FrontKind::Boundary,
            Front::Slot(key) => FrontKind::Slot(key.index()),
            Front::Detached => FrontKind::Detached,
        }
    }

    fn pop_order(&mut self, lane: usize) -> Option<OrderItem> {
        let item = self.order.pop_front(lane)?;
        self.order_credits.release(lane, 1);
        Some(item)
    }

    fn drop_front(&mut self, lane: usize) {
        let Some(item) = self.pop_order(lane) else {
            return;
        };
        if let OrderItem::Slot(key) = item
            && let Some(entry) = self.slots.get_mut(key)
        {
            entry.ordered = false;
            if !entry.live {
                self.release_entry(key);
            }
        }
    }

    fn pop_live(&mut self, lane: usize, key: EntryKey) {
        let Some(OrderItem::Slot(front)) = self.pop_order(lane) else {
            unreachable!()
        };
        debug_assert_eq!(front, key);
        let entry = self
            .slots
            .get_mut(key)
            .unwrap_or_else(|| unreachable!("live reply entry must exist"));
        entry.ordered = false;
    }

    fn can_register(&self, lane: usize, trailing_boundary: bool) -> bool {
        self.lanes.get(lane).is_some_and(|state| state.accepting)
            && self.can_reserve(lane, 1 + usize::from(trailing_boundary), true)
    }

    fn mark_boundary(&mut self, lane: usize) -> bool {
        if !self.lanes[lane].accepting {
            return false;
        }
        if !self.reserve_order(lane, 1, false) {
            return false;
        }
        self.append_order(lane, OrderItem::Boundary);
        true
    }

    fn pop_boundary(&mut self, lane: usize) {
        if matches!(self.front_kind(lane), FrontKind::Boundary) {
            self.pop_order(lane);
        }
    }

    fn try_push(
        &mut self,
        lane: usize,
        item: C,
        item_bytes: usize,
        item_credits: usize,
    ) -> (Result<(), C>, Option<CompletionWaker<'d>>) {
        let Front::Slot(key) = self.front(lane) else {
            return (Err(item), None);
        };
        let limits = self.lanes[lane].limits;
        let Self {
            slots,
            items,
            resources,
            ..
        } = self;
        let entry = slots
            .get_mut(key)
            .unwrap_or_else(|| unreachable!("front reply entry must exist"));
        debug_assert_eq!(entry.lane, lane);
        let item_lane = key.index() as usize;
        let slot = &mut entry.slot;
        let Some(next_bytes) = slot.bytes.checked_add(item_bytes) else {
            slot.overflow = true;
            return (Err(item), None);
        };
        let Some(next_credits) = slot.credits.checked_add(item_credits) else {
            slot.overflow = true;
            return (Err(item), None);
        };
        if slot.completed
            || slot.overflow
            || usize::from(slot.first.is_some()) + items.lane_len(item_lane) >= limits.item_capacity
            || next_bytes > limits.byte_capacity
            || next_credits > limits.credit_capacity
            || (slot.first.is_some() && items.is_full())
        {
            slot.overflow = true;
            return (Err(item), None);
        }
        if !resources.try_acquire_all(lane, [item_credits, item_bytes]) {
            slot.overflow = true;
            return (Err(item), None);
        }
        let item = Item {
            value: item,
            bytes: item_bytes,
            credits: item_credits,
        };
        if slot.first.is_none() {
            slot.first = Some(item);
        } else {
            items
                .push_back(item_lane, item)
                .unwrap_or_else(|_| unreachable!("reply item capacity was reserved"));
        }
        slot.bytes = next_bytes;
        slot.credits = next_credits;
        (Ok(()), entry.waker.take())
    }

    fn complete(&mut self, lane: usize) -> Option<CompletionWaker<'d>> {
        match self.front(lane) {
            Front::Slot(key) => {
                self.pop_live(lane, key);
                let entry = self
                    .slots
                    .get_mut(key)
                    .unwrap_or_else(|| unreachable!("completed reply entry must exist"));
                entry.slot.completed = true;
                entry.waker.take()
            }
            Front::Detached => {
                self.drop_front(lane);
                None
            }
            Front::Boundary | Front::Empty => None,
        }
    }

    fn fail_one(&mut self, lane: usize, item: C) -> (Option<C>, Option<CompletionWaker<'d>>) {
        let Front::Slot(key) = self.front(lane) else {
            if matches!(self.front(lane), Front::Detached) {
                self.drop_front(lane);
            }
            return (Some(item), None);
        };
        self.pop_live(lane, key);
        let (result, wake) = self.try_push_detached(key, item, 0, 1);
        let entry = self
            .slots
            .get_mut(key)
            .unwrap_or_else(|| unreachable!("failed reply entry must exist"));
        entry.slot.completed = true;
        (result.err(), wake.or_else(|| entry.waker.take()))
    }

    fn try_push_detached(
        &mut self,
        key: EntryKey,
        item: C,
        item_bytes: usize,
        item_credits: usize,
    ) -> (Result<(), C>, Option<CompletionWaker<'d>>) {
        let Self {
            lanes,
            slots,
            items,
            resources,
            ..
        } = self;
        let entry = slots
            .get_mut(key)
            .unwrap_or_else(|| unreachable!("detached reply entry must exist"));
        let lane = entry.lane;
        let limits = lanes[lane].limits;
        let item_lane = key.index() as usize;
        let slot = &mut entry.slot;
        if usize::from(slot.first.is_some()) + items.lane_len(item_lane) >= limits.item_capacity
            || item_bytes > limits.byte_capacity.saturating_sub(slot.bytes)
            || item_credits > limits.credit_capacity.saturating_sub(slot.credits)
            || (slot.first.is_some() && items.is_full())
        {
            slot.overflow = true;
            return (Err(item), None);
        }
        if !resources.try_acquire_all(lane, [item_credits, item_bytes]) {
            slot.overflow = true;
            return (Err(item), None);
        }
        let item = Item {
            value: item,
            bytes: item_bytes,
            credits: item_credits,
        };
        if slot.first.is_none() {
            slot.first = Some(item);
        } else {
            items
                .push_back(item_lane, item)
                .unwrap_or_else(|_| unreachable!("reply item capacity was reserved"));
        }
        slot.bytes += item_bytes;
        slot.credits += item_credits;
        (Ok(()), entry.waker.take())
    }

    fn activate(&mut self, lane: usize) {
        if !self.active.insert(lane) {
            return;
        }
        if self
            .entry_credits
            .held_by(lane)
            .zip(self.entry_credits.reserved_for(lane))
            .is_some_and(|(held, reserved)| held < reserved)
        {
            self.link_reserved(lane);
        }
    }

    fn deactivate(&mut self, lane: usize) {
        if !self.active.remove(lane) {
            return;
        }
        self.unlink_reserved(lane);
    }

    fn pick_active(&mut self) -> Option<usize> {
        if self.entry_credits.shared_available() != 0 {
            self.active.next_index()
        } else {
            self.reserved.next_index()
        }
    }

    fn link_reserved(&mut self, lane: usize) {
        self.reserved.insert(lane);
    }

    fn unlink_reserved(&mut self, lane: usize) {
        self.reserved.remove(lane);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    item_capacity: usize,
    byte_capacity: usize,
    credit_capacity: usize,
}

impl Limits {
    pub const fn new(item_capacity: usize, byte_capacity: usize, credit_capacity: usize) -> Self {
        Self {
            item_capacity,
            byte_capacity,
            credit_capacity,
        }
    }

    pub const fn item_capacity(self) -> usize {
        self.item_capacity
    }

    pub const fn byte_capacity(self) -> usize {
        self.byte_capacity
    }

    pub const fn credit_capacity(self) -> usize {
        self.credit_capacity
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArenaConfig {
    lanes: usize,
    entries: usize,
    items: usize,
    bytes: usize,
    credits: usize,
    limits: Limits,
}

impl ArenaConfig {
    pub const fn new(
        lanes: usize,
        entries: usize,
        items: usize,
        bytes: usize,
        credits: usize,
        limits: Limits,
    ) -> Self {
        Self {
            lanes,
            entries,
            items,
            bytes,
            credits,
            limits,
        }
    }

    pub const fn single(capacity: usize, limits: Limits) -> Self {
        Self::new(
            1,
            capacity,
            limits.item_capacity,
            limits.byte_capacity,
            limits.credit_capacity,
            limits,
        )
    }
}

#[derive(Clone, Copy)]
struct Retired {
    key: EntryKey,
}

/// A fixed-capacity, multi-lane reply arena protected by a runtime region.
pub struct Arena<'d, C> {
    state: RegionCell<'d, ArenaState<'d, C>>,
    retired: CellQueue<Retired>,
    lanes: usize,
}

pub struct ArenaFactory<C> {
    config: ArenaConfig,
    item: PhantomData<fn() -> C>,
}

pub struct ArenaLane<'d, C> {
    arena: &'d Arena<'d, C>,
    lane: usize,
}

impl<C> Copy for ArenaLane<'_, C> {}

impl<C> Clone for ArenaLane<'_, C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, C> Arena<'d, C> {
    pub fn factory(capacity: usize, limits: Limits) -> ArenaFactory<C> {
        ArenaFactory {
            config: ArenaConfig::single(capacity, limits),
            item: PhantomData,
        }
    }

    pub fn with_limits(capacity: usize, limits: Limits) -> Self {
        Self::new(ArenaConfig::single(capacity, limits))
    }

    pub fn new(config: ArenaConfig) -> Self {
        Self {
            state: RegionCell::new(ArenaState::new(config)),
            retired: CellQueue::with_capacity(config.entries),
            lanes: config.lanes,
        }
    }

    pub fn lane(&'d self, lane: usize) -> ArenaLane<'d, C> {
        assert!(lane < self.lanes, "reply arena lane out of range");
        ArenaLane { arena: self, lane }
    }

    fn drain_retired(&self, token: &mut RegionToken<'d>) {
        while let Some(retired) = self.retired.pop_front() {
            if !self.state.borrow_mut(token).begin_retire(retired) {
                continue;
            }
            while let Some(item) = self.state.borrow_mut(token).pop_slot(retired) {
                drop(item);
            }
            self.state.borrow_mut(token).finish_retire(retired);
        }
    }

    fn defer_retire(&self, retired: Retired) {
        assert!(
            self.retired.push_back(retired).is_ok(),
            "reply retire queue capacity invariant violated"
        );
    }

    pub fn len(&self, token: &mut RegionToken<'d>, lane: usize) -> usize {
        self.drain_retired(token);
        self.state.borrow(token).order.lane_len(lane)
    }

    pub fn is_empty(&self, token: &mut RegionToken<'d>, lane: usize) -> bool {
        self.drain_retired(token);
        self.state.borrow(token).lanes[lane].live == 0
    }

    pub fn can_register(&self, token: &mut RegionToken<'d>, lane: usize) -> bool {
        self.drain_retired(token);
        self.state.borrow(token).can_register(lane, false)
    }

    pub fn can_mark_boundary(&self, token: &mut RegionToken<'d>, lane: usize) -> bool {
        self.drain_retired(token);
        let state = self.state.borrow(token);
        state.lanes[lane].accepting && state.can_reserve(lane, 1, false)
    }

    pub fn front_kind(&self, token: &mut RegionToken<'d>, lane: usize) -> FrontKind {
        self.drain_retired(token);
        self.state.borrow(token).front_kind(lane)
    }

    pub fn mark_boundary(&self, token: &mut RegionToken<'d>, lane: usize) -> bool {
        self.drain_retired(token);
        self.state.borrow_mut(token).mark_boundary(lane)
    }

    pub fn pop_boundary(&self, token: &mut RegionToken<'d>, lane: usize) {
        self.drain_retired(token);
        self.state.borrow_mut(token).pop_boundary(lane);
    }

    pub fn try_push(
        &self,
        token: &mut RegionToken<'d>,
        lane: usize,
        item: C,
        item_bytes: usize,
        item_credits: usize,
    ) -> bool {
        self.drain_retired(token);
        let (pushed, waker) =
            self.state
                .borrow_mut(token)
                .try_push(lane, item, item_bytes, item_credits);
        if let Some(waker) = waker {
            waker.wake();
        }
        pushed.is_ok()
    }

    pub fn complete(&self, token: &mut RegionToken<'d>, lane: usize) {
        self.drain_retired(token);
        let waker = self.state.borrow_mut(token).complete(lane);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub fn fail_one(&self, token: &mut RegionToken<'d>, lane: usize, make: impl FnOnce() -> C) {
        self.drain_retired(token);
        let (rejected, waker) = self.state.borrow_mut(token).fail_one(lane, make());
        if let Some(waker) = waker {
            waker.wake();
        }
        drop(rejected);
    }

    pub fn fail_all(
        &self,
        token: &mut RegionToken<'d>,
        lane: usize,
        mut make: impl FnMut() -> C,
    ) -> usize {
        self.drain_retired(token);
        let mut failed = 0;
        loop {
            match self.state.borrow(token).front_kind(lane) {
                FrontKind::Empty => return failed,
                FrontKind::Boundary => {
                    self.state.borrow_mut(token).pop_order(lane);
                }
                FrontKind::Detached => {
                    self.state.borrow_mut(token).drop_front(lane);
                    failed += 1;
                }
                FrontKind::Slot(_) => {
                    let (rejected, waker) = self.state.borrow_mut(token).fail_one(lane, make());
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                    drop(rejected);
                    failed += 1;
                }
            }
        }
    }

    pub fn activate(&self, token: &mut RegionToken<'d>, lane: usize) {
        self.drain_retired(token);
        self.state.borrow_mut(token).activate(lane);
    }

    pub fn deactivate(&self, token: &mut RegionToken<'d>, lane: usize) {
        self.drain_retired(token);
        self.state.borrow_mut(token).deactivate(lane);
    }

    pub fn pick_active(&self, token: &mut RegionToken<'d>) -> Option<usize> {
        self.drain_retired(token);
        self.state.borrow_mut(token).pick_active()
    }

    pub fn inflight(&self, token: &mut RegionToken<'d>) -> usize {
        self.drain_retired(token);
        self.state.borrow(token).entry_credits.used()
    }

    fn register(
        &self,
        token: &mut RegionToken<'d>,
        lane: usize,
        trailing_boundary: bool,
    ) -> Option<EntryKey> {
        self.drain_retired(token);
        self.state
            .borrow_mut(token)
            .register(lane, trailing_boundary)
    }

    fn with_slot<R>(
        &self,
        token: &mut RegionToken<'d>,
        key: EntryKey,
        f: impl FnOnce(&mut Slot<'_, C>, &mut Option<CompletionWaker<'d>>) -> R,
    ) -> Option<R> {
        self.drain_retired(token);
        let state = self.state.borrow_mut(token);
        let ArenaState {
            slots,
            items,
            resources,
            ..
        } = state;
        let entry = slots.get_mut(key)?;
        if !entry.live {
            return None;
        }
        let mut slot = Slot {
            state: &mut entry.slot,
            items,
            item_lane: key.index() as usize,
            resources,
            lane: entry.lane,
        };
        Some(f(&mut slot, &mut entry.waker))
    }
}

impl<'d, C> ArenaLane<'d, C> {
    pub const fn index(self) -> usize {
        self.lane
    }

    pub fn len(self, token: &mut RegionToken<'d>) -> usize {
        self.arena.len(token, self.lane)
    }

    pub fn is_empty(self, token: &mut RegionToken<'d>) -> bool {
        self.arena.is_empty(token, self.lane)
    }

    pub fn can_register(self, token: &mut RegionToken<'d>) -> bool {
        self.arena.can_register(token, self.lane)
    }

    pub fn can_mark_boundary(self, token: &mut RegionToken<'d>) -> bool {
        self.arena.can_mark_boundary(token, self.lane)
    }

    pub fn front_kind(self, token: &mut RegionToken<'d>) -> FrontKind {
        self.arena.front_kind(token, self.lane)
    }

    pub fn mark_boundary(self, token: &mut RegionToken<'d>) -> bool {
        self.arena.mark_boundary(token, self.lane)
    }

    pub fn pop_boundary(self, token: &mut RegionToken<'d>) {
        self.arena.pop_boundary(token, self.lane);
    }

    pub fn try_push(
        self,
        token: &mut RegionToken<'d>,
        item: C,
        item_bytes: usize,
        item_credits: usize,
    ) -> bool {
        self.arena
            .try_push(token, self.lane, item, item_bytes, item_credits)
    }

    pub fn complete(self, token: &mut RegionToken<'d>) {
        self.arena.complete(token, self.lane);
    }

    pub fn fail_one(self, token: &mut RegionToken<'d>, make: impl FnOnce() -> C) {
        self.arena.fail_one(token, self.lane, make);
    }

    pub fn fail_all(self, token: &mut RegionToken<'d>, make: impl FnMut() -> C) -> usize {
        self.arena.fail_all(token, self.lane, make)
    }
}

impl<C: 'static> StorageFactory for ArenaFactory<C> {
    type Output<'d> = Arena<'d, C>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        Arena::new(self.config)
    }
}

/// Extracts one logical value from a reply slot.
pub trait Extract<C> {
    type Output;
    const SYNC_AFTER: bool = false;
    fn extract(slot: &mut Slot<'_, C>) -> Option<Self::Output>;
}

struct Registration<'d, C> {
    lane: ArenaLane<'d, C>,
    key: EntryKey,
}

struct Handle<'d, C> {
    registration: Option<Registration<'d, C>>,
}

impl<'d, C> Handle<'d, C> {
    const fn new() -> Self {
        Self { registration: None }
    }

    fn try_register(
        &mut self,
        token: &mut RegionToken<'d>,
        lane: ArenaLane<'d, C>,
        trailing_boundary: bool,
    ) -> bool {
        if self.registration.is_some() {
            return false;
        }
        let Some(key) = lane.arena.register(token, lane.lane, trailing_boundary) else {
            return false;
        };
        self.registration = Some(Registration { lane, key });
        true
    }

    fn with_slot<R>(
        &self,
        token: &mut RegionToken<'d>,
        f: impl FnOnce(&mut Slot<'_, C>, &mut Option<CompletionWaker<'d>>) -> R,
    ) -> Option<R> {
        let registration = self.registration.as_ref()?;
        registration
            .lane
            .arena
            .with_slot(token, registration.key, f)
    }

    fn release_done(&mut self) {
        let Some(registration) = self.registration.take() else {
            return;
        };
        registration.lane.arena.defer_retire(Retired {
            key: registration.key,
        });
    }
}

impl<C> Drop for Handle<'_, C> {
    fn drop(&mut self) {
        self.release_done();
    }
}

pub struct Reply<'d, C, X: Extract<C>> {
    handle: Handle<'d, C>,
    extract: PhantomData<fn() -> X>,
}

impl<'d, C, X: Extract<C>> Reply<'d, C, X> {
    pub const fn new() -> Self {
        Self {
            handle: Handle::new(),
            extract: PhantomData,
        }
    }
}

impl<C, X: Extract<C>> Default for Reply<'_, C, X> {
    fn default() -> Self {
        Self::new()
    }
}

mod sealed {
    use o3::cell::RegionToken;

    use super::ArenaLane;

    pub trait Registrable<'d, C> {
        fn attach(
            &mut self,
            token: &mut RegionToken<'d>,
            lane: ArenaLane<'d, C>,
            trailing_boundary: bool,
        ) -> bool;
    }
}

pub trait Registrable<'d, C>: sealed::Registrable<'d, C> {
    fn try_attach(&mut self, token: &mut RegionToken<'d>, lane: ArenaLane<'d, C>) -> bool {
        sealed::Registrable::attach(self, token, lane, false)
    }

    fn try_attach_with_boundary(
        &mut self,
        token: &mut RegionToken<'d>,
        lane: ArenaLane<'d, C>,
        trailing_boundary: bool,
    ) -> bool {
        sealed::Registrable::attach(self, token, lane, trailing_boundary)
    }
}

impl<'d, C, T> Registrable<'d, C> for T where T: sealed::Registrable<'d, C> {}

impl<'d, C, X: Extract<C>> sealed::Registrable<'d, C> for Reply<'d, C, X> {
    fn attach(
        &mut self,
        token: &mut RegionToken<'d>,
        lane: ArenaLane<'d, C>,
        trailing_boundary: bool,
    ) -> bool {
        self.handle.try_register(token, lane, trailing_boundary)
    }
}

impl<'d, C, X: Extract<C>> Fiber<'d> for Reply<'d, C, X> {
    type Output = X::Output;

    fn poll(mut self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<X::Output> {
        let wake = cx.as_ref().completion_waker();
        let token = cx.as_mut().region_token();
        let me = self.as_mut().get_mut();
        let Some(poll) = me.handle.with_slot(token, |slot, waker| {
            if let Some(output) = X::extract(slot) {
                return Poll::Ready(output);
            }
            *waker = Some(wake);
            Poll::Pending
        }) else {
            return Poll::Pending;
        };
        if poll.is_ready() {
            me.handle.release_done();
        }
        poll
    }
}

pub struct ReplyStream<'d, C, X: Extract<C>> {
    handle: Handle<'d, C>,
    extract: PhantomData<fn() -> X>,
}

impl<'d, C, X: Extract<C>> ReplyStream<'d, C, X> {
    pub const fn new() -> Self {
        Self {
            handle: Handle::new(),
            extract: PhantomData,
        }
    }

    pub fn poll_next(
        mut self: Pin<&mut Self>,
        mut cx: Pin<&mut Context<'_, 'd>>,
    ) -> Poll<Option<X::Output>> {
        let wake = cx.as_ref().completion_waker();
        let token = cx.as_mut().region_token();
        let me = self.as_mut().get_mut();
        let Some(poll) = me.handle.with_slot(token, |slot, waker| {
            if let Some(output) = X::extract(slot) {
                return Poll::Ready(Some(output));
            }
            if slot.completed() && slot.is_empty() {
                return Poll::Ready(None);
            }
            *waker = Some(wake);
            Poll::Pending
        }) else {
            return Poll::Ready(None);
        };
        if matches!(poll, Poll::Ready(None)) {
            me.handle.release_done();
        }
        poll
    }
}

impl<C, X: Extract<C>> Default for ReplyStream<'_, C, X> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, C, X: Extract<C>> sealed::Registrable<'d, C> for ReplyStream<'d, C, X> {
    fn attach(
        &mut self,
        token: &mut RegionToken<'d>,
        lane: ArenaLane<'d, C>,
        trailing_boundary: bool,
    ) -> bool {
        self.handle.try_register(token, lane, trailing_boundary)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontKind {
    Empty,
    Boundary,
    Slot(u32),
    Detached,
}
