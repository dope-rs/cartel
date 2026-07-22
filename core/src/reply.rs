use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::task::Poll;

use dope::DriverContext;
use dope::runtime::StorageFactory;
use dope_fiber::{Context, Fiber, Waker};
use o3::mem::{ByteBudget, ByteBudgetHandle, ByteLease};

use crate::credits::FairCredits;

pub struct LaneBudget {
    credits: Box<UnsafeCell<FairCredits>>,
}

#[derive(Clone, Copy)]
pub struct LaneBudgetRef<'d> {
    credits: NonNull<FairCredits>,
    lane: usize,
    lifetime: PhantomData<&'d LaneBudget>,
}

impl LaneBudget {
    pub fn with_capacity(capacity: usize, lanes: usize) -> Self {
        Self {
            credits: Box::new(UnsafeCell::new(FairCredits::balanced(capacity, lanes))),
        }
    }

    pub fn handle(&self, lane: usize) -> LaneBudgetRef<'_> {
        assert!(lane < unsafe { &*self.credits.get() }.lanes());
        LaneBudgetRef {
            credits: NonNull::from(unsafe { &mut *self.credits.get() }),
            lane,
            lifetime: PhantomData,
        }
    }
}

impl LaneBudgetRef<'_> {
    /// # Safety
    /// The budget must outlive `'a`.
    pub unsafe fn assume_lifetime<'a>(self) -> LaneBudgetRef<'a> {
        LaneBudgetRef {
            credits: self.credits,
            lane: self.lane,
            lifetime: PhantomData,
        }
    }

    fn try_acquire(self, amount: usize) -> bool {
        unsafe { &mut *self.credits.as_ptr() }.try_acquire(self.lane, amount)
    }

    fn release(self, amount: usize) {
        unsafe { &mut *self.credits.as_ptr() }.release(self.lane, amount);
    }
}

#[derive(Clone, Copy)]
enum BudgetRef {
    Shared(ByteBudgetHandle<'static>),
    Lane(LaneBudgetRef<'static>),
}

impl BudgetRef {
    fn shared(owner: &Pin<Rc<ByteBudget>>) -> Self {
        let budget = owner.as_ref().handle();
        Self::Shared(unsafe {
            std::mem::transmute::<ByteBudgetHandle<'_>, ByteBudgetHandle<'static>>(budget)
        })
    }

    fn slot(self) -> SlotBudget {
        match self {
            Self::Shared(budget) => {
                let Some(lease) = budget.try_acquire(0) else {
                    unreachable!()
                };
                SlotBudget::Shared(lease)
            }
            Self::Lane(budget) => SlotBudget::Lane { budget, amount: 0 },
        }
    }
}

enum SlotBudget {
    Shared(ByteLease<'static>),
    Lane {
        budget: LaneBudgetRef<'static>,
        amount: usize,
    },
}

impl SlotBudget {
    fn amount(&self) -> usize {
        match self {
            Self::Shared(lease) => lease.amount(),
            Self::Lane { amount, .. } => *amount,
        }
    }

    fn try_grow(&mut self, additional: usize) -> bool {
        if additional == 0 {
            return true;
        }
        match self {
            Self::Shared(lease) => lease.try_grow(additional),
            Self::Lane { budget, amount } => {
                if !budget.try_acquire(additional) {
                    return false;
                }
                *amount += additional;
                true
            }
        }
    }

    fn shrink(&mut self, amount: usize) {
        if amount == 0 {
            return;
        }
        match self {
            Self::Shared(lease) => lease.shrink(amount),
            Self::Lane {
                budget,
                amount: held,
            } => {
                assert!(amount <= *held, "lane budget underflow");
                *held -= amount;
                budget.release(amount);
            }
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Shared(lease) => lease.shrink(lease.amount()),
            Self::Lane { budget, amount } => {
                budget.release(*amount);
                *amount = 0;
            }
        }
    }
}

impl Drop for SlotBudget {
    fn drop(&mut self) {
        self.clear();
    }
}

pub struct Slot<'d, C> {
    first: Option<(C, usize, usize)>,
    rest_head: u32,
    rest_tail: u32,
    rest_len: usize,
    credits: usize,
    completed: bool,
    overflow: bool,
    waker: Option<Waker<'d>>,
    budget: SlotBudget,
    nodes: RawItemPool<C>,
}

impl<'d, C> Slot<'d, C> {
    fn new(budget: BudgetRef, nodes: RawItemPool<C>) -> Self {
        Self {
            first: None,
            rest_head: NONE,
            rest_tail: NONE,
            rest_len: 0,
            credits: 0,
            completed: false,
            overflow: false,
            waker: None,
            budget: budget.slot(),
            nodes,
        }
    }

    fn reset(&mut self) {
        self.completed = false;
        self.overflow = false;
        self.waker = None;
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    pub fn take_overflow(&mut self) -> bool {
        let overflow = std::mem::replace(&mut self.overflow, false);
        self.completed |= overflow;
        overflow
    }

    pub fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    pub fn len(&self) -> usize {
        self.first.is_some() as usize + self.rest_len
    }

    pub fn pop(&mut self) -> Option<C> {
        let (head, bytes, credits) = self.first.take()?;
        if self.rest_head != NONE {
            let (next, item) = self.nodes.take(self.rest_head);
            self.rest_head = next;
            self.rest_len -= 1;
            if next == NONE {
                self.rest_tail = NONE;
            }
            self.first = Some(item);
        }
        self.credits = self.credits.saturating_sub(credits);
        self.budget.shrink(bytes);
        self.nodes.release(credits);
        Some(head)
    }

    fn try_push(
        &mut self,
        item: C,
        max_items: usize,
        item_bytes: usize,
        max_bytes: usize,
        item_credits: usize,
        max_credits: usize,
    ) -> Result<(), C> {
        let next_bytes = self.budget.amount().checked_add(item_bytes);
        let next_credits = self.credits.checked_add(item_credits);
        if self.completed
            || self.overflow
            || self.len() >= max_items
            || next_bytes.is_none_or(|bytes| bytes > max_bytes)
            || next_credits.is_none_or(|credits| credits > max_credits)
            || !self.nodes.can_acquire(self.first.is_some(), item_credits)
        {
            self.overflow = true;
            return Err(item);
        }
        self.nodes.acquire_reserved(item_credits);
        if !self.budget.try_grow(item_bytes) {
            self.nodes.release(item_credits);
            self.overflow = true;
            return Err(item);
        }
        if self.first.is_none() {
            self.first = Some((item, item_bytes, item_credits));
        } else {
            let index = self.nodes.insert_reserved((item, item_bytes, item_credits));
            if self.rest_tail == NONE {
                self.rest_head = index;
            } else {
                self.nodes.set_next(self.rest_tail, index);
            }
            self.rest_tail = index;
            self.rest_len += 1;
        }
        self.credits = next_credits.expect("reply credits checked before reservation");
        Ok(())
    }

    fn detach(&mut self) {
        let first = self.first.take();
        let mut rest = std::mem::replace(&mut self.rest_head, NONE);
        self.rest_tail = NONE;
        self.rest_len = 0;
        let credits = std::mem::take(&mut self.credits);
        self.budget.clear();
        drop(first);
        while rest != NONE {
            let (next, item) = self.nodes.take(rest);
            rest = next;
            drop(item);
        }
        self.nodes.release(credits);
    }
}

impl<'d, C> Drop for Slot<'d, C> {
    fn drop(&mut self) {
        self.detach();
    }
}

const NONE: u32 = u32::MAX;

struct Node<T> {
    item: Option<T>,
    next: u32,
}

pub(crate) struct NodePool<T> {
    nodes: Box<[Node<T>]>,
    free: u32,
    available: usize,
}

impl<T> NodePool<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            u32::try_from(capacity).is_ok(),
            "node pool capacity overflow"
        );
        Self {
            nodes: (0..capacity)
                .map(|index| Node {
                    item: None,
                    next: if index + 1 == capacity {
                        NONE
                    } else {
                        (index + 1) as u32
                    },
                })
                .collect(),
            free: if capacity == 0 { NONE } else { 0 },
            available: capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len() - self.available
    }

    pub fn capacity(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_full(&self) -> bool {
        self.available == 0
    }

    pub(crate) fn insert_reserved(&mut self, item: T) -> u32 {
        assert!(!self.is_full(), "node reservation must precede insertion");
        let index = self.free;
        let node = self
            .nodes
            .get_mut(index as usize)
            .expect("free-list index must point into the node pool");
        assert!(node.item.is_none(), "free-list node must be vacant");
        self.free = node.next;
        self.available -= 1;
        node.item = Some(item);
        node.next = NONE;
        index
    }

    pub fn set_next(&mut self, index: u32, next: u32) {
        self.nodes[index as usize].next = next;
    }

    pub fn get(&self, index: u32) -> Option<&T> {
        self.nodes.get(index as usize)?.item.as_ref()
    }

    pub fn take(&mut self, index: u32) -> (u32, T) {
        let node = self
            .nodes
            .get_mut(index as usize)
            .expect("occupied node index must point into the node pool");
        let next = node.next;
        node.next = NONE;
        let item = node
            .item
            .take()
            .expect("occupied node must contain an item");
        (next, item)
    }

    pub fn restore(&mut self, index: u32, next: u32, item: T) {
        let node = &mut self.nodes[index as usize];
        debug_assert!(node.item.is_none());
        node.next = next;
        node.item = Some(item);
    }

    pub fn release(&mut self, index: u32) {
        let node = &mut self.nodes[index as usize];
        debug_assert!(node.item.is_none());
        node.next = self.free;
        self.free = index;
        self.available += 1;
    }

    fn remove(&mut self, index: u32) -> (u32, T) {
        let removed = self.take(index);
        self.release(index);
        removed
    }
}

struct ItemPoolState<C> {
    nodes: NodePool<(C, usize, usize)>,
    rows: FairCredits,
}

pub struct ItemPool<C> {
    state: Box<UnsafeCell<ItemPoolState<C>>>,
}

pub struct ItemPoolRef<'d, C> {
    state: NonNull<ItemPoolState<C>>,
    lane: usize,
    lifetime: PhantomData<&'d ItemPool<C>>,
}

struct RawItemPool<C> {
    state: NonNull<ItemPoolState<C>>,
    lane: usize,
}

impl<C> Clone for RawItemPool<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C> Copy for RawItemPool<C> {}

impl<C> Clone for ItemPoolRef<'_, C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C> Copy for ItemPoolRef<'_, C> {}

impl<C> ItemPool<C> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_credit_capacity(capacity, capacity, 1)
    }

    pub fn with_lanes(capacity: usize, lanes: usize) -> Self {
        Self::with_credit_capacity(capacity, capacity, lanes)
    }

    pub fn with_credit_capacity(
        item_capacity: usize,
        credit_capacity: usize,
        lanes: usize,
    ) -> Self {
        Self {
            state: Box::new(UnsafeCell::new(ItemPoolState {
                nodes: NodePool::with_capacity(item_capacity),
                rows: FairCredits::balanced(credit_capacity, lanes),
            })),
        }
    }

    pub fn handle(&self) -> ItemPoolRef<'_, C> {
        self.handle_for(0)
    }

    pub fn handle_for(&self, lane: usize) -> ItemPoolRef<'_, C> {
        assert!(lane < unsafe { &*self.state.get() }.rows.lanes());
        ItemPoolRef {
            state: NonNull::from(unsafe { &mut *self.state.get() }),
            lane,
            lifetime: PhantomData,
        }
    }
}

impl<C> ItemPoolRef<'_, C> {
    /// # Safety
    /// The pool must outlive `'a`.
    pub unsafe fn assume_lifetime<'a>(self) -> ItemPoolRef<'a, C> {
        ItemPoolRef {
            state: self.state,
            lane: self.lane,
            lifetime: PhantomData,
        }
    }

    fn raw(self) -> RawItemPool<C> {
        RawItemPool {
            state: self.state,
            lane: self.lane,
        }
    }
}

impl<C> RawItemPool<C> {
    fn can_acquire(self, needs_node: bool, credits: usize) -> bool {
        let state = unsafe { &mut *self.state.as_ptr() };
        (!needs_node || !state.nodes.is_full()) && state.rows.can_acquire(self.lane, credits)
    }

    fn acquire_reserved(self, credits: usize) {
        let state = unsafe { &mut *self.state.as_ptr() };
        state.rows.acquire_reserved(self.lane, credits);
    }

    fn release(self, rows: usize) {
        unsafe { &mut *self.state.as_ptr() }
            .rows
            .release(self.lane, rows);
    }

    fn insert_reserved(self, item: (C, usize, usize)) -> u32 {
        let state = unsafe { &mut *self.state.as_ptr() };
        state.nodes.insert_reserved(item)
    }

    fn set_next(self, index: u32, next: u32) {
        unsafe { &mut *self.state.as_ptr() }
            .nodes
            .set_next(index, next);
    }

    fn take(self, index: u32) -> (u32, (C, usize, usize)) {
        unsafe { &mut *self.state.as_ptr() }.nodes.remove(index)
    }
}

#[derive(Clone, Copy)]
enum OrderItem {
    Slot { idx: u32, epoch: u32 },
    Boundary,
}

struct ArenaPoolState {
    order: NodePool<OrderItem>,
    entries: FairCredits,
    order_credits: FairCredits,
    active: Box<[bool]>,
    active_next: Box<[u32]>,
    active_prev: Box<[u32]>,
    active_head: u32,
    reserved: Box<[bool]>,
    reserved_next: Box<[u32]>,
    reserved_prev: Box<[u32]>,
    reserved_head: u32,
}

impl ArenaPoolState {
    fn with_capacity(capacity: usize, lanes: usize) -> Self {
        assert!(capacity > 0, "reply metadata capacity must be positive");
        assert!(lanes > 0, "reply metadata lanes must be positive");
        assert!(
            capacity >= lanes,
            "reply metadata capacity must cover all lanes"
        );
        assert!(
            u32::try_from(capacity).is_ok(),
            "reply metadata capacity overflow"
        );
        let order_capacity = capacity
            .checked_mul(2)
            .expect("reply order capacity overflow");
        assert!(
            u32::try_from(order_capacity).is_ok(),
            "reply order capacity overflow"
        );
        Self {
            order: NodePool::with_capacity(order_capacity),
            entries: FairCredits::with_reserve(capacity, lanes, 1),
            order_credits: FairCredits::with_reserve(order_capacity, lanes, 2),
            active: vec![false; lanes].into_boxed_slice(),
            active_next: vec![NONE; lanes].into_boxed_slice(),
            active_prev: vec![NONE; lanes].into_boxed_slice(),
            active_head: NONE,
            reserved: vec![false; lanes].into_boxed_slice(),
            reserved_next: vec![NONE; lanes].into_boxed_slice(),
            reserved_prev: vec![NONE; lanes].into_boxed_slice(),
            reserved_head: NONE,
        }
    }

    fn can_reserve(&self, lane: usize, order: usize, entry: bool) -> bool {
        (!entry || self.entries.can_acquire(lane, 1))
            && self.order_credits.can_acquire(lane, order)
            && self.order.capacity() - self.order.len() >= order
    }

    fn reserve(&mut self, lane: usize, items: &[OrderItem], entry: bool) -> Option<[u32; 2]> {
        if !self.can_reserve(lane, items.len(), entry) {
            return None;
        }
        self.order_credits.acquire_reserved(lane, items.len());
        if entry {
            self.entries.acquire_reserved(lane, 1);
            if self.entries.held(lane) == self.entries.reserve(lane) {
                self.unlink_reserved(lane);
            }
        }
        let mut reserved = [NONE; 2];
        for (slot, item) in reserved.iter_mut().zip(items.iter().copied()) {
            *slot = self.order.insert_reserved(item);
        }
        Some(reserved)
    }

    fn set_next(&mut self, index: u32, next: u32) {
        self.order.set_next(index, next);
    }

    fn item(&self, index: u32) -> OrderItem {
        *self.order.get(index).expect("reply order node empty")
    }

    fn take(&mut self, lane: usize, index: u32) -> (u32, OrderItem) {
        let item = self.order.remove(index);
        self.order_credits.release(lane, 1);
        item
    }

    fn release_entry(&mut self, lane: usize) {
        let was_reserved = self.entries.held(lane) == self.entries.reserve(lane);
        self.entries.release(lane, 1);
        if was_reserved && self.active[lane] {
            self.link_reserved(lane);
        }
    }

    fn activate(&mut self, lane: usize) {
        if self.active[lane] {
            return;
        }
        self.active[lane] = true;
        Self::link(
            lane,
            &mut self.active_head,
            &mut self.active_next,
            &mut self.active_prev,
        );
        if self.entries.held(lane) < self.entries.reserve(lane) {
            self.link_reserved(lane);
        }
    }

    fn deactivate(&mut self, lane: usize) {
        if !self.active[lane] {
            return;
        }
        self.active[lane] = false;
        Self::unlink(
            lane,
            &mut self.active_head,
            &mut self.active_next,
            &mut self.active_prev,
        );
        self.unlink_reserved(lane);
    }

    fn pick_active(&mut self) -> Option<usize> {
        let shared = self.entries.shared_available() != 0;
        let head = if shared {
            &mut self.active_head
        } else {
            &mut self.reserved_head
        };
        if *head == NONE {
            return None;
        }
        let lane = *head as usize;
        *head = if shared {
            self.active_next[lane]
        } else {
            self.reserved_next[lane]
        };
        Some(lane)
    }

    fn link_reserved(&mut self, lane: usize) {
        if self.reserved[lane] {
            return;
        }
        self.reserved[lane] = true;
        Self::link(
            lane,
            &mut self.reserved_head,
            &mut self.reserved_next,
            &mut self.reserved_prev,
        );
    }

    fn unlink_reserved(&mut self, lane: usize) {
        if !self.reserved[lane] {
            return;
        }
        self.reserved[lane] = false;
        Self::unlink(
            lane,
            &mut self.reserved_head,
            &mut self.reserved_next,
            &mut self.reserved_prev,
        );
    }

    fn link(lane: usize, head: &mut u32, next: &mut [u32], prev: &mut [u32]) {
        if *head == NONE {
            *head = lane as u32;
            next[lane] = lane as u32;
            prev[lane] = lane as u32;
            return;
        }
        let first = *head as usize;
        let last = prev[first] as usize;
        next[lane] = first as u32;
        prev[lane] = last as u32;
        next[last] = lane as u32;
        prev[first] = lane as u32;
    }

    fn unlink(lane: usize, head: &mut u32, next: &mut [u32], prev: &mut [u32]) {
        let following = next[lane];
        let preceding = prev[lane];
        if following == NONE {
            return;
        }
        if following == lane as u32 {
            *head = NONE;
        } else {
            next[preceding as usize] = following;
            prev[following as usize] = preceding;
            if *head == lane as u32 {
                *head = following;
            }
        }
        next[lane] = NONE;
        prev[lane] = NONE;
    }
}

pub struct ArenaPool {
    state: Box<UnsafeCell<ArenaPoolState>>,
}

pub struct ArenaPoolRef<'d> {
    state: NonNull<ArenaPoolState>,
    lifetime: PhantomData<&'d ArenaPool>,
}

impl Clone for ArenaPoolRef<'_> {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for ArenaPoolRef<'_> {}

impl ArenaPool {
    pub fn with_capacity(capacity: usize, lanes: usize) -> Self {
        Self {
            state: Box::new(UnsafeCell::new(ArenaPoolState::with_capacity(
                capacity, lanes,
            ))),
        }
    }

    pub fn handle(&self) -> ArenaPoolRef<'_> {
        ArenaPoolRef {
            state: NonNull::from(unsafe { &mut *self.state.get() }),
            lifetime: PhantomData,
        }
    }

    pub fn activate(&self, lane: usize) {
        unsafe { &mut *self.state.get() }.activate(lane);
    }

    pub fn deactivate(&self, lane: usize) {
        unsafe { &mut *self.state.get() }.deactivate(lane);
    }

    pub fn pick_active(&self) -> Option<usize> {
        unsafe { &mut *self.state.get() }.pick_active()
    }

    pub fn len(&self) -> usize {
        unsafe { &*self.state.get() }.entries.used()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ArenaPoolRef<'_> {
    /// # Safety
    /// The pool must outlive `'a`.
    pub unsafe fn assume_lifetime<'a>(self) -> ArenaPoolRef<'a> {
        ArenaPoolRef {
            state: self.state,
            lifetime: PhantomData,
        }
    }

    fn get(self) -> &'static mut ArenaPoolState {
        unsafe { &mut *self.state.as_ptr() }
    }
}

pub enum FrontKind {
    Slot(u32),
    Detached,
    Boundary,
    Empty,
}

struct Entry<'d, C> {
    slot: Slot<'d, C>,
    epoch: u32,
    held: bool,
    live: bool,
    ordered: bool,
    retiring: bool,
}

struct ArenaState<'d, C> {
    entries: Vec<Entry<'d, C>>,
    nodes: RawItemPool<C>,
    free: Vec<u32>,
    order_head: u32,
    order_tail: u32,
    order_len: usize,
    live: usize,
    lane: usize,
    metadata: ArenaPoolRef<'d>,
    accepting: Cell<bool>,
    budget: BudgetRef,
    limits: Limits,
}

impl<'d, C> ArenaState<'d, C> {
    fn with_pool(
        metadata: ArenaPoolRef<'d>,
        lane: usize,
        budget: BudgetRef,
        nodes: RawItemPool<C>,
        limits: Limits,
    ) -> Self {
        let reserve = metadata
            .get()
            .entries
            .reserve(lane)
            .expect("reply metadata lane out of range");
        Self {
            entries: Vec::with_capacity(reserve),
            nodes,
            free: Vec::with_capacity(reserve),
            order_head: NONE,
            order_tail: NONE,
            order_len: 0,
            live: 0,
            lane,
            metadata,
            accepting: Cell::new(true),
            budget,
            limits,
        }
    }

    fn len(&self) -> usize {
        self.order_len
    }

    fn is_empty(&self) -> bool {
        self.live == 0
    }

    fn can_mark_boundary(&self) -> bool {
        self.accepting.get() && self.metadata.get().can_reserve(self.lane, 1, false)
    }

    fn can_register(&self, trailing_boundary: bool) -> bool {
        self.accepting.get()
            && self
                .metadata
                .get()
                .can_reserve(self.lane, 1 + trailing_boundary as usize, true)
    }

    fn append_order(&mut self, index: u32) {
        if self.order_tail == NONE {
            self.order_head = index;
        } else {
            self.metadata.get().set_next(self.order_tail, index);
        }
        self.order_tail = index;
        self.order_len += 1;
    }

    fn register(&mut self, trailing_boundary: bool) -> Option<(u32, u32)> {
        let (idx, epoch) = if let Some(&idx) = self.free.last() {
            (idx, self.entries[idx as usize].epoch.wrapping_add(1))
        } else {
            (self.entries.len() as u32, 1)
        };
        let items = [OrderItem::Slot { idx, epoch }, OrderItem::Boundary];
        let count = 1 + trailing_boundary as usize;
        let reserved = self
            .metadata
            .get()
            .reserve(self.lane, &items[..count], true)?;
        if self.free.last() == Some(&idx) {
            self.free.pop();
            let entry = &mut self.entries[idx as usize];
            entry.slot.reset();
            entry.epoch = epoch;
            entry.held = true;
            entry.live = true;
            entry.ordered = true;
            entry.retiring = false;
        } else {
            self.entries.push(Entry {
                slot: Slot::new(self.budget, self.nodes),
                epoch,
                held: true,
                live: true,
                ordered: true,
                retiring: false,
            });
        }
        self.live += 1;
        self.append_order(reserved[0]);
        if trailing_boundary {
            self.append_order(reserved[1]);
        }
        Some((idx, epoch))
    }

    fn release_entry(&mut self, idx: u32) {
        let entry = &mut self.entries[idx as usize];
        if !entry.held {
            return;
        }
        entry.held = false;
        self.free.push(idx);
        self.metadata.get().release_entry(self.lane);
    }

    fn begin_retire(&mut self, idx: u32, epoch: u32) -> bool {
        let Some(e) = self.entries.get_mut(idx as usize) else {
            return false;
        };
        if e.epoch != epoch || !e.live {
            return false;
        }
        e.live = false;
        e.retiring = true;
        e.slot.completed = false;
        e.slot.overflow = false;
        e.slot.waker = None;
        self.live -= 1;
        true
    }

    fn pop_retired(&mut self, idx: u32, epoch: u32) -> Option<C> {
        let e = self.entries.get_mut(idx as usize)?;
        (e.epoch == epoch && e.retiring)
            .then(|| e.slot.pop())
            .flatten()
    }

    fn finish_retire(&mut self, idx: u32, epoch: u32) {
        let Some(e) = self.entries.get_mut(idx as usize) else {
            return;
        };
        if e.epoch != epoch || !e.retiring {
            return;
        }
        e.retiring = false;
        e.slot.reset();
        if !e.ordered {
            self.release_entry(idx);
        }
    }

    pub fn front_kind(&mut self) -> FrontKind {
        if self.order_head == NONE {
            return FrontKind::Empty;
        }
        match self.metadata.get().item(self.order_head) {
            OrderItem::Boundary => FrontKind::Boundary,
            OrderItem::Slot { idx, epoch } => {
                let e = &self.entries[idx as usize];
                if e.epoch == epoch && e.live {
                    FrontKind::Slot(idx)
                } else {
                    FrontKind::Detached
                }
            }
        }
    }

    fn drop_front(&mut self) {
        let Some(item) = self.pop_order() else {
            return;
        };
        if let OrderItem::Slot { idx, epoch } = item {
            let entry = &mut self.entries[idx as usize];
            if entry.epoch == epoch {
                entry.ordered = false;
                if !entry.live && !entry.retiring {
                    self.release_entry(idx);
                }
            }
        }
    }

    fn pop_live(&mut self, idx: u32) {
        let Some(OrderItem::Slot { idx: front, epoch }) = self.pop_order() else {
            unreachable!()
        };
        debug_assert_eq!(front, idx);
        let entry = &mut self.entries[idx as usize];
        debug_assert_eq!(entry.epoch, epoch);
        entry.ordered = false;
    }

    fn finish(&mut self, idx: u32) -> Option<Waker<'d>> {
        let slot = &mut self.entries[idx as usize].slot;
        slot.completed = true;
        slot.waker.take()
    }

    pub fn mark_boundary(&mut self) -> bool {
        if !self.accepting.get() {
            return false;
        }
        let Some(reserved) = self
            .metadata
            .get()
            .reserve(self.lane, &[OrderItem::Boundary], false)
        else {
            return false;
        };
        self.append_order(reserved[0]);
        true
    }

    pub fn pop_boundary(&mut self) {
        if matches!(self.front_kind(), FrontKind::Boundary) {
            self.pop_order();
        }
    }

    fn pop_order(&mut self) -> Option<OrderItem> {
        let index = self.order_head;
        if index == NONE {
            return None;
        }
        let (next, item) = self.metadata.get().take(self.lane, index);
        self.order_head = next;
        if next == NONE {
            self.order_tail = NONE;
        }
        self.order_len -= 1;
        Some(item)
    }

    fn drain(&mut self) {
        self.accepting.set(false);
        while let Some(item) = self.pop_order() {
            if let OrderItem::Slot { idx, epoch } = item {
                let entry = &mut self.entries[idx as usize];
                if entry.epoch == epoch {
                    entry.ordered = false;
                }
            }
        }
        for idx in 0..self.entries.len() {
            if !self.entries[idx].held {
                continue;
            }
            let entry = &mut self.entries[idx];
            entry.live = false;
            entry.retiring = false;
            entry.ordered = false;
            entry.held = false;
            self.metadata.get().release_entry(self.lane);
        }
        self.live = 0;
        for entry in &mut self.entries {
            entry.slot.detach();
        }
    }

    pub fn try_push(
        &mut self,
        item: C,
        item_bytes: usize,
        item_credits: usize,
    ) -> (Result<(), C>, Option<Waker<'d>>) {
        if let FrontKind::Slot(idx) = self.front_kind() {
            let slot = &mut self.entries[idx as usize].slot;
            let pushed = slot.try_push(
                item,
                self.limits.item_capacity,
                item_bytes,
                self.limits.byte_capacity,
                item_credits,
                self.limits.credit_capacity,
            );
            return (pushed, slot.waker.take());
        }
        (Err(item), None)
    }

    fn complete(&mut self) -> Option<Waker<'d>> {
        match self.front_kind() {
            FrontKind::Slot(idx) => {
                self.pop_live(idx);
                self.finish(idx)
            }
            FrontKind::Detached => {
                self.drop_front();
                None
            }
            FrontKind::Boundary | FrontKind::Empty => None,
        }
    }

    fn fail_one(&mut self, item: C) -> (Option<C>, Option<Waker<'d>>) {
        match self.front_kind() {
            FrontKind::Slot(idx) => {
                self.pop_live(idx);
                let pushed = self.entries[idx as usize]
                    .slot
                    .try_push(
                        item,
                        self.limits.item_capacity,
                        0,
                        self.limits.byte_capacity,
                        1,
                        self.limits.credit_capacity,
                    )
                    .err();
                (pushed, self.finish(idx))
            }
            FrontKind::Detached => {
                self.drop_front();
                (Some(item), None)
            }
            FrontKind::Boundary | FrontKind::Empty => (Some(item), None),
        }
    }

    fn fail_front(&mut self, item: C) -> (bool, bool, Option<C>, Option<Waker<'d>>) {
        match self.front_kind() {
            FrontKind::Empty => (true, false, Some(item), None),
            FrontKind::Boundary => {
                self.pop_order();
                (false, false, Some(item), None)
            }
            FrontKind::Detached => {
                self.drop_front();
                (false, true, Some(item), None)
            }
            FrontKind::Slot(idx) => {
                self.pop_live(idx);
                let rejected = self.entries[idx as usize]
                    .slot
                    .try_push(
                        item,
                        self.limits.item_capacity,
                        0,
                        self.limits.byte_capacity,
                        1,
                        self.limits.credit_capacity,
                    )
                    .err();
                (false, true, rejected, self.finish(idx))
            }
        }
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

pub struct Arena<'d, C> {
    slab: UnsafeCell<ArenaState<'d, C>>,
    _metadata: Option<ArenaPool>,
    _items: Option<ItemPool<C>>,
    _budget: Option<Pin<Rc<ByteBudget>>>,
}

pub struct ArenaFactory<C> {
    capacity: usize,
    limits: Limits,
    item: PhantomData<fn() -> C>,
}

impl<'d, C> Arena<'d, C> {
    pub fn factory(capacity: usize, limits: Limits) -> ArenaFactory<C> {
        ArenaFactory {
            capacity,
            limits,
            item: PhantomData,
        }
    }

    pub fn with_limits(capacity: usize, limits: Limits) -> Self {
        let budget = Rc::pin(ByteBudget::new(limits.byte_capacity));
        let handle = BudgetRef::shared(&budget);
        let items = ItemPool::with_credit_capacity(limits.item_capacity, limits.credit_capacity, 1);
        let nodes = items.handle().raw();
        let metadata = ArenaPool::with_capacity(capacity, 1);
        let metadata_handle = unsafe { metadata.handle().assume_lifetime() };
        Self {
            slab: UnsafeCell::new(ArenaState::with_pool(
                metadata_handle,
                0,
                handle,
                nodes,
                limits,
            )),
            _metadata: Some(metadata),
            _items: Some(items),
            _budget: Some(budget),
        }
    }

    pub fn with_item_pool(capacity: usize, limits: Limits, items: ItemPoolRef<'d, C>) -> Self {
        let budget = Rc::pin(ByteBudget::new(limits.byte_capacity));
        let handle = BudgetRef::shared(&budget);
        let metadata = ArenaPool::with_capacity(capacity, 1);
        let metadata_handle = unsafe { metadata.handle().assume_lifetime() };
        Self {
            slab: UnsafeCell::new(ArenaState::with_pool(
                metadata_handle,
                0,
                handle,
                items.raw(),
                limits,
            )),
            _metadata: Some(metadata),
            _items: None,
            _budget: Some(budget),
        }
    }

    pub fn with_shared_budget(
        capacity: usize,
        limits: Limits,
        budget: Pin<Rc<ByteBudget>>,
    ) -> Self {
        let handle = BudgetRef::shared(&budget);
        let items = ItemPool::with_credit_capacity(limits.item_capacity, limits.credit_capacity, 1);
        let nodes = items.handle().raw();
        let metadata = ArenaPool::with_capacity(capacity, 1);
        let metadata_handle = unsafe { metadata.handle().assume_lifetime() };
        Self {
            slab: UnsafeCell::new(ArenaState::with_pool(
                metadata_handle,
                0,
                handle,
                nodes,
                limits,
            )),
            _metadata: Some(metadata),
            _items: Some(items),
            _budget: Some(budget),
        }
    }

    pub fn with_shared_item_pool(
        capacity: usize,
        limits: Limits,
        budget: Pin<Rc<ByteBudget>>,
        items: ItemPoolRef<'d, C>,
    ) -> Self {
        let handle = BudgetRef::shared(&budget);
        let metadata = ArenaPool::with_capacity(capacity, 1);
        let metadata_handle = unsafe { metadata.handle().assume_lifetime() };
        Self {
            slab: UnsafeCell::new(ArenaState::with_pool(
                metadata_handle,
                0,
                handle,
                items.raw(),
                limits,
            )),
            _metadata: Some(metadata),
            _items: None,
            _budget: Some(budget),
        }
    }

    pub fn with_shared_metadata_pool(
        metadata: ArenaPoolRef<'d>,
        lane: usize,
        limits: Limits,
        budget: Pin<Rc<ByteBudget>>,
        items: ItemPoolRef<'d, C>,
    ) -> Self {
        let handle = BudgetRef::shared(&budget);
        Self {
            slab: UnsafeCell::new(ArenaState::with_pool(
                metadata,
                lane,
                handle,
                items.raw(),
                limits,
            )),
            _metadata: None,
            _items: None,
            _budget: Some(budget),
        }
    }

    pub fn with_fair_shared_pools(
        metadata: ArenaPoolRef<'d>,
        lane: usize,
        limits: Limits,
        budget: LaneBudgetRef<'d>,
        items: ItemPoolRef<'d, C>,
    ) -> Self {
        let budget = unsafe { budget.assume_lifetime() };
        Self {
            slab: UnsafeCell::new(ArenaState::with_pool(
                metadata,
                lane,
                BudgetRef::Lane(budget),
                items.raw(),
                limits,
            )),
            _metadata: None,
            _items: None,
            _budget: None,
        }
    }

    pub fn len(&self) -> usize {
        unsafe { &*self.slab.get() }.len()
    }

    pub fn is_empty(&self) -> bool {
        unsafe { &*self.slab.get() }.is_empty()
    }

    pub fn can_register(&self) -> bool {
        unsafe { &*self.slab.get() }.can_register(false)
    }

    pub fn can_mark_boundary(&self) -> bool {
        unsafe { &*self.slab.get() }.can_mark_boundary()
    }

    pub fn front_kind(&self) -> FrontKind {
        unsafe { &mut *self.slab.get() }.front_kind()
    }

    pub fn mark_boundary(&self) -> bool {
        unsafe { &mut *self.slab.get() }.mark_boundary()
    }

    pub fn pop_boundary(&self) {
        unsafe { &mut *self.slab.get() }.pop_boundary();
    }

    pub fn try_push(&self, item: C, item_bytes: usize, item_credits: usize) -> bool {
        let (pushed, waker) =
            unsafe { &mut *self.slab.get() }.try_push(item, item_bytes, item_credits);
        if let Some(waker) = waker {
            waker.wake();
        }
        pushed.is_ok()
    }

    pub fn complete(&self) {
        let waker = unsafe { &mut *self.slab.get() }.complete();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub fn fail_one(&self, make: impl FnOnce() -> C) {
        let item = make();
        let (rejected, waker) = unsafe { &mut *self.slab.get() }.fail_one(item);
        if let Some(waker) = waker {
            waker.wake();
        }
        drop(rejected);
    }

    pub fn fail_all(&self, mut make: impl FnMut() -> C) -> usize {
        let mut failed = 0;
        loop {
            if matches!(self.front_kind(), FrontKind::Empty) {
                return failed;
            }
            let item = make();
            let (done, counted, rejected, waker) =
                unsafe { &mut *self.slab.get() }.fail_front(item);
            if let Some(waker) = waker {
                waker.wake();
            }
            drop(rejected);
            if done {
                return failed;
            }
            failed += counted as usize;
        }
    }

    fn register(&self, trailing_boundary: bool) -> Option<(u32, u32)> {
        let slab = unsafe { &mut *self.slab.get() };
        slab.register(trailing_boundary)
    }

    fn retire(&self, idx: u32, epoch: u32) {
        if !unsafe { &mut *self.slab.get() }.begin_retire(idx, epoch) {
            return;
        }
        loop {
            let item = unsafe { &mut *self.slab.get() }.pop_retired(idx, epoch);
            let Some(item) = item else {
                break;
            };
            drop(item);
        }
        unsafe { &mut *self.slab.get() }.finish_retire(idx, epoch);
    }

    unsafe fn with_slot<R>(
        &self,
        idx: u32,
        epoch: u32,
        f: impl FnOnce(&mut Slot<'d, C>) -> R,
    ) -> Option<R> {
        let slab = unsafe { &mut *self.slab.get() };
        let entry = slab.entries.get_mut(idx as usize)?;
        (entry.epoch == epoch && entry.live).then(|| f(&mut entry.slot))
    }
}

impl<C> Drop for Arena<'_, C> {
    fn drop(&mut self) {
        unsafe { &mut *self.slab.get() }.drain();
    }
}

impl<C: 'static> StorageFactory for ArenaFactory<C> {
    type Output<'d> = Arena<'d, C>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        Arena::with_limits(self.capacity, self.limits)
    }
}

/// # Safety
/// `extract` must not reenter its arena.
pub unsafe trait Extract<C> {
    type Output;
    const SYNC_AFTER: bool = false;
    fn extract(slot: &mut Slot<'_, C>) -> Option<Self::Output>;
}

struct Handle<'d, C> {
    arena: Option<&'d Arena<'d, C>>,
    idx: u32,
    epoch: u32,
}

impl<'d, C> Handle<'d, C> {
    fn new() -> Self {
        Self {
            arena: None,
            idx: 0,
            epoch: 0,
        }
    }

    fn try_register(&mut self, arena: &'d Arena<'d, C>, trailing_boundary: bool) -> bool {
        if self.arena.is_some() {
            return false;
        }
        let Some((idx, epoch)) = arena.register(trailing_boundary) else {
            return false;
        };
        self.arena = Some(arena);
        self.idx = idx;
        self.epoch = epoch;
        true
    }

    fn with_slot<R>(&self, f: impl FnOnce(&mut Slot<'d, C>) -> R) -> Option<R> {
        unsafe { self.arena?.with_slot(self.idx, self.epoch, f) }
    }

    fn release_done(&mut self) {
        if let Some(arena) = self.arena.take() {
            arena.retire(self.idx, self.epoch);
        }
    }
}

impl<'d, C> Drop for Handle<'d, C> {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.take() {
            arena.retire(self.idx, self.epoch);
        }
    }
}

pub struct Reply<'d, C, X: Extract<C>> {
    handle: Handle<'d, C>,
    _x: PhantomData<fn() -> X>,
}

impl<'d, C, X: Extract<C>> Default for Reply<'d, C, X> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, C, X: Extract<C>> Reply<'d, C, X> {
    pub fn new() -> Self {
        Self {
            handle: Handle::new(),
            _x: PhantomData,
        }
    }
}

mod sealed {
    use super::Arena;

    pub trait Registrable<'d, C> {
        fn attach(&mut self, arena: &'d Arena<'d, C>, trailing_boundary: bool) -> bool;
    }
}

pub trait Registrable<'d, C>: sealed::Registrable<'d, C> {
    fn try_attach(&mut self, arena: &'d Arena<'d, C>) -> bool {
        sealed::Registrable::attach(self, arena, false)
    }

    fn try_attach_with_boundary(
        &mut self,
        arena: &'d Arena<'d, C>,
        trailing_boundary: bool,
    ) -> bool {
        sealed::Registrable::attach(self, arena, trailing_boundary)
    }
}

impl<'d, C, T> Registrable<'d, C> for T where T: sealed::Registrable<'d, C> {}

impl<'d, C, X: Extract<C>> sealed::Registrable<'d, C> for Reply<'d, C, X> {
    fn attach(&mut self, arena: &'d Arena<'d, C>, trailing_boundary: bool) -> bool {
        self.handle.try_register(arena, trailing_boundary)
    }
}

impl<'d, C, X: Extract<C>> Fiber<'d> for Reply<'d, C, X> {
    type Output = X::Output;
    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<X::Output> {
        let me = self.get_mut();
        let Some(poll) = me.handle.with_slot(|slot| {
            if let Some(out) = X::extract(slot) {
                return Poll::Ready(out);
            }
            slot.waker = Some(unsafe { cx.waker_unchecked() });
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
    _x: PhantomData<fn() -> X>,
}

impl<'d, C, X: Extract<C>> Default for ReplyStream<'d, C, X> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, C, X: Extract<C>> ReplyStream<'d, C, X> {
    pub fn new() -> Self {
        Self {
            handle: Handle::new(),
            _x: PhantomData,
        }
    }

    pub fn poll_next(
        self: Pin<&mut Self>,
        cx: Pin<&mut Context<'_, 'd>>,
    ) -> Poll<Option<X::Output>> {
        let me = self.get_mut();
        let Some(poll) = me.handle.with_slot(|slot| {
            if let Some(out) = X::extract(slot) {
                return Poll::Ready(Some(out));
            }
            if slot.completed && slot.is_empty() {
                return Poll::Ready(None);
            }
            slot.waker = Some(unsafe { cx.waker_unchecked() });
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

impl<'d, C, X: Extract<C>> sealed::Registrable<'d, C> for ReplyStream<'d, C, X> {
    fn attach(&mut self, arena: &'d Arena<'d, C>, trailing_boundary: bool) -> bool {
        self.handle.try_register(arena, trailing_boundary)
    }
}
