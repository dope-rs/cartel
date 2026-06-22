use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use dope::WakeRef;

pub struct Slot<C> {
    first: Option<C>,
    rest: VecDeque<C>,
    completed: bool,
    waker: Option<WakeRef>,
}

impl<C> Slot<C> {
    fn new() -> Self {
        Self {
            first: None,
            rest: VecDeque::new(),
            completed: false,
            waker: None,
        }
    }

    fn reset(&mut self) {
        self.first = None;
        self.rest.clear();
        self.completed = false;
        self.waker = None;
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    pub fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    pub fn pop(&mut self) -> Option<C> {
        let head = self.first.take()?;
        self.first = self.rest.pop_front();
        Some(head)
    }

    fn push(&mut self, item: C) {
        if self.first.is_none() {
            self.first = Some(item);
        } else {
            self.rest.push_back(item);
        }
    }
}

enum OrderItem {
    Slot { idx: u32, epoch: u32 },
    Detached { idx: u32 },
    Boundary,
}

pub enum FrontKind {
    Slot(u32),
    Detached,
    Boundary,
    Empty,
}

struct Entry<C> {
    slot: Slot<C>,
    epoch: u32,
    live: bool,
}

pub struct Slab<C> {
    entries: Vec<Entry<C>>,
    free: Vec<u32>,
    order: VecDeque<OrderItem>,
}

impl<C> Default for Slab<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> Slab<C> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
            order: VecDeque::new(),
        }
    }

    pub fn depth(&self) -> usize {
        self.order.len()
    }

    pub fn is_drained(&self) -> bool {
        self.entries.iter().all(|e| !e.live)
    }

    fn register(&mut self) -> (u32, u32) {
        let idx = if let Some(idx) = self.free.pop() {
            let entry = &mut self.entries[idx as usize];
            entry.slot.reset();
            entry.epoch = entry.epoch.wrapping_add(1);
            entry.live = true;
            idx
        } else {
            self.entries.push(Entry {
                slot: Slot::new(),
                epoch: 1,
                live: true,
            });
            (self.entries.len() - 1) as u32
        };
        let epoch = self.entries[idx as usize].epoch;
        self.order.push_back(OrderItem::Slot { idx, epoch });
        (idx, epoch)
    }

    fn detach(&mut self, idx: u32, epoch: u32) {
        let Some(e) = self.entries.get_mut(idx as usize) else {
            return;
        };
        if e.epoch != epoch || !e.live {
            return;
        }
        e.live = false;
        let mut in_order = false;
        for item in &mut self.order {
            if let OrderItem::Slot { idx: i, epoch: ep } = *item
                && i == idx
                && ep == epoch
            {
                *item = OrderItem::Detached { idx };
                in_order = true;
                break;
            }
        }
        if !in_order {
            self.free.push(idx);
        }
    }

    fn release(&mut self, idx: u32, epoch: u32) {
        let Some(e) = self.entries.get_mut(idx as usize) else {
            return;
        };
        if e.epoch != epoch || !e.live {
            return;
        }
        e.live = false;
        e.slot.reset();
        self.free.push(idx);
    }

    pub fn front_kind(&mut self) -> FrontKind {
        match self.order.front() {
            None => FrontKind::Empty,
            Some(OrderItem::Boundary) => FrontKind::Boundary,
            Some(OrderItem::Detached { .. }) => FrontKind::Detached,
            Some(&OrderItem::Slot { idx, epoch }) => {
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
        if let Some(OrderItem::Detached { idx }) = self.order.pop_front() {
            self.free.push(idx);
        }
    }

    fn finish(&mut self, idx: u32) {
        let slot = &mut self.entries[idx as usize].slot;
        slot.completed = true;
        if let Some(w) = slot.waker.take() {
            w.wake();
        }
    }

    pub fn mark_boundary(&mut self) {
        self.order.push_back(OrderItem::Boundary);
    }

    pub fn pop_boundary(&mut self) {
        if matches!(self.front_kind(), FrontKind::Boundary) {
            self.order.pop_front();
        }
    }

    pub fn push(&mut self, item: C) {
        if let FrontKind::Slot(idx) = self.front_kind() {
            self.entries[idx as usize].slot.push(item);
        }
    }

    pub fn complete(&mut self) {
        match self.front_kind() {
            FrontKind::Slot(idx) => {
                self.order.pop_front();
                self.finish(idx);
            }
            FrontKind::Detached => self.drop_front(),
            FrontKind::Boundary | FrontKind::Empty => {}
        }
    }

    pub fn fail_one(&mut self, make: impl FnOnce() -> C) {
        match self.front_kind() {
            FrontKind::Slot(idx) => {
                self.order.pop_front();
                self.entries[idx as usize].slot.push(make());
                self.finish(idx);
            }
            FrontKind::Detached => self.drop_front(),
            FrontKind::Boundary | FrontKind::Empty => {}
        }
    }

    pub fn fail_all(&mut self, mut make: impl FnMut() -> C) -> usize {
        let mut n = 0usize;
        loop {
            match self.front_kind() {
                FrontKind::Empty => break,
                FrontKind::Boundary => {
                    self.order.pop_front();
                }
                FrontKind::Detached => {
                    self.drop_front();
                    n += 1;
                }
                FrontKind::Slot(idx) => {
                    self.order.pop_front();
                    self.entries[idx as usize].slot.push(make());
                    self.finish(idx);
                    n += 1;
                }
            }
        }
        n
    }
}

pub trait Extract<C> {
    type Output;
    const SYNC_AFTER: bool = false;
    fn extract(slot: &mut Slot<C>) -> Option<Self::Output>;
}

struct Handle<'d, C> {
    slab: Option<NonNull<Slab<C>>>,
    _brand: PhantomData<&'d Slab<C>>,
    idx: u32,
    epoch: u32,
}

impl<'d, C> Handle<'d, C> {
    fn new() -> Self {
        Self {
            slab: None,
            _brand: PhantomData,
            idx: 0,
            epoch: 0,
        }
    }

    unsafe fn register_raw(&mut self, slab: NonNull<Slab<C>>) {
        // SAFETY: caller ensures `slab` ptr outlives the 'd brand of this Handle.
        let s = unsafe { &mut *slab.as_ptr() };
        let (idx, epoch) = s.register();
        self.slab = Some(slab);
        self.idx = idx;
        self.epoch = epoch;
    }

    #[allow(clippy::mut_from_ref)]
    fn slot(&self) -> Option<&mut Slot<C>> {
        let mut slab = self.slab?;
        // SAFETY: 'd brand enforces Slab outlives this Handle; thread-per-core run-loop.
        let slab = unsafe { slab.as_mut() };
        let entry = slab.entries.get_mut(self.idx as usize)?;
        if entry.epoch != self.epoch || !entry.live {
            return None;
        }
        Some(&mut entry.slot)
    }

    fn release_done(&mut self) {
        if let Some(mut slab) = self.slab.take() {
            // SAFETY: 'd brand enforces Slab outlives this Handle; thread-per-core run-loop.
            unsafe { slab.as_mut() }.release(self.idx, self.epoch);
        }
    }
}

impl<'d, C> Drop for Handle<'d, C> {
    fn drop(&mut self) {
        if let Some(mut slab) = self.slab.take() {
            // SAFETY: 'd brand enforces Slab outlives this Handle; thread-per-core run-loop.
            unsafe { slab.as_mut() }.detach(self.idx, self.epoch);
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

    pub unsafe fn register_mut_raw(&mut self, slab: NonNull<Slab<C>>) {
        // SAFETY: caller ensures `slab` ptr outlives the 'd brand of this Reply.
        unsafe { self.handle.register_raw(slab) }
    }
}

pub trait Registrable<'d, C> {
    fn attach(&mut self, slab: &mut Slab<C>);
}

impl<'d, C, X: Extract<C>> Registrable<'d, C> for Reply<'d, C, X> {
    fn attach(&mut self, slab: &mut Slab<C>) {
        // SAFETY: caller invariant — Slab outlives 'd brand (thread-per-core, pinned Connector).
        unsafe { self.register_mut_raw(NonNull::from(slab)) }
    }
}

impl<'d, C, X: Extract<C>> Registrable<'d, C> for ReplyStream<'d, C, X> {
    fn attach(&mut self, slab: &mut Slab<C>) {
        // SAFETY: caller invariant — Slab outlives 'd brand (thread-per-core, pinned Connector).
        unsafe { self.register_mut_raw(NonNull::from(slab)) }
    }
}

impl<'d, C, X: Extract<C>> Future for Reply<'d, C, X> {
    type Output = X::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<X::Output> {
        let me = self.get_mut();
        let Some(slot) = me.handle.slot() else {
            return Poll::Pending;
        };
        if let Some(out) = X::extract(slot) {
            me.handle.release_done();
            return Poll::Ready(out);
        }
        slot.waker = Some(WakeRef::verified(cx.waker()));
        Poll::Pending
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

    pub unsafe fn register_mut_raw(&mut self, slab: NonNull<Slab<C>>) {
        // SAFETY: caller ensures `slab` ptr outlives the 'd brand of this ReplyStream.
        unsafe { self.handle.register_raw(slab) }
    }

    pub fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<X::Output>> {
        let me = self.get_mut();
        let Some(slot) = me.handle.slot() else {
            return Poll::Ready(None);
        };
        if let Some(out) = X::extract(slot) {
            return Poll::Ready(Some(out));
        }
        if slot.completed && slot.is_empty() {
            me.handle.release_done();
            return Poll::Ready(None);
        }
        slot.waker = Some(WakeRef::verified(cx.waker()));
        Poll::Pending
    }
}
