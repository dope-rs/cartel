use std::cell::Cell;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;

use cartel_core::{
    Arena, ArenaFactory, ArenaPool, Extract, ItemPool, LaneBudget, Limits, Registrable, Reply,
    ReplyStream, Slot,
};
use dope::driver::ready::ReadySlot;
use dope::driver::token::{Epoch, SlotIndex, Token};
use dope::runtime::{Executor, Session, StorageFactory};
use dope::{DriverContext, DriverRef};
use dope_fiber::{Context, Fiber};
use o3::mem::ByteBudget;

struct First;

unsafe impl Extract<u32> for First {
    type Output = u32;

    fn extract(slot: &mut Slot<'_, u32>) -> Option<Self::Output> {
        slot.pop()
    }
}

struct Capped;

unsafe impl Extract<u32> for Capped {
    type Output = Result<u32, ()>;

    fn extract(slot: &mut Slot<'_, u32>) -> Option<Self::Output> {
        slot.pop()
            .map(Ok)
            .or_else(|| slot.take_overflow().then_some(Err(())))
    }
}

struct Harness<'d, C> {
    arena: Arena<'d, C>,
    wake: Pin<Box<ReadySlot<'d>>>,
}

struct HarnessFactory<C> {
    arena: ArenaFactory<C>,
}

impl<C: 'static> StorageFactory for HarnessFactory<C> {
    type Output<'d> = Harness<'d, C>;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        let reference = driver.driver_ref();
        Harness {
            arena: self.arena.build(driver),
            wake: Box::pin(reference.make_ready_slot(Token::new(
                0,
                SlotIndex::new(0),
                Epoch::INITIAL,
            ))),
        }
    }
}

/// Rebrands executor-owned storage with the driver's logical lifetime.
///
/// # Safety
/// Values borrowing the returned reference must be destroyed before the
/// executor session returns. Every caller in this module keeps them inside the
/// synchronous `Executor::enter` closure, whose storage is pinned throughout.
unsafe fn branded_storage<'scope, 'd, S: 'd>(session: &Session<'scope, 'd, S>) -> &'d S {
    unsafe { &*(session.storage() as *const S) }
}

fn with_arena<C: 'static>(
    factory: ArenaFactory<C>,
    f: impl for<'poll, 'd> FnOnce(&'d Arena<'d, C>, Pin<&mut Context<'poll, 'd>>, DriverRef<'d>),
) {
    let exec = Executor::new(dope::driver::Config::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor")
    .with_storage_factory(HarnessFactory { arena: factory });
    exec.enter(|mut sess| {
        let harness = sess.storage() as *const Harness<'_, C>;
        let reference = sess.driver();
        let access = sess.driver_access();
        // `harness` is executor-owned and all borrows created below are
        // consumed before this synchronous session closure returns.
        let harness = unsafe { &*harness };
        let mut context = pin!(Context::from_ready(
            reference,
            harness.wake.as_ref().key(),
            access,
        ));
        f(&harness.arena, context.as_mut(), reference);
    });
}

fn poll<'d>(reply: &mut Reply<'d, u32, First>, context: Pin<&mut Context<'_, 'd>>) -> Poll<u32> {
    Fiber::poll(Pin::new(reply), context)
}

#[test]
fn early_reply_does_not_reuse_an_ordered_slot() {
    with_arena(
        Arena::factory(1, Limits::new(1, 1, 1)),
        |arena, mut context, _driver| {
            let mut first = Reply::<_, First>::new();
            assert!(first.try_attach(arena));
            assert!(arena.try_push(1, 0, 1));
            assert_eq!(poll(&mut first, context.as_mut()), Poll::Ready(1));

            let mut second = Reply::<_, First>::new();
            assert!(!second.try_attach(arena));
            arena.complete();
            assert!(second.try_attach(arena));
            assert!(arena.try_push(2, 0, 1));
            arena.complete();
            assert_eq!(poll(&mut second, context.as_mut()), Poll::Ready(2));

            let mut third = Reply::<_, First>::new();
            assert!(third.try_attach(arena));
            assert!(arena.try_push(3, 0, 1));
            arena.complete();
            assert_eq!(poll(&mut third, context.as_mut()), Poll::Ready(3));
        },
    );
}

#[test]
fn stream_push_wakes_and_reports_its_bound() {
    with_arena(
        Arena::factory(1, Limits::new(1, 4, 1)),
        |arena, mut context, driver| {
            let mut stream = ReplyStream::<_, Capped>::new();
            assert!(stream.try_attach(arena));

            assert_eq!(
                Pin::new(&mut stream).poll_next(context.as_mut()),
                Poll::Pending
            );
            assert!(arena.try_push(1, 4, 1));
            assert!(!arena.try_push(2, 4, 1));

            let mut wakes = Vec::new();
            driver.drain_ready(|token| wakes.push(token));
            assert_eq!(wakes, [Token::new(0, SlotIndex::new(0), Epoch::INITIAL)]);
            assert_eq!(
                Pin::new(&mut stream).poll_next(context.as_mut()),
                Poll::Ready(Some(Ok(1)))
            );
            assert_eq!(
                Pin::new(&mut stream).poll_next(context.as_mut()),
                Poll::Ready(Some(Err(())))
            );
            assert_eq!(
                Pin::new(&mut stream).poll_next(context.as_mut()),
                Poll::Ready(None)
            );
            assert!(!arena.try_push(3, 4, 1));
            arena.complete();
            assert!(arena.is_empty());
        },
    );
}

#[test]
fn item_pool_bounds_multi_item_replies() {
    with_arena(Arena::factory(1, Limits::new(3, 1, 3)), |arena, _, _| {
        let mut stream = ReplyStream::<_, Capped>::new();
        assert!(stream.try_attach(arena));
        assert!(arena.try_push(1, 0, 1));
        assert!(arena.try_push(2, 0, 1));
        assert!(arena.try_push(3, 0, 1));
        assert!(!arena.try_push(4, 0, 1));
    });
}

struct SharedPoolHarness<'d> {
    first: Arena<'d, u32>,
    second: Arena<'d, u32>,
    _items: ItemPool<u32>,
}

struct SharedPoolFactory;

impl StorageFactory for SharedPoolFactory {
    type Output<'d> = SharedPoolHarness<'d>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        let items = ItemPool::with_capacity(3);
        let handle = unsafe { items.handle().assume_lifetime() };
        SharedPoolHarness {
            first: Arena::with_item_pool(1, Limits::new(3, 1, 3), handle),
            second: Arena::with_item_pool(1, Limits::new(3, 1, 3), handle),
            _items: items,
        }
    }
}

#[test]
fn inline_items_consume_the_shared_item_pool() {
    let exec = Executor::new(dope::driver::Config::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor")
    .with_storage_factory(SharedPoolFactory);
    exec.enter(|sess| {
        let harness = unsafe { branded_storage(&sess) };
        let mut first = ReplyStream::<_, Capped>::new();
        let mut second = ReplyStream::<_, Capped>::new();
        assert!(first.try_attach(&harness.first));
        assert!(second.try_attach(&harness.second));
        assert!(harness.first.try_push(1, 0, 1));
        assert!(harness.first.try_push(2, 0, 1));
        assert!(harness.second.try_push(3, 0, 1));
        assert!(!harness.second.try_push(4, 0, 1));
    });
}

#[test]
fn order_ring_rejects_excess_boundaries() {
    with_arena(
        Arena::<u32>::factory(1, Limits::new(1, 1, 1)),
        |arena, _, _| {
            let mut reply = Reply::<_, First>::new();
            assert!(reply.try_attach(arena));
            assert!(arena.mark_boundary());
            assert!(!arena.mark_boundary());
            assert_eq!(arena.len(), 2);
        },
    );
}

#[test]
fn arena_budget_counts_completed_live_replies() {
    with_arena(
        Arena::factory(2, Limits::new(2, 4, 2)),
        |arena, mut context, _driver| {
            let mut first = Reply::<_, First>::new();
            let mut second = Reply::<_, Capped>::new();
            assert!(first.try_attach(arena));
            assert!(second.try_attach(arena));
            assert!(arena.try_push(1, 4, 1));
            arena.complete();
            assert!(!arena.try_push(2, 1, 1));
            arena.complete();
            assert_eq!(poll(&mut first, context.as_mut()), Poll::Ready(1));
            assert_eq!(
                Fiber::poll(Pin::new(&mut second), context.as_mut()),
                Poll::Ready(Err(()))
            );

            let mut third = Reply::<_, First>::new();
            assert!(third.try_attach(arena));
            assert!(arena.try_push(2, 1, 1));
            arena.complete();
            assert_eq!(poll(&mut third, context.as_mut()), Poll::Ready(2));
        },
    );
}

struct Reenter<'a> {
    arena: &'a Arena<'a, Reenter<'a>>,
    drops: Rc<Cell<usize>>,
}

impl Drop for Reenter<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        self.arena.complete();
    }
}

struct Never;

unsafe impl<'a> Extract<Reenter<'a>> for Never {
    type Output = ();

    fn extract(_slot: &mut Slot<'_, Reenter<'a>>) -> Option<Self::Output> {
        None
    }
}

struct ReenterFactory;

impl StorageFactory for ReenterFactory {
    type Output<'d> = Arena<'d, Reenter<'d>>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        Arena::with_limits(1, Limits::new(2, 1, 2))
    }
}

#[test]
fn item_drop_can_reenter_arena() {
    let exec = Executor::new(dope::driver::Config::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor")
    .with_storage_factory(ReenterFactory);
    exec.enter(|sess| {
        let arena = unsafe { branded_storage(&sess) };
        let drops = Rc::new(Cell::new(0));
        let mut reply = Reply::<_, Never>::new();
        assert!(reply.try_attach(arena));
        assert!(arena.try_push(
            Reenter {
                arena,
                drops: drops.clone(),
            },
            0,
            1
        ));
        assert!(!reply.try_attach_with_boundary(arena, true));
        assert!(!arena.is_empty());
        assert_eq!(arena.len(), 1);
        drop(reply);
        assert!(arena.is_empty());
        assert_eq!(arena.len(), 0);

        let mut next = Reply::<_, Never>::new();
        assert!(next.try_attach(arena));
        arena.complete();
        drop(next);
        assert!(arena.is_empty());
    });
}

#[test]
fn arena_drop_detaches_all_items_before_destructor_reentry() {
    let drops = Rc::new(Cell::new(0));
    let exec = Executor::new(dope::driver::Config::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor")
    .with_storage_factory(ReenterFactory);
    exec.enter(|sess| {
        let arena = unsafe { branded_storage(&sess) };
        let mut reply = Reply::<_, Never>::new();
        assert!(reply.try_attach(arena));
        assert!(arena.try_push(
            Reenter {
                arena,
                drops: drops.clone(),
            },
            0,
            1
        ));
        assert!(arena.try_push(
            Reenter {
                arena,
                drops: drops.clone(),
            },
            0,
            1
        ));
        std::mem::forget(reply);
    });
    assert_eq!(drops.get(), 2);
}

#[test]
fn registration_reserves_trailing_boundary_atomically() {
    with_arena(Arena::factory(2, Limits::new(2, 1, 2)), |arena, _, _| {
        let mut occupied = Reply::<u32, First>::new();
        assert!(occupied.try_attach(arena));
        assert!(arena.mark_boundary());
        assert!(arena.mark_boundary());
        assert!(arena.can_register());
        assert!(arena.can_mark_boundary());
        assert_eq!(arena.len(), 3);

        let mut reply = Reply::<u32, First>::new();
        assert!(!reply.try_attach_with_boundary(arena, true));
        assert_eq!(arena.len(), 3);
        arena.complete();
        drop(occupied);
        arena.pop_boundary();
        arena.pop_boundary();
        assert_eq!(arena.len(), 0);

        assert!(reply.try_attach_with_boundary(arena, true));
        assert_eq!(arena.len(), 2);
        arena.complete();
        drop(reply);
        assert!(arena.is_empty());
        arena.pop_boundary();
        assert_eq!(arena.len(), 0);
    });
}

struct SharedMetadataHarness<'d> {
    first: Arena<'d, u32>,
    second: Arena<'d, u32>,
    _metadata: ArenaPool,
    budget: Pin<Rc<ByteBudget>>,
    _items: ItemPool<u32>,
}

struct SharedMetadataFactory;

impl StorageFactory for SharedMetadataFactory {
    type Output<'d> = SharedMetadataHarness<'d>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        let budget = Rc::pin(ByteBudget::new(8));
        let items = ItemPool::with_capacity(0);
        let item_handle = unsafe { items.handle().assume_lifetime() };
        let metadata = ArenaPool::with_capacity(4, 2);
        let metadata_handle = unsafe { metadata.handle().assume_lifetime() };
        SharedMetadataHarness {
            first: Arena::with_shared_metadata_pool(
                metadata_handle,
                0,
                Limits::new(1, 4, 1),
                budget.clone(),
                item_handle,
            ),
            second: Arena::with_shared_metadata_pool(
                metadata_handle,
                1,
                Limits::new(1, 4, 1),
                budget.clone(),
                item_handle,
            ),
            _metadata: metadata,
            budget,
            _items: items,
        }
    }
}

#[test]
fn shared_metadata_pool_preserves_reserve_after_shared_surplus() {
    let exec = Executor::new(dope::driver::Config::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor")
    .with_storage_factory(SharedMetadataFactory);
    exec.enter(|sess| {
        let harness = unsafe { branded_storage(&sess) };
        let mut first = Reply::<u32, First>::new();
        let mut first_more = Reply::<u32, First>::new();
        let mut monopolize = Reply::<u32, First>::new();
        let mut second = Reply::<u32, First>::new();
        let mut second_more = Reply::<u32, First>::new();
        assert!(first.try_attach(&harness.first));
        assert!(first_more.try_attach(&harness.first));
        assert!(monopolize.try_attach(&harness.first));
        assert!(second.try_attach(&harness.second));
        assert!(!second_more.try_attach(&harness.second));
    });
}

#[test]
fn shared_budget_lease_releases_with_reply() {
    let exec = Executor::new(dope::driver::Config::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor")
    .with_storage_factory(SharedMetadataFactory);
    exec.enter(|sess| {
        let harness = unsafe { branded_storage(&sess) };
        let mut first = Reply::<u32, First>::new();
        assert!(first.try_attach(&harness.first));
        assert!(harness.first.try_push(1, 4, 0));
        assert_eq!(harness.budget.as_ref().used(), 4);
        drop(first);
        assert_eq!(harness.budget.as_ref().used(), 0);

        let mut second = Reply::<u32, First>::new();
        assert!(second.try_attach(&harness.second));
        assert!(harness.second.try_push(2, 4, 0));
        assert_eq!(harness.budget.as_ref().used(), 4);
        drop(second);
        assert_eq!(harness.budget.as_ref().used(), 0);
    });
}

struct FairPoolHarness<'d> {
    first: Arena<'d, u32>,
    second: Arena<'d, u32>,
    _metadata: ArenaPool,
    _budget: LaneBudget,
    _items: ItemPool<u32>,
}

struct FairPoolFactory;

impl StorageFactory for FairPoolFactory {
    type Output<'d> = FairPoolHarness<'d>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        let metadata = ArenaPool::with_capacity(4, 2);
        let metadata_handle = unsafe { metadata.handle().assume_lifetime() };
        let budget = LaneBudget::with_capacity(8, 2);
        let first_budget = unsafe { budget.handle(0).assume_lifetime() };
        let second_budget = unsafe { budget.handle(1).assume_lifetime() };
        let items = ItemPool::with_lanes(8, 2);
        let first_items = unsafe { items.handle_for(0).assume_lifetime() };
        let second_items = unsafe { items.handle_for(1).assume_lifetime() };
        FairPoolHarness {
            first: Arena::with_fair_shared_pools(
                metadata_handle,
                0,
                Limits::new(4, 8, 4),
                first_budget,
                first_items,
            ),
            second: Arena::with_fair_shared_pools(
                metadata_handle,
                1,
                Limits::new(4, 8, 4),
                second_budget,
                second_items,
            ),
            _metadata: metadata,
            _budget: budget,
            _items: items,
        }
    }
}

#[test]
fn byte_lanes_keep_reserve_after_shared_surplus_is_used() {
    let exec = Executor::new(dope::driver::Config::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor")
    .with_storage_factory(FairPoolFactory);
    exec.enter(|sess| {
        let harness = unsafe { branded_storage(&sess) };
        let mut first = ReplyStream::<_, Capped>::new();
        let mut second = ReplyStream::<_, Capped>::new();
        assert!(first.try_attach(&harness.first));
        assert!(second.try_attach(&harness.second));
        assert!(harness.first.try_push(1, 2, 1));
        assert!(harness.first.try_push(2, 2, 1));
        assert!(harness.first.try_push(3, 2, 1));
        assert!(!harness.first.try_push(4, 2, 1));
        assert!(harness.second.try_push(5, 2, 1));
    });
}

struct FairRowPoolFactory;

impl StorageFactory for FairRowPoolFactory {
    type Output<'d> = FairPoolHarness<'d>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        let metadata = ArenaPool::with_capacity(4, 2);
        let metadata_handle = unsafe { metadata.handle().assume_lifetime() };
        let budget = LaneBudget::with_capacity(8, 2);
        let first_budget = unsafe { budget.handle(0).assume_lifetime() };
        let second_budget = unsafe { budget.handle(1).assume_lifetime() };
        let items = ItemPool::with_lanes(4, 2);
        let first_items = unsafe { items.handle_for(0).assume_lifetime() };
        let second_items = unsafe { items.handle_for(1).assume_lifetime() };
        FairPoolHarness {
            first: Arena::with_fair_shared_pools(
                metadata_handle,
                0,
                Limits::new(4, 4, 4),
                first_budget,
                first_items,
            ),
            second: Arena::with_fair_shared_pools(
                metadata_handle,
                1,
                Limits::new(4, 4, 4),
                second_budget,
                second_items,
            ),
            _metadata: metadata,
            _budget: budget,
            _items: items,
        }
    }
}

#[test]
fn row_lanes_count_inline_rows_and_preserve_peer_reserve() {
    let exec = Executor::new(dope::driver::Config::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor")
    .with_storage_factory(FairRowPoolFactory);
    exec.enter(|sess| {
        let harness = unsafe { branded_storage(&sess) };
        let mut first = ReplyStream::<_, Capped>::new();
        let mut second = ReplyStream::<_, Capped>::new();
        assert!(first.try_attach(&harness.first));
        assert!(second.try_attach(&harness.second));
        assert!(harness.first.try_push(1, 0, 1));
        assert!(harness.first.try_push(2, 0, 1));
        assert!(harness.first.try_push(3, 0, 1));
        assert!(!harness.first.try_push(4, 0, 1));
        assert!(harness.second.try_push(5, 0, 1));
    });
}
