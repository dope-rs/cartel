use std::pin::{Pin, pin};
use std::task::Poll;

use cartel_core::{
    Arena, ArenaConfig, Extract, FrontKind, Limits, Registrable, Reply, ReplyStream, Slot,
};
use dope::driver::token::{Epoch, SlotIndex, Token};
use dope::runtime::profile::Throughput;
use dope::runtime::{Executor, StorageFactory};
use dope::{DriverContext, driver};
use dope_fiber::{Context, Fiber};

struct First;

impl Extract<u32> for First {
    type Output = u32;

    fn extract(slot: &mut Slot<'_, u32>) -> Option<Self::Output> {
        slot.pop()
    }
}

struct Factory(ArenaConfig);

impl StorageFactory for Factory {
    type Output<'d> = Arena<'d, u32>;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        Arena::new(self.0)
    }
}

fn with_arena(
    config: ArenaConfig,
    f: impl for<'poll, 'd> FnOnce(&'d Arena<'d, u32>, Pin<&mut Context<'poll, 'd>>),
) {
    let cfg = driver::Config::for_tcp_profile::<Throughput>(8);
    Executor::new(cfg)
        .expect("executor")
        .with_storage_factory(Factory(config))
        .enter(|mut session| {
            let arena = session.storage();
            let reference = session.driver();
            let wake = Box::pin(
                reference
                    .make_ready_slot(Token::new(0, SlotIndex::new(0), Epoch::INITIAL))
                    .expect("ready slot"),
            );
            let mut context = pin!(Context::from_ready(
                reference,
                wake.as_ref().key(),
                session.driver_access(),
            ));
            f(arena, context.as_mut());
        });
}

fn single(capacity: usize, limits: Limits) -> ArenaConfig {
    ArenaConfig::single(capacity, limits)
}

#[test]
fn completed_reply_releases_its_slot_for_reuse() {
    with_arena(single(1, Limits::new(1, 16, 1)), |arena, mut cx| {
        let lane = arena.lane(0);
        let mut first = Reply::<u32, First>::new();
        assert!(first.try_attach(cx.as_mut().region_token(), lane));
        assert!(lane.try_push(cx.as_mut().region_token(), 7, 1, 1));
        lane.complete(cx.as_mut().region_token());
        assert_eq!(
            Fiber::poll(Pin::new(&mut first), cx.as_mut()),
            Poll::Ready(7)
        );

        let mut second = Reply::<u32, First>::new();
        assert!(second.try_attach(cx.as_mut().region_token(), lane));
        assert!(lane.try_push(cx.as_mut().region_token(), 9, 1, 1));
        lane.complete(cx.as_mut().region_token());
        assert_eq!(Fiber::poll(Pin::new(&mut second), cx), Poll::Ready(9));
    });
}

#[test]
fn item_lanes_follow_live_entries_across_protocol_lane_high_water() {
    let config = ArenaConfig::new(2, 4, 4, 16, 4, Limits::new(1, 4, 1));
    with_arena(config, |arena, mut cx| {
        let left = arena.lane(0);
        let right = arena.lane(1);
        let mut left1 = Reply::<u32, First>::new();
        let mut left2 = Reply::<u32, First>::new();
        let mut left3 = Reply::<u32, First>::new();
        assert!(left1.try_attach(cx.as_mut().region_token(), left));
        assert!(left2.try_attach(cx.as_mut().region_token(), left));
        assert!(left3.try_attach(cx.as_mut().region_token(), left));
        for value in 1..=3 {
            assert!(left.try_push(cx.as_mut().region_token(), value, 1, 1));
            left.complete(cx.as_mut().region_token());
        }
        assert_eq!(
            Fiber::poll(Pin::new(&mut left1), cx.as_mut()),
            Poll::Ready(1)
        );
        assert_eq!(
            Fiber::poll(Pin::new(&mut left2), cx.as_mut()),
            Poll::Ready(2)
        );
        assert_eq!(
            Fiber::poll(Pin::new(&mut left3), cx.as_mut()),
            Poll::Ready(3)
        );

        let mut right1 = Reply::<u32, First>::new();
        let mut right2 = Reply::<u32, First>::new();
        let mut right3 = Reply::<u32, First>::new();
        assert!(right1.try_attach(cx.as_mut().region_token(), right));
        assert!(right2.try_attach(cx.as_mut().region_token(), right));
        assert!(right3.try_attach(cx.as_mut().region_token(), right));
    });
}

#[test]
fn dropped_reply_is_retired_before_the_next_registration() {
    with_arena(single(1, Limits::new(1, 16, 1)), |arena, mut cx| {
        let lane = arena.lane(0);
        let mut reply = Reply::<u32, First>::new();
        assert!(reply.try_attach(cx.as_mut().region_token(), lane));
        assert!(lane.try_push(cx.as_mut().region_token(), 1, 1, 1));
        drop(reply);

        let mut next = Reply::<u32, First>::new();
        assert!(!next.try_attach(cx.as_mut().region_token(), lane));
        lane.complete(cx.as_mut().region_token());
        assert!(next.try_attach(cx.as_mut().region_token(), lane));
    });
}

#[test]
fn stream_observes_items_then_completion() {
    with_arena(single(1, Limits::new(2, 16, 2)), |arena, mut cx| {
        let lane = arena.lane(0);
        let mut stream = ReplyStream::<u32, First>::new();
        assert!(stream.try_attach(cx.as_mut().region_token(), lane));
        assert!(lane.try_push(cx.as_mut().region_token(), 1, 1, 1));
        assert!(lane.try_push(cx.as_mut().region_token(), 2, 1, 1));
        lane.complete(cx.as_mut().region_token());

        assert_eq!(
            Pin::new(&mut stream).poll_next(cx.as_mut()),
            Poll::Ready(Some(1))
        );
        assert_eq!(
            Pin::new(&mut stream).poll_next(cx.as_mut()),
            Poll::Ready(Some(2))
        );
        assert_eq!(Pin::new(&mut stream).poll_next(cx), Poll::Ready(None));
    });
}

#[test]
fn slot_limits_fail_closed_after_overflow() {
    with_arena(single(1, Limits::new(2, 4, 2)), |arena, mut cx| {
        let lane = arena.lane(0);
        let mut stream = ReplyStream::<u32, First>::new();
        assert!(stream.try_attach(cx.as_mut().region_token(), lane));
        assert!(lane.try_push(cx.as_mut().region_token(), 1, 4, 1));
        assert!(!lane.try_push(cx.as_mut().region_token(), 2, 1, 1));
        assert!(!lane.try_push(cx.as_mut().region_token(), 3, 0, 1));
    });
}

#[test]
fn boundaries_preserve_pipeline_batches() {
    with_arena(single(2, Limits::new(1, 8, 1)), |arena, mut cx| {
        let lane = arena.lane(0);
        let mut reply = Reply::<u32, First>::new();
        assert!(reply.try_attach_with_boundary(cx.as_mut().region_token(), lane, true));
        assert!(lane.try_push(cx.as_mut().region_token(), 1, 0, 1));
        lane.complete(cx.as_mut().region_token());
        assert_eq!(
            lane.front_kind(cx.as_mut().region_token()),
            FrontKind::Boundary
        );
        lane.pop_boundary(cx.as_mut().region_token());
        assert_eq!(
            lane.front_kind(cx.as_mut().region_token()),
            FrontKind::Empty
        );
    });
}

#[test]
fn metadata_reserve_prevents_one_lane_from_starving_another() {
    let limits = Limits::new(1, 8, 1);
    let config = ArenaConfig::new(2, 4, 4, 4, 4, limits);
    with_arena(config, |arena, mut cx| {
        let a = arena.lane(0);
        let b = arena.lane(1);
        let mut a1 = Reply::<u32, First>::new();
        let mut a2 = Reply::<u32, First>::new();
        let mut a3 = Reply::<u32, First>::new();
        let mut a4 = Reply::<u32, First>::new();
        let mut b1 = Reply::<u32, First>::new();
        assert!(a1.try_attach(cx.as_mut().region_token(), a));
        assert!(a2.try_attach(cx.as_mut().region_token(), a));
        assert!(a3.try_attach(cx.as_mut().region_token(), a));
        assert!(!a4.try_attach(cx.as_mut().region_token(), a));
        assert!(b1.try_attach(cx.as_mut().region_token(), b));
    });
}

#[test]
fn byte_reserve_prevents_one_lane_from_starving_another() {
    let limits = Limits::new(3, 4, 3);
    let config = ArenaConfig::new(2, 2, 4, 4, 4, limits);
    with_arena(config, |arena, mut cx| {
        let a = arena.lane(0);
        let b = arena.lane(1);
        let mut first = Reply::<u32, First>::new();
        let mut second = Reply::<u32, First>::new();
        assert!(first.try_attach(cx.as_mut().region_token(), a));
        assert!(second.try_attach(cx.as_mut().region_token(), b));
        assert!(a.try_push(cx.as_mut().region_token(), 1, 3, 1));
        assert!(!a.try_push(cx.as_mut().region_token(), 2, 1, 1));
        assert!(b.try_push(cx.as_mut().region_token(), 3, 1, 1));
    });
}

#[test]
fn active_lane_selection_is_round_robin() {
    let config = ArenaConfig::new(2, 4, 4, 4, 4, Limits::new(1, 1, 1));
    with_arena(config, |arena, mut cx| {
        arena.activate(cx.as_mut().region_token(), 0);
        arena.activate(cx.as_mut().region_token(), 1);
        assert_eq!(arena.pick_active(cx.as_mut().region_token()), Some(0));
        assert_eq!(arena.pick_active(cx.as_mut().region_token()), Some(1));
        assert_eq!(arena.pick_active(cx.as_mut().region_token()), Some(0));
        arena.deactivate(cx.as_mut().region_token(), 0);
        assert_eq!(arena.pick_active(cx.as_mut().region_token()), Some(1));
    });
}

#[test]
fn fail_all_crosses_detached_slots_and_boundaries() {
    with_arena(single(2, Limits::new(1, 8, 1)), |arena, mut cx| {
        let lane = arena.lane(0);
        let mut first = Reply::<u32, First>::new();
        let mut second = Reply::<u32, First>::new();
        assert!(first.try_attach_with_boundary(cx.as_mut().region_token(), lane, true));
        assert!(second.try_attach(cx.as_mut().region_token(), lane));
        drop(first);
        assert_eq!(lane.fail_all(cx.as_mut().region_token(), || 99), 2);
        drop(second);
        assert!(lane.is_empty(cx.as_mut().region_token()));
    });
}
