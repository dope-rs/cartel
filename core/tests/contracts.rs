mod common;

use std::{cell, convert::Infallible};

use cartel_core::{
    frame::{Buffer, Sensitivity},
    handoff, queue, reply,
    reply::{layout, recv, recv::attach::Attach as _, usage},
};
use common::Pending;
use dope::{
    core::driver::{life::recovery, settings},
    net::link::egress,
};
use dope_test::scope::Driver;
use o3::{
    base::cell::region,
    buffer::pool,
    depot::queue::pool::fair,
    mem::credit::fair::{Axis, Cause},
};

#[test]
fn decimal_suffix() {
    for (value, expected) in [(0, b"0".as_slice()), (u64::MAX, b"18446744073709551615")] {
        let mut buffer = [0; 20];
        let start = cartel_core::write_digits(&mut buffer, value);
        assert_eq!(&buffer[start..], expected);
    }
}

#[test]
fn completed_frames() {
    for (kind, bytes, cleared) in [
        (
            Sensitivity::Plain,
            b"S\0\0\0\x04".as_slice(),
            b"S\0\0\0\x04".as_slice(),
        ),
        (Sensitivity::Secret, b"secret".as_slice(), &[0; 6]),
    ] {
        let pool = (pool::Pool::try_new(1, 8)).expect("the frame pool must allocate");
        let origin;
        {
            let mut span = (pool.acquire()).expect("the frame pool has one slot");
            origin = span.as_ref().as_ptr();
            span.try_append(bytes).expect("the frame fits").commit();
            let frame = Buffer::new(span, kind);
            let frame = egress::NonEmpty::try_new(frame).expect("nonempty initialized frame");
            assert_eq!(frame.as_inner().as_ref(), bytes);
            assert_eq!(pool.available(), 0);
        }
        assert_eq!(pool.available(), 1);
        // SAFETY: The pool retains the slot; the completed frame initialized exactly bytes.len() bytes.
        assert_eq!(
            unsafe { std::slice::from_raw_parts(origin, bytes.len()) },
            cleared
        );
    }
}

#[test]
fn restores_input_credit() {
    let requests = (fair::Plan::new(1, 1)).expect("required fixture outcome");
    let replies = (layout::Layout::single(
        layout::Entries::new(1),
        layout::Limit::new(
            layout::Data::new(1),
            usage::Bytes::new(1),
            usage::Values::new(1),
        ),
    ))
    .expect("topology fixture completed");
    region::Token::scope(|mut token| {
        let requests = (queue::Arena::try_new(requests, &token)).expect("required fixture outcome");
        let result = reply::Arena::try_with(replies, token.proof(), |replies| {
            let arena = requests
                .try_bind(replies.domain())
                .expect("matching lane extent");
            let replies =
                (replies.try_lane(reply::LaneIndex::ZERO)).expect("topology fixture completed");
            let requests = arena.lane_for(replies);
            let token = &mut token;
            let mut first = recv::Response::<_, Pending>::new();
            let success = handoff::Lane::new(&arena, replies).try_with(
                token,
                7,
                (),
                11,
                |token, lane, tag, item| match first.try_ordered(token, lane, tag) {
                    Ok(depth) => Ok((depth, item)),
                    Err(cause) => Err((cause, item)),
                },
                |item, depth| (item, depth.get()),
            );
            assert!(success == Ok(1));
            assert_eq!(requests.weight(token), 7);
            assert_eq!(requests.pop_front(token), Some(11));
            assert_eq!(requests.weight(token), 0);

            let mut second = recv::Response::<_, Pending>::new();
            let built = cell::Cell::new(false);
            let full = handoff::Lane::new(&arena, replies).try_publish(
                token,
                19,
                &mut second,
                (),
                23,
                |item| {
                    built.set(true);
                    (item, ())
                },
            );
            assert!(
                matches!(full, Err((handoff::Failure::Reply(recv::attach::Error::Full {
            len,
            capacity,
        }), 23)) if len.get() == 1 && capacity.get() == 1)
            );
            assert!(!built.get());
            assert!(requests.is_empty(token));
            assert_eq!(requests.weight(token), 0);
            assert_eq!(replies.depth(token).get(), 1);

            assert!(
                requests.try_push(token, 29, 3).is_ok(),
                "queue must preserve the admitted value"
            );
            let full = handoff::Lane::new(&arena, replies).try_publish(
                token,
                31,
                &mut second,
                (),
                37,
                |item| {
                    built.set(true);
                    (item, ())
                },
            );
            assert!(matches!(
                full,
                Err((handoff::Failure::Queue(queue::Rejection::Capacity(_)), 37))
            ));
            assert!(!built.get());
            assert_eq!(requests.weight(token), 3);
            assert_eq!(requests.pop_front(token), Some(29));
            assert_eq!(requests.weight(token), 0);
            assert_eq!(replies.depth(token).get(), 1);
        });
        result.expect("reply arena allocation");
    });
}

#[test]
fn queue_domain() {
    let replies = layout::Layout::new(
        layout::Capacity::new(
            layout::Lanes::new(2),
            layout::Entries::new(2),
            layout::Data::new(2),
            usage::Bytes::new(2),
            usage::Values::new(2),
        ),
        layout::Limit::new(
            layout::Data::new(1),
            usage::Bytes::new(1),
            usage::Values::new(1),
        ),
    )
    .expect("reply layout");
    region::Token::scope(|mut token| {
        reply::Arena::<()>::try_with(replies, token.proof(), |replies| {
            let config = fair::Plan::new(1, 1).expect("queue layout");
            let mismatch = queue::Arena::<()>::try_unweighted(config, &token)
                .expect("queue storage")
                .try_bind(replies.domain())
                .err()
                .expect("different lane extent");
            assert_eq!((mismatch.expected.get(), mismatch.actual), (2, 1));

            let config = fair::Plan::new(2, 2).expect("queue layout");
            let requests = queue::Arena::try_unweighted(config, &token)
                .expect("queue storage")
                .try_bind(replies.domain())
                .expect("matching lane extent");
            let first = requests.lane_for(
                replies
                    .try_lane(reply::LaneIndex::ZERO)
                    .expect("first lane"),
            );
            let reply = replies
                .try_lane(reply::LaneIndex::new::<1>())
                .expect("second lane");
            let second = requests.lane_for(reply);
            let mut response = recv::Response::<_, Pending>::new();
            assert!(
                handoff::Lane::new(&requests, reply)
                    .try_publish(&mut token, (), &mut response, (), 7, |item| (item, ()))
                    .is_ok()
            );
            assert!(first.is_empty(&token));
            assert_eq!(second.pop_front(&mut token), Some(7));
        })
        .expect("reply storage");
    });
}

#[test]
fn lane_weights() {
    region::Token::scope(|mut token| {
        let config = (fair::Plan::new(3, 2)).expect("three slots must fit two lanes");
        let arena = (queue::Arena::try_new(config, &token)).expect("the queue must allocate");
        let (left, right) = match (arena.lane(0), arena.lane(1)) {
            (Some(left), Some(right)) => Some((left, right)),
            _ => None,
        }
        .expect("both configured lanes must exist");

        assert!(
            left.try_push(&mut token, 1, 10).is_ok(),
            "queue must preserve the admitted value"
        );
        assert!(
            left.try_push(&mut token, 2, 20).is_ok(),
            "queue must preserve the admitted value"
        );
        assert!(
            right.try_push(&mut token, 5, 50).is_ok(),
            "queue must preserve the admitted value"
        );
        assert_eq!(left.weight(&token), 30);
        assert_eq!(right.weight(&token), 50);
    });
}

#[test]
fn clear_budget() {
    Driver::new(settings::Config::default()).with_scope(
        |scope| {
            let (_, mut context, mut control) = scope.split();
            for (extra, expected) in [(0, queue::Drain::Empty), (1, queue::Drain::Exhausted)] {
                let turn = control.begin();
                let work = turn.turn().app();
                let budget = work.remaining();
                let token = context.region_token();
                let plan = fair::Plan::new(budget + extra, 1).expect("queue layout");
                let arena = queue::Arena::try_unweighted(plan, token).expect("queue storage");
                let lane = arena.lane(0).expect("queue lane");
                for value in 0..budget + extra {
                    lane.try_push(token, value, ()).expect("queue entry");
                }
                let mut released = 0;
                assert!(lane.clear(work, token, |_| released += 1) == expected);
                assert_eq!(released, budget);
                assert_eq!(lane.pop_front(token), (extra == 1).then_some(budget));
            }
        },
        recovery::Policy::new(
            |event| panic!("unexpected reclamation: {event:?}"),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("driver recovery: {:?}", failure.cause());
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(dope_test::scope::Error::cause)
                .expect("driver lifecycle");
        },
    );
}

#[test]
fn deadline_expires_at_clock_limit() {
    use cartel_core::deadline::{Elapsed, RequestPhase, Source};
    use dope::core::driver::{route, schedule::clock};
    use dope_fiber::context::Context;
    use o3::base::time::Positive;
    use std::{pin::pin, task::Poll, time::Duration};

    Driver::new(settings::Config::default()).with_scope(
        |scope| {
            let (_, driver, mut control) = scope.split();
            let site = driver.driver_ref();
            let target = site
                .targets::<route::KeyTag<0>>()
                .bind(route::indices::SlotIndex::ZERO, route::Epoch::INITIAL);
            let ready = site
                .ready()
                .make_slot(target.dispatch())
                .expect("ready slot");
            let mut guard = pin!(
                Source::new(site.timer(), Positive::from_nanos::<1>())
                    .guard()
                    .expect("deadline slot")
            );
            let last = site.clock().now().clamp_add(Duration::MAX).into_raw();
            let turn = control.begin_at(clock::Sample::new(last));
            Context::with_target(ready.target(), turn.turn().app(), driver, |context| {
                assert_eq!(
                    guard
                        .as_mut()
                        .poll_expired(context, RequestPhase::Admission),
                    Poll::Ready(Elapsed::new(RequestPhase::Admission))
                );
            });
        },
        recovery::Policy::new(
            |event| panic!("unexpected reclamation: {event:?}"),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("driver recovery: {:?}", failure.cause());
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(dope_test::scope::Error::cause)
                .expect("driver lifecycle");
        },
    );
}

#[test]
fn guards_restore_credit() {
    region::Token::scope(|mut token| {
        let config = (fair::Plan::new(2, 2)).expect("two slots must fit two lanes");
        let arena = (queue::Arena::try_new(config, &token)).expect("the queue must allocate");
        let lane = (arena.lane(1)).expect("the configured lane must exist");

        assert!(
            lane.try_push(&mut token, 11, usize::MAX).is_ok(),
            "queue must preserve the admitted value"
        );
        assert!(
            lane.try_push(&mut token, 12, 1)
                == Err(queue::Rejected {
                    value: 12,
                    cause: queue::Rejection::Weight {
                        lane: 1,
                        cause: Cause::Overflow {
                            axis: Axis::at::<0>(),
                            held: usize::MAX,
                            requested: 1,
                        },
                    },
                })
        );
        assert_eq!(lane.pop_front(&mut token), Some(11));
        assert!(
            lane.try_push(&mut token, 12, 9).is_ok(),
            "queue must preserve the admitted value"
        );
        assert_eq!(lane.pop_front(&mut token), Some(12));
    });
}
