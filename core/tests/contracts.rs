mod common;

use std::cell;

use cartel_core::{
    frame::{Buffer, Sensitivity},
    handoff, queue, reply,
    reply::{layout, recv, recv::attach::Attach as _, usage},
};
use common::Pending;
use dope::net::link::egress;
use o3::{
    base::cell::region,
    buffer::pool,
    depot::queue::pool::fair,
    mem::credit::fair::{Axis, Cause},
};

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
            let mut cursor = (pool.cursor()).expect("the frame pool has one slot");
            origin = cursor.as_ref().as_ptr();
            cursor.try_append(bytes).expect("the frame fits").commit();
            let frame = Buffer::new(cursor, kind);
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
            drop(
                handoff::Lane::new(&arena, replies)
                    .try_prepare(token, 5)
                    .expect("request reservation"),
            );
            assert_eq!(replies.depth(token).get(), 0);
            let mut first = recv::Response::<_, Pending>::new();
            let success = handoff::Lane::new(&arena, replies)
                .try_prepare(token, 7)
                .expect("request reservation")
                .try_with(
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
        reply::Arena::<u8>::try_with(replies, token.proof(), |replies| {
            let config = fair::Plan::new(1, 1).expect("queue layout");
            let mismatch = queue::Arena::<()>::try_new(config, &token)
                .expect("queue storage")
                .try_bind(replies.domain())
                .err()
                .expect("different lane extent");
            assert_eq!((mismatch.expected.get(), mismatch.actual), (2, 1));

            let config = fair::Plan::new(2, 2).expect("queue layout");
            let requests = queue::Arena::try_new(config, &token)
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
                    .try_publish(&mut token, 3, &mut response, (), 7, |item| (item, ()))
                    .is_ok()
            );
            assert!(first.is_empty(&token));
            assert_eq!(second.pop_front(&mut token), Some(7));
        })
        .expect("reply storage");
    });
}

#[test]
fn preserve_peer_capacity() {
    region::Token::scope(|mut token| {
        let config = (fair::Plan::new(4, 2)).expect("four slots must fit two lanes");
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
            left.try_push(&mut token, 3, 30).is_ok(),
            "queue must preserve the admitted value"
        );
        assert!(
            left.try_push(&mut token, 4, 40)
                == Err(queue::Rejected {
                    value: 4,
                    cause: queue::Rejection::Capacity(Cause::Exhausted {
                        axis: Axis::at::<0>(),
                        required: 1,
                        available: 0,
                    }),
                })
        );
        assert!(
            right.try_push(&mut token, 5, 50).is_ok(),
            "queue must preserve the admitted value"
        );
        assert_eq!(left.weight(&token), 60);
        assert_eq!(right.weight(&token), 50);
    });
}

#[test]
fn guards_restore_credit() {
    for index in 0..3 {
        region::Token::scope(|mut token| {
            let config = (fair::Plan::new(4, 3)).expect("four slots must fit three lanes");
            let arena = (queue::Arena::try_new(config, &token)).expect("the queue must allocate");
            let lane = (arena.lane(index)).expect("the configured lane must exist");

            drop(
                lane.try_reserve(&mut token, 7)
                    .expect("queue credit must be reserved"),
            );
            assert_eq!(lane.len(&token), 0);
            assert_eq!(lane.weight(&token), 0);

            assert!(
                lane.try_push(&mut token, 11, usize::MAX).is_ok(),
                "queue must preserve the admitted value"
            );
            assert!(
                lane.try_push(&mut token, 12, 1)
                    == Err(queue::Rejected {
                        value: 12,
                        cause: queue::Rejection::Weight {
                            lane: index,
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
}
