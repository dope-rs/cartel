use cartel_core::QueueArena;
use o3::cell::RegionToken;

#[test]
fn lanes_share_capacity_without_spending_another_lanes_last_slot() {
    RegionToken::scope(|mut token| {
        let arena = QueueArena::with_capacity(4, 2);
        let left = arena.lane(0);
        let right = arena.lane(1);

        assert!(left.try_push(&mut token, 1, 10).is_ok());
        assert!(left.try_push(&mut token, 2, 20).is_ok());
        assert!(left.try_push(&mut token, 3, 30).is_ok());
        assert_eq!(left.try_push(&mut token, 4, 40), Err(4));
        assert!(right.try_push(&mut token, 5, 50).is_ok());
        assert_eq!(left.weight(&token), 60);
        assert_eq!(right.weight(&token), 50);
    });
}

#[test]
fn rejected_drain_item_is_restored_at_the_front() {
    RegionToken::scope(|mut token| {
        let arena = QueueArena::with_capacity(2, 1);
        let queue = arena.lane(0);
        queue.try_push(&mut token, 1, 10).unwrap();
        queue.try_push(&mut token, 2, 20).unwrap();

        queue.drain(&mut token, Err);
        assert_eq!(queue.len(&token), 2);
        assert_eq!(queue.weight(&token), 30);
        assert_eq!(queue.pop_front(&mut token), Some(1));
        assert_eq!(queue.pop_front(&mut token), Some(2));
    });
}
