use cartel_pg::{Config, ConfigError, ConfigField};
use dope_test::alloc::Tracker;
use o3::alloc;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn password_failures() {
    for (value, allocations) in [("\u{c0}", 1), ("\u{2168}", 2)] {
        for allows in 0..=allocations {
            let user = "user".to_owned();
            let password = value.repeat(128);
            let database = "database".to_owned();
            let (error, activity) = Tracker::activity(|| {
                Tracker::fail_after(allows, || Config::new(user, password, database)).err()
            });
            let expected =
                (allows < allocations).then_some(ConfigError::Allocation(alloc::Error::Exhausted));
            assert_eq!(error, expected);
            assert_eq!(activity.allocations, allows);
            // Both decompose to 256 ordered chars; only À can reuse its exact UTF-8 capacity.
            assert_eq!(activity.allocated_bytes, [0, 1024, 1280][allows]);
            assert_eq!(activity.deallocations, allows + 3);
        }
    }

    let user = "user".to_owned();
    let password = "\u{c0}".to_owned();
    let database = "db\0".to_owned();
    let result = Tracker::fail_after(0, || Config::new(user, password, database));
    assert_eq!(
        result.err(),
        Some(ConfigError::Nul {
            field: ConfigField::Database,
            position: 2,
        })
    );
}

#[test]
fn password_fallback_does_not_allocate() {
    for password in ["\u{ad}", "A\u{0340}", "\u{1d2c}", "\u{0627}a\u{0627}"] {
        let user = "user".to_owned();
        let password = password.to_owned();
        let database = "database".to_owned();
        let (error, count) = Tracker::count(|| Config::new(user, password, database).err());
        assert_eq!((error, count), (None, 0));
    }
}
