use cartel_redis::{client::nonempty::Error, client::nonempty::Slice};

#[test]
fn dynamic_is_checked() {
    assert!(matches!(Slice::<u8>::new(&[]), Err(Error)));
    let input = [1, 2, 3];
    let values = (Slice::new(&input)).expect("nonempty input must be accepted");
    assert_eq!(values.as_slice(), input.as_slice());
}

#[test]
fn fixed_are_infallible() {
    let value = 1;
    assert_eq!(Slice::from_ref(&value).as_slice(), &[value]);

    let pair = [1, 2];
    assert_eq!(Slice::from_pair(&pair).as_slice(), &pair);
}
