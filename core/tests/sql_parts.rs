use cartel_core::sql::{self, raw::Parts};
use std::mem;

type Function = fn(u8) -> u8;

const _: fn(sql::Target<Function>) = |target| sql::Assign::assign(target, |value| value);
const _: for<'a> fn(sql::Target<&'a [u8]>, &'a mut [u8; 2]) =
    |target, value| sql::Assign::assign(target, value);
const _: fn(sql::Target<u8>) = |target| sql::Assign::assign(target, 0);
const _: fn(sql::Target<String>, &str) = |target, value| sql::Assign::assign(target, value);

#[test]
fn layout() {
    assert_eq!(
        mem::size_of::<Parts<'static>>(),
        mem::size_of::<&'static [&'static str]>()
    );
}

#[test]
fn fragments_match() {
    const { assert!(Parts::new(&["α", "", "β"]).matches("αβ")) };
    assert!(Parts::new(&[]).matches(""));
    assert!(!Parts::new(&[]).matches("a"));
    assert!(!Parts::new(&["a"]).matches("ab"));
    assert!(!Parts::new(&["ab", "c"]).matches("a"));
    assert!(!Parts::new(&["a", "c"]).matches("ab"));
}
