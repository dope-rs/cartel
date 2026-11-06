use cartel_core::sql::raw::Parts;

#[test]
fn fragments_match() {
    assert!(Parts::new(&["α", "", "β"]).matches("αβ"));
    assert!(Parts::new(&[]).matches(""));
    assert!(!Parts::new(&[]).matches("a"));
    assert!(!Parts::new(&["a"]).matches("ab"));
    assert!(!Parts::new(&["ab", "c"]).matches("a"));
    assert!(!Parts::new(&["a", "c"]).matches("ab"));
}
