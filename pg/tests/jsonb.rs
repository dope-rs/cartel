use cartel_pg::{Identifier, Jsonb, Ltree, Text};
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn reuses_string_storage() {
    let mut sources: [String; 4] = std::array::from_fn(|_| {
        let mut source = String::with_capacity(1024);
        source.push_str("branch");
        source
    });
    let pointers = sources.each_ref().map(|source| source.as_ptr());
    let bytes = sources.iter().map(String::capacity).sum::<usize>();
    let [identifier, text, json, tree] = &mut sources;
    let ((identifier, text, json, tree), counts) = Tracker::measure(|| {
        (
            Identifier::try_from(identifier).expect("identifier transfer"),
            Text::try_from(text).expect("text transfer"),
            Jsonb::try_from(json).expect("JSON transfer"),
            Ltree::try_from(tree).expect("ltree transfer"),
        )
    });
    assert_eq!(counts, [0; 4]);
    let views: [&str; 4] = [&identifier, &text, &json, &tree];
    for (view, pointer) in views.into_iter().zip(pointers) {
        assert_eq!(view, "branch");
        assert_eq!(view.as_ptr(), pointer);
    }
    assert!(
        sources
            .iter()
            .all(|source| source.is_empty() && source.capacity() == 0)
    );
    let ((), counts) = Tracker::measure(|| drop((identifier, text, json, tree)));
    assert_eq!(counts, [0, 0, 4, bytes]);

    let mut source = String::with_capacity(1024);
    let bytes = source.capacity();
    let (text, counts) = Tracker::measure(|| Text::try_from(&mut source));
    let text = text.expect("empty text transfer");
    assert!(text.is_empty() && source.is_empty() && source.capacity() == 0);
    assert_eq!(counts, [0, 0, 1, bytes]);
    let ((), counts) = Tracker::measure(|| drop(text));
    assert_eq!(counts, [0; 4]);
}
