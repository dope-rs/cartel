use cartel_pg::Jsonb;

#[test]
fn reuses_string_storage() {
    let mut source = format!("\"{}\"", "x".repeat(1024));
    let ptr = source.as_ptr();

    let json = Jsonb::try_from(&mut source).expect("owned JSON storage must freeze");

    assert_eq!(json.as_bytes().as_ptr(), ptr);
}
