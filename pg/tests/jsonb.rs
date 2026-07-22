use cartel_pg::Jsonb;

#[test]
fn owned_json_reuses_large_string_storage() {
    let source = format!("\"{}\"", "x".repeat(1024));
    let ptr = source.as_ptr();

    let json = Jsonb::from_string(source);

    assert_eq!(json.as_bytes().as_ptr(), ptr);
}
