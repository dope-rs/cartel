use crate::common::schema::Item;

type I = Item;
type S = i32;

#[cartel_sqlite::query]
#[doc = "Selects an item by its composite key."]
#[must_use]
pub fn by_key(source: i32, query: S) -> Option<I> {
    I::filter(|i| i.a == source && i.b == query).rows().first()
}

#[cartel_sqlite::query]
pub fn insert(a: i32, b: i32, payload: String) {
    I::insert(|i| {
        i.a = a;
        i.b = b;
        i.payload = payload;
    })
}
