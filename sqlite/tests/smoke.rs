use cartel_sqlite::{Connection, SqliteTable, sqlite_query};

#[derive(SqliteTable)]
#[table_name("users")]
struct User {
    #[pk]
    id: i64,
    name: String,
}

#[sqlite_query]
fn user_by_id(id: i64) -> User {
    User::filter(|u| u.id == id).one()
}

#[sqlite_query]
fn user_names(min_id: i64) -> Vec<String> {
    User::filter(|u| u.id >= min_id).select(|u| u.name).all()
}

#[sqlite_query]
fn maybe_user(id: i64) -> Option<User> {
    User::filter(|u| u.id == id).first()
}

#[sqlite_query]
fn rename(id: i64, name: String) {
    User::filter(|u| u.id == id).update(|u| u.name = name)
}

#[test]
fn round_trip() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    conn.execute(
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')",
        [],
    )
    .unwrap();

    let u = user_by_id(&conn, 1).unwrap();
    assert_eq!(u.id, 1);
    assert_eq!(u.name, "alice");

    let names = user_names(&conn, 1).unwrap();
    assert_eq!(names, vec!["alice".to_string(), "bob".to_string()]);

    assert!(maybe_user(&conn, 99).unwrap().is_none());

    rename(&conn, 2, "carol".into()).unwrap();
    let u = user_by_id(&conn, 2).unwrap();
    assert_eq!(u.name, "carol");
}
