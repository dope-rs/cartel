use cartel_sqlite::Catalog as _;
use common::engine::mutation::*;

pub mod common;

#[test]
fn preserves_commands() {
    let entries = Commands::STATEMENTS;
    assert_eq!(entries.len(), 6);
    assert_eq!(
        entries[0].sql(),
        "INSERT INTO \"items\" AS q0_0 (\"id\",\"select\") VALUES (?1,?2)"
    );
    assert_eq!((entries[0].parameters(), entries[0].columns()), (2, 0));
    assert!(
        entries[1]
            .sql()
            .starts_with("UPDATE \"items\" AS q0_0 SET \"select\" = ")
    );
    assert_eq!((entries[1].parameters(), entries[1].columns()), (2, 0));
    assert!(
        entries[2]
            .sql()
            .starts_with("DELETE FROM \"items\" AS q0_0 WHERE ")
    );
    assert_eq!((entries[2].parameters(), entries[2].columns()), (1, 0));
}

#[test]
fn retains_qualified_table() {
    let entry = Commands::STATEMENTS[3];
    assert_eq!(
        entry.sql(),
        "INSERT INTO \"items\" AS q0_0 (\"id\",\"select\") VALUES (?1,?2) RETURNING \"id\",\"select\""
    );
    assert_eq!((entry.parameters(), entry.columns()), (2, 2));
}

#[test]
fn preserves_shorthand() {
    let legacy = Commands::STATEMENTS[4];
    let short = Commands::STATEMENTS[5];
    assert_eq!(
        (legacy.sql(), legacy.parameters(), legacy.columns()),
        (short.sql(), short.parameters(), short.columns()),
    );
}
