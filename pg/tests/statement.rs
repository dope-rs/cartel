use cartel_pg::{
    api::sql::Error,
    api::sql::Timeout,
    protocol::{self, Reason},
    schema::{self, desc},
};
use common::wire::{checked, prepare::*};
use std::time::Duration;

pub mod common;

#[test]
fn metadata_rejects() {
    assert!(matches!(
        schema::QueryMeta::try_new::<NegParams>(),
        Err(protocol::encode::Error::ParameterCount {
            declared: -1,
            types: 0
        })
    ));
    assert!(matches!(
        schema::QueryMeta::try_new::<ParamCount>(),
        Err(protocol::encode::Error::ParameterCount {
            declared: 1,
            types: 0
        })
    ));
    assert!(matches!(
        schema::QueryMeta::try_new::<BadFormats>(),
        Err(protocol::encode::Error::ParameterFormat {
            index: 0,
            actual: 2
        })
    ));
    assert!(matches!(
        schema::QueryMeta::try_new::<BadName>(),
        Err(protocol::encode::Error::StatementNameNul { position: 3 })
    ));
    assert!(matches!(
        schema::QueryMeta::try_new::<EmptyName>(),
        Err(protocol::encode::Error::StatementNameEmpty)
    ));
    assert!(matches!(
        schema::QueryMeta::try_new::<ReservedName>(),
        Err(protocol::encode::Error::StatementNameReserved)
    ));
    assert!(matches!(
        schema::QueryMeta::try_new::<TooManyCols>(),
        Err(protocol::encode::Error::ResultCount { declared: u16::MAX })
    ));
}

#[test]
fn probe_is_counted() {
    assert_eq!(LTREE.statements(), 2);
    assert_eq!(schema::Prepare::try_wire_len(LTREE_GROUPS), Ok(LTREE_LEN));
    let wire = (schema::Prepare::try_build::<LTREE_LEN>(LTREE_GROUPS))
        .expect("the validated ltree query must encode");
    assert_eq!(wire.prepare().wire(), LTREE.wire());
    assert_eq!(
        LTREE.wire(),
        b"P\0\0\0-__cartel_type_ltree\0SELECT NULL::ltree\0\0\0\
          D\0\0\0\x19S__cartel_type_ltree\0\
          P\0\0\0\x14test\0SELECT 1\0\0\0D\0\0\0\nStest\0S\0\0\0\x04"
    );
}

#[test]
fn ltree_absent() {
    let payload = checked::description(&[42]);
    assert!(matches!(
        desc::Types::query::<LtreeQuery>(&payload, desc::Registry::default()),
        Err(cartel_pg::Error::Protocol(Reason::LtreeMissing))
    ));
}

#[test]
fn duplicate_name_rejects() {
    assert!(matches!(
        schema::Prepare::try_build::<0>(DUP_GROUPS),
        Err(protocol::encode::Error::DuplicateStatement {
            first: protocol::encode::Position { group: 0, query: 0 },
            second: protocol::encode::Position { group: 0, query: 1 },
        })
    ));
}

#[test]
fn validates_timeout() {
    assert!(Timeout::new(Duration::ZERO).expect("zero disables timeout") == Timeout::DISABLED);
    let short = Duration::from_micros(999);
    assert_eq!(Timeout::new(short).err(), Some(Error::Short(short)));
    assert!(matches!(
        Timeout::new(Duration::from_millis(1)),
        Ok(timeout) if timeout.milliseconds() == 1
    ));
    assert!(matches!(
        Timeout::new(Duration::from_millis(i32::MAX as u64)),
        Ok(timeout) if timeout.milliseconds() == i32::MAX as u32
    ));
    let long = Duration::from_millis(i32::MAX as u64 + 1);
    assert_eq!(Timeout::new(long).err(), Some(Error::Long(long)));
}

#[test]
fn identifier_nul() {
    let mut source = String::from("safe\0truncated");
    let original = (source.as_ptr(), source.capacity());
    assert!(matches!(
        cartel_pg::Identifier::try_from(&mut source),
        Err(cartel_pg::ParseError::Nul { position: 4 })
    ));
    assert_eq!(source, "safe\0truncated");
    assert_eq!((source.as_ptr(), source.capacity()), original);
}

#[test]
fn identifier_empty() {
    let mut source = String::with_capacity(64);
    let original = (source.as_ptr(), source.capacity());
    assert!(matches!(
        cartel_pg::Identifier::try_from(&mut source),
        Err(cartel_pg::ParseError::Empty)
    ));
    assert!(source.is_empty());
    assert_eq!((source.as_ptr(), source.capacity()), original);
}

#[test]
fn config_nul() {
    assert!(matches!(
        cartel_pg::Config::new(
            String::from("user"),
            String::from("secret"),
            String::from("data\0base"),
        ),
        Err(cartel_pg::ConfigError::Nul {
            field: cartel_pg::ConfigField::Database,
            position: 4,
        })
    ));
}
