pub mod common;

#[test]
fn preserves_subquery_distinct() {
    use cartel_pg::schema::TypedQuery;
    use common::types::{ProjectionCteDistinct, ProjectionLateralDistinct};

    assert_eq!(
        ProjectionCteDistinct::SQL,
        "WITH \"selected\" AS (SELECT DISTINCT \"id\",\"name\",\"age\" FROM \"users\" AS q0_0 WHERE q0_0.\"id\" >= $1) SELECT \"id\",\"name\",\"age\" FROM \"selected\" AS q0_0"
    );
    assert_eq!(
        ProjectionLateralDistinct::SQL,
        "SELECT q0_0.\"id\",q0_0.\"name\",q0_0.\"age\",q0_1.\"id\",q0_1.\"name\",q0_1.\"age\" FROM \"users\" AS q0_0 INNER JOIN LATERAL (SELECT DISTINCT ON (((q1_0.\"age\" + q0_0.\"age\") + $1)) q1_0.\"id\",q1_0.\"name\",q1_0.\"age\" FROM \"users\" AS q1_0 WHERE q1_0.\"id\" > q0_0.\"id\") q0_1 ON true"
    );
    assert_eq!(ProjectionLateralDistinct::PARAM_OIDS, &[23]);
}

#[test]
fn preserves_set_receiver_order() {
    use cartel_pg::schema::TypedQuery;
    use common::types::{ProjectionMixedSet, User};

    let select = format!(
        "SELECT {} FROM \"users\" AS q0_0 WHERE q0_0.\"id\" = ",
        User::__CARTEL_SELECT_COLS
    );
    assert_eq!(
        ProjectionMixedSet::SQL,
        format!(
            "((({select}1) UNION ({select}2)) INTERSECT ({select}2)) EXCEPT ({select}4) ORDER BY \"id\""
        )
    );
}

#[test]
fn preserves_row_locks() {
    use cartel_pg::schema::TypedQuery;
    use common::external::search::{OrderLocked, OrderShareLock};

    assert_eq!(
        OrderLocked::SQL,
        "SELECT q0_0.\"id\" FROM \"cartel_pg_users\" AS q0_0 WHERE q0_0.\"id\" <= 2 ORDER BY q0_0.\"id\" FOR UPDATE"
    );
    assert_eq!(
        OrderShareLock::SQL,
        "SELECT q0_0.\"id\" FROM \"cartel_pg_users\" AS q0_0 WHERE q0_0.\"id\" <= 2 ORDER BY q0_0.\"id\" FOR SHARE"
    );
}

#[test]
fn quotes_batch_columns() {
    use cartel_pg::schema::TypedQuery;
    use common::types::ProjectionInsertBatch;

    assert_eq!(
        ProjectionInsertBatch::SQL,
        "INSERT INTO \"select\" AS q0_0 (\"select\") SELECT * FROM UNNEST($1)"
    );
}

#[test]
fn preserves_borrowed_text() {
    use cartel_pg::schema::TypedQuery;
    use common::types::{ProjectionRenameOwned, ProjectionRenameRef};

    assert_eq!(ProjectionRenameRef::SQL, ProjectionRenameOwned::SQL);
    assert_eq!(
        ProjectionRenameRef::PARAM_OIDS,
        ProjectionRenameOwned::PARAM_OIDS
    );
}

#[test]
fn preserves_result_formats() {
    use cartel_pg::schema::TypedQuery;
    use common::types::{
        ProjectionBinary, ProjectionMaybePath, ProjectionMixedFields, ProjectionMixedRows,
        ProjectionScalarPath, ProjectionSelfJoin, ProjectionTimestamp,
    };

    assert_eq!(ProjectionMixedRows::N_RESULT_COLS, 11);
    assert_eq!(
        ProjectionMixedRows::RESULT_FORMAT_CODES,
        &[1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1]
    );
    assert_eq!(ProjectionMixedFields::RESULT_FORMAT_CODES, &[1, 0, 0, 1]);
    assert_eq!(ProjectionBinary::RESULT_FORMAT_CODES, &[1]);
    assert_eq!(ProjectionSelfJoin::RESULT_FORMAT_CODES, &[1]);
    assert_eq!(ProjectionScalarPath::RESULT_FORMAT_CODES, &[0]);
    assert_eq!(ProjectionMaybePath::RESULT_FORMAT_CODES, &[0]);
    assert_eq!(ProjectionTimestamp::RESULT_FORMAT_CODES, &[1]);
}

#[test]
fn preserves_sources() {
    use cartel_pg::schema::TypedQuery;
    use common::types::{
        ProjectionAliased, ProjectionLateral, ProjectionNested, ProjectionQualified,
        ProjectionReordered, ProjectionSameTable, ProjectionSelfJoin, ProjectionSubset,
    };

    assert_eq!(ProjectionAliased::SQL, ProjectionSubset::SQL);
    assert_eq!(ProjectionQualified::SQL, ProjectionReordered::SQL);

    assert_eq!(
        ProjectionSameTable::SQL,
        "SELECT q0_0.\"id\",q0_0.\"name\",q0_0.\"age\",q0_1.\"id\" FROM \"users\" AS q0_0 INNER JOIN \"users\" AS q0_1 ON q0_0.\"id\" = q0_1.\"id\""
    );
    assert!(ProjectionSelfJoin::SQL.starts_with(
        "SELECT q0_0.\"id\",q0_0.\"name\",q0_0.\"age\",q0_1.\"id\",q0_1.\"name\",q0_1.\"age\" FROM "
    ));
    assert!(
        ProjectionSubset::SQL.starts_with("SELECT q0_0.\"id\",q0_0.\"name\",q0_0.\"age\" FROM ")
    );
    assert!(ProjectionReordered::SQL.starts_with(
        "SELECT q0_1.\"id\",q0_1.\"author_id\",q0_0.\"id\",q0_0.\"name\",q0_0.\"age\" FROM "
    ));
    assert!(ProjectionNested::SQL.contains("q1_0.\"id\" > q0_0.\"id\""));
    assert!(ProjectionNested::SQL.contains("q2_0.\"id\" > q1_0.\"id\""));
    assert!(ProjectionLateral::SQL.contains("q1_0.\"id\" > q0_0.\"id\" FOR SHARE) q0_1 ON true"));
    assert_eq!(
        common::external::row::LtreeNodeDescendants::SQL,
        "SELECT q0_0.\"id\" FROM \"cartel_pg_nodes\" AS q0_0 WHERE q0_0.\"path\" <@ $1::ltree::ltree ORDER BY q0_0.\"id\""
    );
    assert_eq!(
        common::external::row::LtreeNodeAncestors::SQL,
        "SELECT q0_0.\"id\" FROM \"cartel_pg_nodes\" AS q0_0 WHERE q0_0.\"path\" @> $1::ltree::ltree ORDER BY q0_0.\"id\""
    );
}

#[test]
fn preserves_shorthand() {
    use cartel_pg::schema::TypedQuery;
    use common::types::{
        ProjectionCteLegacy, ProjectionCteShort, ProjectionGroupLegacy, ProjectionGroupShort,
        ProjectionLegacy, ProjectionShorthand,
    };

    assert_eq!(
        (
            ProjectionLegacy::SQL,
            ProjectionLegacy::N_PARAMS,
            ProjectionLegacy::PARAM_OIDS,
            ProjectionLegacy::N_RESULT_COLS
        ),
        (
            ProjectionShorthand::SQL,
            ProjectionShorthand::N_PARAMS,
            ProjectionShorthand::PARAM_OIDS,
            ProjectionShorthand::N_RESULT_COLS
        ),
    );
    assert!(ProjectionLegacy::SQL.contains("WHERE $1 <= q0_0.\"id\""));
    assert_eq!(
        (
            ProjectionGroupLegacy::SQL,
            ProjectionGroupLegacy::PARAM_OIDS,
            ProjectionGroupLegacy::N_RESULT_COLS
        ),
        (
            ProjectionGroupShort::SQL,
            ProjectionGroupShort::PARAM_OIDS,
            ProjectionGroupShort::N_RESULT_COLS
        ),
    );
    assert_eq!(
        (
            ProjectionCteLegacy::SQL,
            ProjectionCteLegacy::PARAM_OIDS,
            ProjectionCteLegacy::N_RESULT_COLS
        ),
        (
            ProjectionCteShort::SQL,
            ProjectionCteShort::PARAM_OIDS,
            ProjectionCteShort::N_RESULT_COLS
        ),
    );
    assert!(ProjectionCteShort::SQL.contains("FOR UPDATE) SELECT"));
}
