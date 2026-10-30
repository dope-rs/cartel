use cartel_pg::{
    protocol::encode, protocol::value::encode::BindWriter, schema, schema::groups, schema::result,
};

pub struct NegParams;

pub struct ParamCount;

pub struct BadFormats;

pub struct BadName;

pub struct EmptyName;

pub struct ReservedName;

pub struct TooManyCols;

pub struct LtreeQuery;

pub struct Group;

struct Set;

impl schema::TypedQuery for NegParams {
    type Params<'p> = ();
    type Row = ();
    type Result = result::Command;
    type Group = Group;
    const STATEMENT_NAME: &str = "test";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = -1;
    const N_RESULT_COLS: u16 = 0;
    const PARAM_FORMAT_CODES: &[u16] = &[1];
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::TypedQuery for ParamCount {
    type Params<'p> = ();
    type Row = ();
    type Result = result::Command;
    type Group = Group;
    const STATEMENT_NAME: &str = "test";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 1;
    const N_RESULT_COLS: u16 = 0;
    const PARAM_FORMAT_CODES: &[u16] = &[1];
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::TypedQuery for BadFormats {
    type Params<'p> = ();
    type Row = ();
    type Result = result::Command;
    type Group = Group;
    const STATEMENT_NAME: &str = "test";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 0;
    const PARAM_FORMAT_CODES: &[u16] = &[2];
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::TypedQuery for BadName {
    type Params<'p> = ();
    type Row = ();
    type Result = result::Command;
    type Group = Group;
    const STATEMENT_NAME: &str = "bad\0name";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 0;
    const PARAM_FORMAT_CODES: &[u16] = &[1];
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::TypedQuery for EmptyName {
    type Params<'p> = ();
    type Row = ();
    type Result = result::Command;
    type Group = Group;
    const STATEMENT_NAME: &str = "";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 0;
    const PARAM_FORMAT_CODES: &[u16] = &[1];
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::TypedQuery for ReservedName {
    type Params<'p> = ();
    type Row = ();
    type Result = result::Command;
    type Group = Group;
    const STATEMENT_NAME: &str = "__cartel_type_ltree";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 0;
    const PARAM_FORMAT_CODES: &[u16] = &[1];
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::TypedQuery for TooManyCols {
    type Params<'p> = ();
    type Row = ();
    type Result = result::Rows;
    type Group = Group;
    const STATEMENT_NAME: &str = "test";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = u16::MAX;
    const PARAM_FORMAT_CODES: &[u16] = &[1];
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

impl schema::TypedQuery for LtreeQuery {
    type Params<'p> = ();
    type Row = cartel_pg::Ltree;
    type Result = result::Rows;
    type Group = Group;
    const STATEMENT_NAME: &str = "test";
    const SQL: &str = "SELECT 1";
    const PARAM_OIDS: &[u32] = &[];
    const N_PARAMS: i16 = 0;
    const N_RESULT_COLS: u16 = 1;
    const PARAM_FORMAT_CODES: &[u16] = &[1];
    fn encode(_: (), _: &mut BindWriter<'_, '_>) -> Result<(), encode::Error> {
        Ok(())
    }
}

const LTREE_META: schema::QueryMeta<'_> = schema::QueryMeta::fixed::<LtreeQuery>();

const LTREE_QUERIES: &[schema::QueryMeta<'_>] = &[LTREE_META];

pub const LTREE_GROUPS: &[&[schema::QueryMeta<'_>]] = &[LTREE_QUERIES];

pub const LTREE_LEN: usize = schema::Prepare::fixed_len::<Set>();

const LTREE_BYTES: schema::Wire<'_, LTREE_LEN> = schema::Prepare::fixed::<Set, LTREE_LEN>();

pub const LTREE: schema::Prepare<'_> = LTREE_BYTES.prepare();

const DUP_QUERIES: &[schema::QueryMeta<'_>] = &[LTREE_META, LTREE_META];

pub const DUP_GROUPS: &[&[schema::QueryMeta<'_>]] = &[DUP_QUERIES];

impl schema::QueryGroup for Group {
    type Items = groups::Item<LtreeQuery, groups::Empty>;

    const QUERIES: &[schema::QueryMeta<'_>] = LTREE_QUERIES;
}

impl schema::QuerySet for Set {
    type Groups = groups::Cons<Group, groups::End>;

    const QUERIES: &[&[schema::QueryMeta<'_>]] = LTREE_GROUPS;
    const PREPARE: schema::Prepare<'_> = LTREE;
}
