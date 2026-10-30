pub use crate::sql::tokens::*;

pub const DISTINCT_ON_OPEN: &str = "DISTINCT ON (";
pub const DISTINCT_ON_CLOSE: &str = ") ";

pub const FOR_UPDATE: &str = " FOR UPDATE";
pub const FOR_SHARE: &str = " FOR SHARE";

pub const INNER_JOIN_LATERAL: &str = " INNER JOIN LATERAL ";
pub const LEFT_JOIN_LATERAL: &str = " LEFT JOIN LATERAL ";
pub const ON_TRUE: &str = " ON true";

pub const ILIKE: &str = " ILIKE ";
pub const NOT_ILIKE: &str = " NOT ILIKE ";
pub const CONTAINS: &str = " @> ";
pub const OVERLAPS: &str = " && ";

pub const LTREE_DESC_OF: &str = " <@ ";
pub const LTREE_ANC_OF: &str = " @> ";
pub const CAST_LTREE_SUFFIX: &str = "::ltree";

pub const EQ_ANY_OPEN: &str = " = ANY(";
pub const NE_ALL_OPEN: &str = " <> ALL(";

pub const COUNT_OVER: &str = "COUNT(";
pub const SUM_OVER: &str = "SUM(";
pub const AVG_OVER: &str = "AVG(";
pub const MIN_OVER: &str = "MIN(";
pub const MAX_OVER: &str = "MAX(";

pub const UNNEST_OPEN: &str = ") SELECT * FROM UNNEST(";
pub const USING_KW: &str = " USING ";

pub const INTERSECT_ALL_KW: &str = " INTERSECT ALL ";
pub const EXCEPT_ALL_KW: &str = " EXCEPT ALL ";

pub const SETOP_BRANCH_OPEN: &str = "(SELECT ";
pub const SETOP_BRANCH_CLOSE: &str = ")";

pub const JSON_PATH: &str = " #> ";
pub const JSON_PATH_TEXT: &str = " #>> ";

pub const REGEX_MATCH: &str = " ~ ";
pub const REGEX_IMATCH: &str = " ~* ";
pub const NOT_REGEX_MATCH: &str = " !~ ";
pub const NOT_REGEX_IMATCH: &str = " !~* ";

pub const FTS_MATCH: &str = " @@ ";

pub const UNNEST_FROM_OPEN: &str = " FROM unnest(";
pub const UNNEST_ALIAS_OPEN: &str = ") AS __d(";

pub const TRUE: &str = "TRUE";
pub const FALSE: &str = "FALSE";
