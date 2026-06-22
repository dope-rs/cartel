pub const SELECT_KW: &str = "SELECT ";
pub const FROM_KW: &str = " FROM ";
pub const WHERE_KW: &str = " WHERE ";
pub const ORDER_BY_KW: &str = " ORDER BY ";
pub const GROUP_BY_KW: &str = " GROUP BY ";
pub const HAVING_KW: &str = " HAVING ";
pub const LIMIT_KW: &str = " LIMIT ";
pub const OFFSET_KW: &str = " OFFSET ";

pub const ASC: &str = " ASC";
pub const DESC: &str = " DESC";

pub const DISTINCT_KW: &str = "DISTINCT ";
pub const DISTINCT_ON_OPEN: &str = "DISTINCT ON (";
pub const DISTINCT_ON_CLOSE: &str = ") ";

pub const FOR_UPDATE: &str = " FOR UPDATE";
pub const FOR_SHARE: &str = " FOR SHARE";

pub const INNER_JOIN: &str = " INNER JOIN ";
pub const LEFT_OUTER_JOIN: &str = " LEFT OUTER JOIN ";
pub const RIGHT_OUTER_JOIN: &str = " RIGHT OUTER JOIN ";
pub const FULL_OUTER_JOIN: &str = " FULL OUTER JOIN ";
pub const INNER_JOIN_LATERAL: &str = " INNER JOIN LATERAL ";
pub const LEFT_JOIN_LATERAL: &str = " LEFT JOIN LATERAL ";
pub const ON_KW: &str = " ON ";
pub const ON_TRUE: &str = " ON true";

pub const AND_KW: &str = " AND ";
pub const OR_KW: &str = " OR ";
pub const EQ: &str = " = ";
pub const NE: &str = " <> ";
pub const LT: &str = " < ";
pub const GT: &str = " > ";
pub const LE: &str = " <= ";
pub const GE: &str = " >= ";
pub const LIKE: &str = " LIKE ";
pub const ILIKE: &str = " ILIKE ";
pub const NOT_LIKE: &str = " NOT LIKE ";
pub const NOT_ILIKE: &str = " NOT ILIKE ";
pub const CONTAINS: &str = " @> ";
pub const OVERLAPS: &str = " && ";

pub const LTREE_DESC_OF: &str = " <@ ";
pub const LTREE_ANC_OF: &str = " @> ";
pub const CAST_LTREE_SUFFIX: &str = "::ltree";

pub const IS_NULL: &str = " IS NULL";
pub const IS_NOT_NULL: &str = " IS NOT NULL";

pub const EQ_ANY_OPEN: &str = " = ANY(";
pub const NE_ALL_OPEN: &str = " <> ALL(";

pub const COUNT_STAR: &str = "COUNT(*)";
pub const COUNT_OPEN: &str = "COUNT(";
pub const SUM_OPEN: &str = "SUM(";
pub const AVG_OPEN: &str = "AVG(";
pub const MIN_OPEN: &str = "MIN(";
pub const MAX_OPEN: &str = "MAX(";

pub const ROW_NUMBER_OVER: &str = "ROW_NUMBER() OVER (";
pub const RANK_OVER: &str = "RANK() OVER (";
pub const DENSE_RANK_OVER: &str = "DENSE_RANK() OVER (";
pub const COUNT_STAR_OVER: &str = "COUNT(*) OVER (";
pub const COUNT_OVER: &str = "COUNT(";
pub const SUM_OVER: &str = "SUM(";
pub const AVG_OVER: &str = "AVG(";
pub const MIN_OVER: &str = "MIN(";
pub const MAX_OVER: &str = "MAX(";
pub const OVER_OPEN: &str = ") OVER (";
pub const PARTITION_BY: &str = "PARTITION BY ";
pub const WIN_ORDER_BY: &str = "ORDER BY ";

pub const INSERT_INTO_KW: &str = "INSERT INTO ";
pub const VALUES_OPEN: &str = ") VALUES (";
pub const UNNEST_OPEN: &str = ") SELECT * FROM UNNEST(";
pub const ON_CONFLICT_DO_NOTHING: &str = " ON CONFLICT DO NOTHING";
pub const RETURNING_KW: &str = " RETURNING ";
pub const UPDATE_KW: &str = "UPDATE ";
pub const SET_KW: &str = " SET ";
pub const DELETE_FROM_KW: &str = "DELETE FROM ";
pub const USING_KW: &str = " USING ";

pub const UNION_KW: &str = " UNION ";
pub const UNION_ALL_KW: &str = " UNION ALL ";
pub const INTERSECT_KW: &str = " INTERSECT ";
pub const INTERSECT_ALL_KW: &str = " INTERSECT ALL ";
pub const EXCEPT_KW: &str = " EXCEPT ";
pub const EXCEPT_ALL_KW: &str = " EXCEPT ALL ";

pub const WITH_KW: &str = "WITH ";
pub const AS_OPEN: &str = " AS (";

pub const EXISTS_OPEN: &str = "EXISTS (SELECT 1 FROM ";
pub const NOT_EXISTS_OPEN: &str = "NOT EXISTS (SELECT 1 FROM ";
pub const SUBQUERY_OPEN: &str = "(SELECT ";
pub const SETOP_BRANCH_OPEN: &str = "(SELECT ";
pub const SETOP_BRANCH_CLOSE: &str = ")";

pub const JSON_GET: &str = " -> ";
pub const JSON_GET_TEXT: &str = " ->> ";
pub const JSON_PATH: &str = " #> ";
pub const JSON_PATH_TEXT: &str = " #>> ";

pub const CONCAT: &str = " || ";

pub const REGEX_MATCH: &str = " ~ ";
pub const REGEX_IMATCH: &str = " ~* ";
pub const NOT_REGEX_MATCH: &str = " !~ ";
pub const NOT_REGEX_IMATCH: &str = " !~* ";

pub const FTS_MATCH: &str = " @@ ";

pub const COMMA: &str = ", ";
pub const COMMA_TIGHT: &str = ",";
pub const SPACE: &str = " ";
pub const PAREN_OPEN: &str = "(";
pub const PAREN_CLOSE: &str = ")";
pub const PAREN_OPEN_LEADING_SPACE: &str = " (";
pub const PAREN_CLOSE_SPACE: &str = ") ";

pub const AND_PAREN_WRAP: &str = ") AND (";
pub const OR_PAREN_WRAP: &str = ") OR (";

pub const INSERT_FROM_SELECT: &str = ") SELECT ";

pub const UNNEST_FROM_OPEN: &str = " FROM unnest(";
pub const UNNEST_ALIAS_OPEN: &str = ") AS __d(";

pub const LIMIT_ONE: &str = "1";

pub const TRUE: &str = "TRUE";
pub const FALSE: &str = "FALSE";
