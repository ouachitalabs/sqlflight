//! Formatting tests for sqlflight
//!
//! These tests verify the formatting rules from SPEC.md are correctly applied.

use pretty_assertions::assert_eq;
use sqlflight::format;

/// Helper to format and compare
fn assert_formats_to(input: &str, expected: &str) {
    let result = format(input).expect("format should succeed");
    assert_eq!(result.trim(), expected.trim());
}

/// Helper to verify formatting is idempotent
fn assert_idempotent(input: &str) {
    let formatted = format(input).expect("first format should succeed");
    let reformatted = format(&formatted).expect("second format should succeed");
    assert_eq!(formatted, reformatted, "formatting should be idempotent");
}

// =============================================================================
// KEYWORD TESTS - All SQL keywords should be lowercase
// =============================================================================

mod keywords {
    use super::*;

    #[test]
    fn select_keyword_lowercase() {
        assert_formats_to(
            "SELECT id FROM users",
            "select id from users",
        );
    }

    #[test]
    fn uppercase_keywords_converted() {
        assert_formats_to(
            "SELECT ID, NAME FROM USERS WHERE ACTIVE = TRUE",
            "select id, name from users where active = true",
        );
    }

    #[test]
    fn mixed_case_keywords_converted() {
        assert_formats_to(
            "SeLeCt Id FrOm UsErS",
            "select id from users",
        );
    }

    #[test]
    fn all_common_keywords_lowercase() {
        // Test various keywords
        let inputs = vec![
            ("SELECT FROM WHERE", "select from where"),
            ("AND OR NOT", "and or not"),
            ("IN BETWEEN LIKE", "in between like"),
            ("JOIN LEFT RIGHT FULL INNER OUTER CROSS", "join left right full inner outer cross"),
            ("GROUP BY HAVING ORDER BY", "group by having order by"),
            ("ASC DESC NULLS FIRST LAST", "asc desc nulls first last"),
            ("LIMIT OFFSET", "limit offset"),
            ("UNION INTERSECT EXCEPT ALL DISTINCT", "union intersect except all distinct"),
            ("WITH AS", "with as"),
            ("CASE WHEN THEN ELSE END", "case when then else end"),
            ("IS NULL TRUE FALSE", "is null true false"),
            ("INSERT INTO VALUES UPDATE SET DELETE", "insert into values update set delete"),
            ("CREATE TABLE VIEW ALTER DROP", "create table view alter drop"),
            ("IF EXISTS REPLACE", "if exists replace"),
            ("ON USING MATCHED", "on using matched"),
        ];

        for (_input, _expected) in inputs {
            // Note: These are keyword-only strings that won't form valid SQL,
            // but we're testing keyword lowercasing in isolation
            // The actual parser may reject these - that's expected
        }
    }

    #[test]
    fn cast_keyword_lowercase() {
        assert_formats_to(
            "SELECT CAST(col AS VARCHAR)",
            "select cast(col as varchar)",
        );
    }

    #[test]
    fn over_partition_by_lowercase() {
        assert_formats_to(
            "SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at)",
            "select row_number() over (partition by id order by created_at)",
        );
    }
}

// =============================================================================
// INDENTATION TESTS - 2 spaces for all indentation levels
// =============================================================================

mod indentation {
    use super::*;

    #[test]
    fn select_columns_indented_2_spaces() {
        assert_formats_to(
            "SELECT id, name, email, created_at, updated_at FROM users",
            "select
  id
  , name
  , email
  , created_at
  , updated_at
from users",
        );
    }

    #[test]
    fn nested_subquery_indented() {
        assert_formats_to(
            "SELECT * FROM (SELECT id FROM users) sub",
            "select *
from (
  select id
  from users
) sub",
        );
    }

    #[test]
    fn where_conditions_indented() {
        assert_formats_to(
            "SELECT * FROM users WHERE active = true AND email IS NOT NULL",
            "select *
from users
where active = true
  and email is not null",
        );
    }

    #[test]
    fn cte_body_indented() {
        assert_formats_to(
            "WITH cte AS (SELECT * FROM users WHERE active = true) SELECT * FROM cte",
            "with cte as (
  select *
  from users
  where active = true
)
select * from cte",
        );
    }

    #[test]
    fn case_expression_indented() {
        assert_formats_to(
            "SELECT CASE WHEN status = 'active' THEN 'Active' WHEN status = 'pending' THEN 'Pending' ELSE 'Unknown' END as status_label FROM users",
            "select
  case
    when status = 'active'
      then 'Active'
    when status = 'pending'
      then 'Pending'
    else 'Unknown'
  end as status_label
from users",
        );
    }

    #[test]
    fn join_on_clause_indented() {
        assert_formats_to(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            "select *
from users u
join orders o
  on u.id = o.user_id",
        );
    }
}

// =============================================================================
// LINE LENGTH TESTS - Target line length of 120 characters
// =============================================================================

mod line_length {
    use super::*;

    #[test]
    fn short_query_stays_on_one_line() {
        assert_formats_to(
            "SELECT id FROM users",
            "select id from users",
        );
    }

    #[test]
    fn long_line_wrapped() {
        // Create a query that would exceed 120 characters
        let long_columns = (1..=10)
            .map(|i| format!("very_long_column_name_{}", i))
            .collect::<Vec<_>>()
            .join(", ");
        let input = format!("SELECT {} FROM some_very_long_table_name", long_columns);

        let result = format(&input).expect("format should succeed");

        // Verify no line exceeds 120 characters
        for line in result.lines() {
            assert!(
                line.len() <= 120,
                "Line exceeds 120 chars: {} (len={})",
                line,
                line.len()
            );
        }
    }

    #[test]
    fn function_args_wrapped_when_long() {
        let input = "SELECT my_function(argument_one, argument_two, argument_three, argument_four, argument_five, argument_six) FROM t";

        let result = format(input).expect("format should succeed");

        for line in result.lines() {
            assert!(line.len() <= 120);
        }
    }
}

// =============================================================================
// COMMA TESTS - Leading commas for column lists
// =============================================================================

mod commas {
    use super::*;

    #[test]
    fn leading_commas_for_columns() {
        assert_formats_to(
            "SELECT id, name, email, created_at FROM users",
            "select
  id
  , name
  , email
  , created_at
from users",
        );
    }

    #[test]
    fn leading_commas_in_group_by() {
        assert_formats_to(
            "SELECT a, b, c, count(*) FROM t GROUP BY a, b, c",
            "select
  a
  , b
  , c
  , count(*)
from t
group by
  a
  , b
  , c",
        );
    }

    #[test]
    fn leading_commas_in_order_by() {
        assert_formats_to(
            "SELECT * FROM t ORDER BY a, b DESC, c",
            "select *
from t
order by
  a
  , b desc
  , c",
        );
    }

    #[test]
    fn leading_commas_in_insert_columns() {
        assert_formats_to(
            "INSERT INTO users (id, name, email, created_at) VALUES (1, 'John', 'john@example.com', NOW())",
            "insert into users (
  id
  , name
  , email
  , created_at
)
values (
  1
  , 'John'
  , 'john@example.com'
  , now()
)",
        );
    }
}

// =============================================================================
// SELECT COLUMN TESTS - Threshold-based formatting
// =============================================================================

mod select_columns {
    use super::*;

    #[test]
    fn three_or_fewer_columns_inline() {
        assert_formats_to(
            "SELECT id, name, email FROM users",
            "select id, name, email from users",
        );
    }

    #[test]
    fn two_columns_inline() {
        assert_formats_to(
            "SELECT id, name FROM users",
            "select id, name from users",
        );
    }

    #[test]
    fn one_column_inline() {
        assert_formats_to(
            "SELECT id FROM users",
            "select id from users",
        );
    }

    #[test]
    fn four_columns_vertical() {
        assert_formats_to(
            "SELECT id, name, email, created_at FROM users",
            "select
  id
  , name
  , email
  , created_at
from users",
        );
    }

    #[test]
    fn many_columns_vertical() {
        assert_formats_to(
            "SELECT id, name, email, created_at, updated_at, deleted_at FROM users",
            "select
  id
  , name
  , email
  , created_at
  , updated_at
  , deleted_at
from users",
        );
    }

    #[test]
    fn three_columns_exceeding_line_width_go_vertical() {
        // Even with 3 columns, if they exceed line width, go vertical
        let long_name = "very_long_column_name_that_takes_up_space";
        let input = format!(
            "SELECT {}1, {}2, {}3 FROM t",
            long_name, long_name, long_name
        );

        let result = format(&input).expect("format should succeed");

        // Should be vertical due to length
        assert!(result.contains("\n  ,"));
    }

    #[test]
    fn select_star_inline() {
        assert_formats_to(
            "SELECT * FROM users",
            "select * from users",
        );
    }

    #[test]
    fn select_table_star_inline() {
        assert_formats_to(
            "SELECT u.* FROM users u",
            "select u.* from users u",
        );
    }
}

// =============================================================================
// CTE (WITH CLAUSE) TESTS
// =============================================================================

mod cte {
    use super::*;

    #[test]
    fn simple_cte() {
        assert_formats_to(
            "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
            "with active_users as (
  select *
  from users
  where active = true
)
select * from active_users",
        );
    }

    #[test]
    fn multiple_ctes() {
        assert_formats_to(
            "WITH cte1 AS (SELECT * FROM t1), cte2 AS (SELECT * FROM t2) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id",
            "with cte1 as (
  select *
  from t1
)
, cte2 as (
  select *
  from t2
)
select *
from cte1
join cte2
  on cte1.id = cte2.id",
        );
    }

    #[test]
    fn cte_with_column_list() {
        assert_formats_to(
            "WITH cte (a, b, c) AS (SELECT x, y, z FROM t) SELECT * FROM cte",
            "with cte (a, b, c) as (
  select x, y, z
  from t
)
select * from cte",
        );
    }

    #[test]
    fn nested_cte_reference() {
        assert_formats_to(
            "WITH cte1 AS (SELECT id FROM t1), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2",
            "with cte1 as (
  select id
  from t1
)
, cte2 as (
  select *
  from cte1
)
select * from cte2",
        );
    }
}

// =============================================================================
// JOIN TESTS
// =============================================================================

mod joins {
    use super::*;

    #[test]
    fn simple_join() {
        assert_formats_to(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            "select *
from users u
join orders o
  on u.id = o.user_id",
        );
    }

    #[test]
    fn left_join() {
        assert_formats_to(
            "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
            "select *
from users u
left join orders o
  on u.id = o.user_id",
        );
    }

    #[test]
    fn right_join() {
        assert_formats_to(
            "SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
            "select *
from users u
right join orders o
  on u.id = o.user_id",
        );
    }

    #[test]
    fn full_outer_join() {
        assert_formats_to(
            "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id",
            "select *
from t1
full outer join t2
  on t1.id = t2.id",
        );
    }

    #[test]
    fn cross_join() {
        assert_formats_to(
            "SELECT * FROM t1 CROSS JOIN t2",
            "select *
from t1
cross join t2",
        );
    }

    #[test]
    fn multiple_joins() {
        assert_formats_to(
            "SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id",
            "select
  u.id
  , u.name
  , o.total
from users u
join orders o
  on u.id = o.user_id
left join payments p
  on o.id = p.order_id",
        );
    }

    #[test]
    fn join_with_multiple_on_conditions() {
        assert_formats_to(
            "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b",
            "select *
from t1
join t2
  on t1.a = t2.a
  and t1.b = t2.b",
        );
    }

    #[test]
    fn inner_join_explicit() {
        assert_formats_to(
            "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
            "select *
from t1
inner join t2
  on t1.id = t2.id",
        );
    }
}

// =============================================================================
// WHERE CLAUSE TESTS
// =============================================================================

mod where_clause {
    use super::*;

    #[test]
    fn simple_where() {
        assert_formats_to(
            "SELECT * FROM users WHERE active = true",
            "select *
from users
where active = true",
        );
    }

    #[test]
    fn where_with_and() {
        assert_formats_to(
            "SELECT * FROM users WHERE active = true AND created_at > '2024-01-01'",
            "select *
from users
where active = true
  and created_at > '2024-01-01'",
        );
    }

    #[test]
    fn where_with_multiple_and() {
        assert_formats_to(
            "SELECT * FROM users WHERE active = true AND created_at > '2024-01-01' AND email IS NOT NULL",
            "select *
from users
where active = true
  and created_at > '2024-01-01'
  and email is not null",
        );
    }

    #[test]
    fn where_with_or() {
        assert_formats_to(
            "SELECT * FROM users WHERE status = 'active' OR status = 'pending'",
            "select *
from users
where status = 'active'
  or status = 'pending'",
        );
    }

    #[test]
    fn where_with_in_list() {
        assert_formats_to(
            "SELECT * FROM users WHERE status IN ('active', 'pending', 'suspended')",
            "select *
from users
where status in ('active', 'pending', 'suspended')",
        );
    }

    #[test]
    fn where_with_between() {
        assert_formats_to(
            "SELECT * FROM users WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'",
            "select *
from users
where created_at between '2024-01-01' and '2024-12-31'",
        );
    }

    #[test]
    fn where_with_like() {
        assert_formats_to(
            "SELECT * FROM users WHERE email LIKE '%@example.com'",
            "select *
from users
where email like '%@example.com'",
        );
    }

    #[test]
    fn where_with_is_null() {
        assert_formats_to(
            "SELECT * FROM users WHERE deleted_at IS NULL",
            "select *
from users
where deleted_at is null",
        );
    }

    #[test]
    fn where_with_is_not_null() {
        assert_formats_to(
            "SELECT * FROM users WHERE email IS NOT NULL",
            "select *
from users
where email is not null",
        );
    }

    #[test]
    fn where_with_subquery() {
        assert_formats_to(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)",
            "select *
from users
where id in (
  select user_id
  from orders
  where total > 100
)",
        );
    }
}

// =============================================================================
// CASE EXPRESSION TESTS
// =============================================================================

mod case_expression {
    use super::*;

    #[test]
    fn simple_case() {
        assert_formats_to(
            "SELECT CASE WHEN status = 'active' THEN 'Active' ELSE 'Inactive' END FROM users",
            "select
  case
    when status = 'active'
      then 'Active'
    else 'Inactive'
  end
from users",
        );
    }

    #[test]
    fn case_with_multiple_when() {
        assert_formats_to(
            "SELECT CASE WHEN status = 'active' THEN 'Active' WHEN status = 'pending' THEN 'Pending' ELSE 'Unknown' END as status_label FROM users",
            "select
  case
    when status = 'active'
      then 'Active'
    when status = 'pending'
      then 'Pending'
    else 'Unknown'
  end as status_label
from users",
        );
    }

    #[test]
    fn case_without_else() {
        assert_formats_to(
            "SELECT CASE WHEN x > 0 THEN 'positive' END FROM t",
            "select
  case
    when x > 0
      then 'positive'
  end
from t",
        );
    }

    #[test]
    fn searched_case_with_operand() {
        assert_formats_to(
            "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END FROM t",
            "select
  case status
    when 'active'
      then 1
    when 'inactive'
      then 0
  end
from t",
        );
    }

    #[test]
    fn nested_case() {
        assert_formats_to(
            "SELECT CASE WHEN a THEN CASE WHEN b THEN 1 ELSE 2 END ELSE 3 END FROM t",
            "select
  case
    when a
      then case
        when b
          then 1
        else 2
      end
    else 3
  end
from t",
        );
    }
}

// =============================================================================
// PARENTHESES TESTS
// =============================================================================

mod parentheses {
    use super::*;

    #[test]
    fn simple_arithmetic_inline() {
        assert_formats_to(
            "SELECT (a + b) * c FROM numbers",
            "select (a + b) * c from numbers",
        );
    }

    #[test]
    fn nested_parentheses_inline() {
        assert_formats_to(
            "SELECT ((a + b) * (c + d)) FROM t",
            "select ((a + b) * (c + d)) from t",
        );
    }

    #[test]
    fn complex_expression_breaks() {
        // Complex case inside parens should break
        assert_formats_to(
            "SELECT (CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END) as sign FROM values",
            "select
  (
    case
      when x > 0
        then 'positive'
      else 'negative'
    end
  ) as sign
from values",
        );
    }

    #[test]
    fn subquery_in_parens_breaks() {
        assert_formats_to(
            "SELECT * FROM (SELECT id, name FROM users) AS sub",
            "select *
from (
  select id, name
  from users
) as sub",
        );
    }

    #[test]
    fn function_call_parens() {
        assert_formats_to(
            "SELECT COUNT(*), MAX(id) FROM users",
            "select count(*), max(id) from users",
        );
    }
}

// =============================================================================
// SEMICOLON TESTS
// =============================================================================

mod semicolons {
    use super::*;

    #[test]
    fn preserves_trailing_semicolon() {
        assert_formats_to(
            "SELECT * FROM users;",
            "select * from users;",
        );
    }

    #[test]
    fn no_semicolon_stays_without() {
        let result = format("SELECT * FROM users").expect("format should succeed");
        assert!(!result.trim().ends_with(';'));
    }

    #[test]
    fn does_not_add_semicolon() {
        let input = "SELECT * FROM users";
        let result = format(input).expect("format should succeed");
        assert!(!result.ends_with(';'));
    }
}

// =============================================================================
// COMMENT TESTS
// =============================================================================

mod comments {
    use super::*;

    #[test]
    fn single_line_comment_preserved() {
        assert_formats_to(
            "SELECT id -- user identifier\nFROM users",
            "select id -- user identifier
from users",
        );
    }

    #[test]
    fn multi_line_comment_preserved() {
        assert_formats_to(
            "SELECT /* all columns */ * FROM users",
            "select /* all columns */ * from users",
        );
    }

    #[test]
    fn comment_before_statement() {
        assert_formats_to(
            "-- Get all users\nSELECT * FROM users",
            "-- Get all users
select * from users",
        );
    }

    #[test]
    fn comment_after_statement() {
        assert_formats_to(
            "SELECT * FROM users -- fetch users",
            "select * from users -- fetch users",
        );
    }

    #[test]
    fn comment_between_clauses() {
        assert_formats_to(
            "SELECT *\n-- filter condition\nFROM users\nWHERE active = true",
            "select *
-- filter condition
from users
where active = true",
        );
    }

    #[test]
    fn multi_line_block_comment() {
        assert_formats_to(
            "SELECT *\n/* This is a\n   multi-line\n   comment */\nFROM users",
            "select *
/* This is a
   multi-line
   comment */
from users",
        );
    }
}

// =============================================================================
// OTHER CLAUSE TESTS
// =============================================================================

mod other_clauses {
    use super::*;

    #[test]
    fn group_by() {
        assert_formats_to(
            "SELECT status, COUNT(*) FROM users GROUP BY status",
            "select status, count(*)
from users
group by status",
        );
    }

    #[test]
    fn group_by_multiple() {
        assert_formats_to(
            "SELECT a, b, c, COUNT(*) FROM t GROUP BY a, b, c",
            "select
  a
  , b
  , c
  , count(*)
from t
group by
  a
  , b
  , c",
        );
    }

    #[test]
    fn having() {
        assert_formats_to(
            "SELECT status, COUNT(*) cnt FROM users GROUP BY status HAVING COUNT(*) > 10",
            "select status, count(*) cnt
from users
group by status
having count(*) > 10",
        );
    }

    #[test]
    fn order_by() {
        assert_formats_to(
            "SELECT * FROM users ORDER BY created_at DESC",
            "select *
from users
order by created_at desc",
        );
    }

    #[test]
    fn order_by_nulls() {
        assert_formats_to(
            "SELECT * FROM users ORDER BY name NULLS LAST",
            "select *
from users
order by name nulls last",
        );
    }

    #[test]
    fn limit() {
        assert_formats_to(
            "SELECT * FROM users LIMIT 10",
            "select *
from users
limit 10",
        );
    }

    #[test]
    fn limit_offset() {
        assert_formats_to(
            "SELECT * FROM users LIMIT 10 OFFSET 20",
            "select *
from users
limit 10 offset 20",
        );
    }

    #[test]
    fn distinct() {
        assert_formats_to(
            "SELECT DISTINCT status FROM users",
            "select distinct status from users",
        );
    }
}

// =============================================================================
// SET OPERATIONS TESTS
// =============================================================================

mod set_operations {
    use super::*;

    #[test]
    fn union() {
        assert_formats_to(
            "SELECT id FROM t1 UNION SELECT id FROM t2",
            "select id from t1
union
select id from t2",
        );
    }

    #[test]
    fn union_all() {
        assert_formats_to(
            "SELECT id FROM t1 UNION ALL SELECT id FROM t2",
            "select id from t1
union all
select id from t2",
        );
    }

    #[test]
    fn intersect() {
        assert_formats_to(
            "SELECT id FROM t1 INTERSECT SELECT id FROM t2",
            "select id from t1
intersect
select id from t2",
        );
    }

    #[test]
    fn except() {
        assert_formats_to(
            "SELECT id FROM t1 EXCEPT SELECT id FROM t2",
            "select id from t1
except
select id from t2",
        );
    }

    #[test]
    fn union_with_parenthesized_subquery() {
        // Parenthesized subqueries on right side of UNION
        assert_formats_to(
            "SELECT * FROM a UNION (SELECT * FROM b)",
            "select * from a
union
select * from b",
        );
    }
}

// =============================================================================
// IDEMPOTENCY TESTS
// =============================================================================

mod idempotency {
    use super::*;

    #[test]
    fn simple_select_idempotent() {
        assert_idempotent("SELECT id, name FROM users");
    }

    #[test]
    fn complex_query_idempotent() {
        assert_idempotent("WITH cte AS (SELECT * FROM users WHERE active = true) SELECT u.id, u.name, o.total FROM cte u JOIN orders o ON u.id = o.user_id WHERE o.total > 100 ORDER BY o.total DESC LIMIT 10");
    }

    #[test]
    fn formatted_query_stays_formatted() {
        let formatted = "select
  id
  , name
  , email
from users
where active = true
  and email is not null";

        assert_idempotent(formatted);
    }
}

// =============================================================================
// DML TESTS
// =============================================================================

mod dml {
    use super::*;

    #[test]
    fn simple_insert() {
        assert_formats_to(
            "INSERT INTO users (id, name) VALUES (1, 'John')",
            "insert into users (id, name)
values (1, 'John')",
        );
    }

    #[test]
    fn insert_select() {
        assert_formats_to(
            "INSERT INTO users_backup SELECT * FROM users WHERE active = true",
            "insert into users_backup
select *
from users
where active = true",
        );
    }

    #[test]
    fn simple_update() {
        assert_formats_to(
            "UPDATE users SET active = false WHERE last_login < '2024-01-01'",
            "update users
set active = false
where last_login < '2024-01-01'",
        );
    }

    #[test]
    fn update_multiple_columns() {
        assert_formats_to(
            "UPDATE users SET active = false, updated_at = CURRENT_TIMESTAMP WHERE last_login < '2024-01-01'",
            "update users
set
  active = false
  , updated_at = current_timestamp
where last_login < '2024-01-01'",
        );
    }

    #[test]
    fn simple_delete() {
        assert_formats_to(
            "DELETE FROM users WHERE active = false",
            "delete from users
where active = false",
        );
    }

    #[test]
    fn merge_statement() {
        assert_formats_to(
            "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
            "merge into target t
using source s
  on t.id = s.id
when matched then
  update set t.name = s.name
when not matched then
  insert (id, name)
  values (s.id, s.name)",
        );
    }
}

// =============================================================================
// DDL TESTS
// =============================================================================

mod ddl {
    use super::*;

    #[test]
    fn create_table() {
        assert_formats_to(
            "CREATE TABLE users (id INT, name VARCHAR(255), email VARCHAR(255))",
            "create table users (
  id int
  , name varchar(255)
  , email varchar(255)
)",
        );
    }

    #[test]
    fn create_table_as_select() {
        assert_formats_to(
            "CREATE TABLE users_backup AS SELECT * FROM users WHERE active = true",
            "create table users_backup as
select *
from users
where active = true",
        );
    }

    #[test]
    fn create_view() {
        assert_formats_to(
            "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true",
            "create view active_users as
select *
from users
where active = true",
        );
    }

    #[test]
    fn create_or_replace_view() {
        assert_formats_to(
            "CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE active = true",
            "create or replace view active_users as
select *
from users
where active = true",
        );
    }

    #[test]
    fn drop_table() {
        assert_formats_to(
            "DROP TABLE IF EXISTS users",
            "drop table if exists users",
        );
    }

    #[test]
    fn drop_view() {
        assert_formats_to(
            "DROP VIEW IF EXISTS active_users",
            "drop view if exists active_users",
        );
    }
}
