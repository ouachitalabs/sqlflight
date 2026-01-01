//! Snowflake-specific SQL syntax tests for sqlflight
//!
//! These tests verify that Snowflake-specific SQL features are correctly parsed
//! and formatted according to the project's formatting rules. Covers features
//! documented in SPEC.md including QUALIFY, FLATTEN, PIVOT, MATCH_RECOGNIZE,
//! SAMPLE, semi-structured data access, and more.

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
// QUALIFY CLAUSE TESTS - Snowflake-specific window function filter
// =============================================================================

mod qualify {
    use super::*;

    #[test]
    fn simple_qualify() {
        assert_formats_to(
            "SELECT id, name FROM users QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) = 1",
            "select id, name
from users
qualify row_number() over (partition by department order by salary desc) = 1",
        );
    }

    #[test]
    fn qualify_with_where() {
        assert_formats_to(
            "SELECT id, name, salary FROM employees WHERE active = true QUALIFY RANK() OVER (ORDER BY salary DESC) <= 10",
            "select id, name, salary
from employees
where active = true
qualify rank() over (order by salary desc) <= 10",
        );
    }

    #[test]
    fn qualify_with_group_by() {
        assert_formats_to(
            "SELECT department, COUNT(*) cnt FROM employees GROUP BY department QUALIFY cnt > 5",
            "select department, count(*) cnt
from employees
group by department
qualify cnt > 5",
        );
    }

    #[test]
    fn qualify_dense_rank() {
        // select * stays inline, QUALIFY goes on its own line
        assert_formats_to(
            "SELECT * FROM sales QUALIFY DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) = 1",
            "select * from sales
qualify dense_rank() over (partition by region order by amount desc) = 1",
        );
    }

    #[test]
    fn qualify_with_cte() {
        assert_formats_to(
            "WITH ranked AS (SELECT *, ROW_NUMBER() OVER (ORDER BY created_at) rn FROM events) SELECT * FROM ranked QUALIFY rn = 1",
            "with ranked as (
  select *, row_number() over (order by created_at) rn
  from events
)
select * from ranked
qualify rn = 1",
        );
    }
}

// =============================================================================
// FLATTEN / LATERAL TESTS - Semi-structured data expansion
// =============================================================================

mod flatten {
    use super::*;

    #[test]
    fn simple_flatten() {
        assert_formats_to(
            "SELECT value FROM table1, LATERAL FLATTEN(input => col1)",
            "select value
from table1
, lateral flatten(input => col1)",
        );
    }

    #[test]
    fn flatten_with_path() {
        assert_formats_to(
            "SELECT f.value FROM events, LATERAL FLATTEN(input => data:items, path => 'nested') f",
            "select f.value
from events
, lateral flatten(input => data:items, path => 'nested') f",
        );
    }

    #[test]
    fn flatten_with_outer() {
        assert_formats_to(
            "SELECT t.id, f.value FROM my_table t, LATERAL FLATTEN(input => t.array_col, outer => true) f",
            "select t.id, f.value
from my_table t
, lateral flatten(input => t.array_col, outer => true) f",
        );
    }

    #[test]
    fn flatten_with_recursive() {
        assert_formats_to(
            "SELECT f.key, f.value FROM data_table, LATERAL FLATTEN(input => json_col, recursive => true) f",
            "select f.key, f.value
from data_table
, lateral flatten(input => json_col, recursive => true) f",
        );
    }

    #[test]
    fn flatten_with_mode() {
        assert_formats_to(
            "SELECT f.value FROM json_data, LATERAL FLATTEN(input => data, mode => 'ARRAY') f",
            "select f.value
from json_data
, lateral flatten(input => data, mode => 'ARRAY') f",
        );
    }

    #[test]
    fn multiple_flatten() {
        assert_formats_to(
            "SELECT f1.value, f2.value FROM nested_data, LATERAL FLATTEN(input => array1) f1, LATERAL FLATTEN(input => array2) f2",
            "select f1.value, f2.value
from nested_data
, lateral flatten(input => array1) f1
, lateral flatten(input => array2) f2",
        );
    }

    #[test]
    fn lateral_subquery() {
        assert_formats_to(
            "SELECT t.id, s.val FROM my_table t, LATERAL (SELECT value AS val FROM other_table WHERE other_table.fk = t.id) s",
            "select t.id, s.val
from my_table t
, lateral (
  select value as val
  from other_table
  where other_table.fk = t.id
) s",
        );
    }
}

// =============================================================================
// PIVOT / UNPIVOT TESTS - Data transformation
// =============================================================================

mod pivot {
    use super::*;

    #[test]
    fn simple_pivot() {
        assert_formats_to(
            "SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))",
            "select *
from sales
pivot (
  sum(amount)
  for quarter in ('Q1', 'Q2', 'Q3', 'Q4')
)",
        );
    }

    #[test]
    fn pivot_with_alias() {
        assert_formats_to(
            "SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2)) p",
            "select *
from sales
pivot (
  sum(amount)
  for quarter in ('Q1' as q1, 'Q2' as q2)
) p",
        );
    }

    #[test]
    fn pivot_multiple_aggregates() {
        assert_formats_to(
            "SELECT * FROM sales PIVOT (SUM(amount) AS total, AVG(amount) AS average FOR quarter IN ('Q1', 'Q2'))",
            "select *
from sales
pivot (
  sum(amount) as total
  , avg(amount) as average
  for quarter in ('Q1', 'Q2')
)",
        );
    }

    #[test]
    fn simple_unpivot() {
        assert_formats_to(
            "SELECT * FROM quarterly_sales UNPIVOT (amount FOR quarter IN (q1, q2, q3, q4))",
            "select *
from quarterly_sales
unpivot (
  amount
  for quarter in (q1, q2, q3, q4)
)",
        );
    }

    #[test]
    fn unpivot_include_nulls() {
        assert_formats_to(
            "SELECT * FROM wide_table UNPIVOT INCLUDE NULLS (value FOR attribute IN (col1, col2, col3))",
            "select *
from wide_table
unpivot include nulls (
  value
  for attribute in (col1, col2, col3)
)",
        );
    }

    #[test]
    fn pivot_with_join() {
        assert_formats_to(
            "SELECT p.* FROM sales s JOIN products prod ON s.product_id = prod.id PIVOT (SUM(s.amount) FOR s.quarter IN ('Q1', 'Q2')) p",
            "select p.*
from sales s
join products prod
  on s.product_id = prod.id
pivot (
  sum(s.amount)
  for s.quarter in ('Q1', 'Q2')
) p",
        );
    }

    #[test]
    fn pivot_with_column_aliases() {
        assert_formats_to(
            "SELECT * FROM (SELECT region, quarter, sales FROM sales_data) PIVOT(SUM(sales) FOR quarter IN ('Q1','Q2'))AS p(region,q1_sales,q2_sales)",
            "select *
from (
  select region, quarter, sales
  from sales_data
)
pivot (
  sum(sales)
  for quarter in ('Q1', 'Q2')
) as p (region, q1_sales, q2_sales)",
        );
    }

    #[test]
    fn pivot_from_subquery_no_alias() {
        assert_formats_to(
            "SELECT * FROM (SELECT region FROM sales_data) PIVOT (SUM(sales) FOR quarter IN ('Q1','Q2'))",
            "select *
from (
  select region
  from sales_data
)
pivot (
  sum(sales)
  for quarter in ('Q1', 'Q2')
)",
        );
    }
}

// =============================================================================
// MATCH_RECOGNIZE TESTS - Pattern matching on rows
// =============================================================================

mod match_recognize {
    use super::*;

    #[test]
    fn simple_match_recognize() {
        assert_formats_to(
            "SELECT * FROM stock_data MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY trade_date MEASURES STRT.trade_date AS start_date, LAST(UP.trade_date) AS end_date PATTERN (STRT UP+) DEFINE UP AS UP.price > PREV(UP.price))",
            "select *
from stock_data
match_recognize (
  partition by symbol
  order by trade_date
  measures
    strt.trade_date as start_date
    , last(up.trade_date) as end_date
  pattern (strt up+)
  define
    up as up.price > prev(up.price)
)",
        );
    }

    #[test]
    fn match_recognize_with_after_match() {
        assert_formats_to(
            "SELECT * FROM events MATCH_RECOGNIZE (ORDER BY event_time MEASURES MATCH_NUMBER() AS match_num AFTER MATCH SKIP PAST LAST ROW PATTERN (A B+ C) DEFINE A AS A.type = 'start', B AS B.type = 'middle', C AS C.type = 'end')",
            "select *
from events
match_recognize (
  order by event_time
  measures
    match_number() as match_num
  after match skip past last row
  pattern (a b+ c)
  define
    a as a.type = 'start'
    , b as b.type = 'middle'
    , c as c.type = 'end'
)",
        );
    }

    #[test]
    fn match_recognize_one_row_per_match() {
        assert_formats_to(
            "SELECT * FROM sensor_data MATCH_RECOGNIZE (PARTITION BY sensor_id ORDER BY reading_time ONE ROW PER MATCH PATTERN (LOW HIGH+) DEFINE LOW AS LOW.value < 10, HIGH AS HIGH.value >= 10)",
            "select *
from sensor_data
match_recognize (
  partition by sensor_id
  order by reading_time
  one row per match
  pattern (low high+)
  define
    low as low.value < 10
    , high as high.value >= 10
)",
        );
    }

    #[test]
    fn match_recognize_all_rows() {
        assert_formats_to(
            "SELECT * FROM data MATCH_RECOGNIZE (ORDER BY ts ALL ROWS PER MATCH PATTERN (A+ B) DEFINE A AS A.x > 0, B AS B.x < 0)",
            "select *
from data
match_recognize (
  order by ts
  all rows per match
  pattern (a+ b)
  define
    a as a.x > 0
    , b as b.x < 0
)",
        );
    }

    #[test]
    fn match_recognize_final_running_modifiers() {
        assert_formats_to(
            "SELECT FINAL LAST(price) AS fp, RUNNING AVG(price) AS rp FROM t",
            "select final last(price) as fp, running avg(price) as rp from t",
        );
    }
}

// =============================================================================
// SAMPLE / TABLESAMPLE TESTS - Random sampling of data
// =============================================================================

mod sample {
    use super::*;

    #[test]
    fn sample_percent() {
        assert_formats_to(
            "SELECT * FROM large_table SAMPLE (10)",
            "select *
from large_table
sample (10)",
        );
    }

    #[test]
    fn sample_rows() {
        assert_formats_to(
            "SELECT * FROM large_table SAMPLE (1000 ROWS)",
            "select *
from large_table
sample (1000 rows)",
        );
    }

    #[test]
    fn tablesample_bernoulli() {
        assert_formats_to(
            "SELECT * FROM events TABLESAMPLE BERNOULLI (5)",
            "select *
from events
tablesample bernoulli (5)",
        );
    }

    #[test]
    fn tablesample_system() {
        assert_formats_to(
            "SELECT * FROM events TABLESAMPLE SYSTEM (10)",
            "select *
from events
tablesample system (10)",
        );
    }

    #[test]
    fn sample_with_seed() {
        assert_formats_to(
            "SELECT * FROM data SAMPLE (5) SEED (42)",
            "select *
from data
sample (5) seed (42)",
        );
    }

    #[test]
    fn sample_block() {
        assert_formats_to(
            "SELECT * FROM warehouse_data SAMPLE BLOCK (10)",
            "select *
from warehouse_data
sample block (10)",
        );
    }
}

// =============================================================================
// RESULT_SCAN / GENERATOR TESTS - Query results and data generation
// =============================================================================

mod result_scan_generator {
    use super::*;

    #[test]
    fn result_scan_basic() {
        assert_formats_to(
            "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))",
            "select *
from table(result_scan(last_query_id()))",
        );
    }

    #[test]
    fn result_scan_specific_id() {
        assert_formats_to(
            "SELECT * FROM TABLE(RESULT_SCAN('01234567-89ab-cdef-0123-456789abcdef'))",
            "select *
from table(result_scan('01234567-89ab-cdef-0123-456789abcdef'))",
        );
    }

    #[test]
    fn generator_rowcount() {
        assert_formats_to(
            "SELECT SEQ4() AS row_num FROM TABLE(GENERATOR(ROWCOUNT => 1000))",
            "select seq4() as row_num
from table(generator(rowcount => 1000))",
        );
    }

    #[test]
    fn generator_timelimit() {
        assert_formats_to(
            "SELECT RANDOM() AS rand_val FROM TABLE(GENERATOR(TIMELIMIT => 5))",
            "select random() as rand_val
from table(generator(timelimit => 5))",
        );
    }

    #[test]
    fn generator_with_sequence() {
        assert_formats_to(
            "SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS id, DATEADD('day', SEQ4(), '2024-01-01') AS date FROM TABLE(GENERATOR(ROWCOUNT => 365))",
            "select
  row_number() over (order by seq4()) as id
  , dateadd('day', seq4(), '2024-01-01') as date
from table(generator(rowcount => 365))",
        );
    }
}

// =============================================================================
// SEMI-STRUCTURED DATA ACCESS TESTS - JSON/VARIANT path notation
// =============================================================================

mod semi_structured {
    use super::*;

    #[test]
    fn colon_notation_simple() {
        assert_formats_to(
            "SELECT data:name FROM events",
            "select data:name from events",
        );
    }

    #[test]
    fn colon_notation_nested() {
        assert_formats_to(
            "SELECT data:user:profile:email FROM events",
            "select data:user:profile:email from events",
        );
    }

    #[test]
    fn colon_notation_array_index() {
        assert_formats_to(
            "SELECT data:items[0]:name FROM orders",
            "select data:items[0]:name from orders",
        );
    }

    #[test]
    fn colon_notation_with_cast() {
        assert_formats_to(
            "SELECT data:age::INTEGER AS age FROM users",
            "select data:age::integer as age from users",
        );
    }

    #[test]
    fn double_colon_cast() {
        assert_formats_to(
            "SELECT json_col:value::VARCHAR, json_col:count::NUMBER FROM data",
            "select json_col:value::varchar, json_col:count::number from data",
        );
    }

    #[test]
    fn bracket_notation() {
        assert_formats_to(
            "SELECT data['key with spaces'] FROM json_table",
            "select data['key with spaces'] from json_table",
        );
    }

    #[test]
    fn mixed_notation() {
        assert_formats_to(
            "SELECT data:items[0]['special-key']:value::TEXT FROM complex_json",
            "select data:items[0]['special-key']:value::text from complex_json",
        );
    }

    #[test]
    fn parse_json() {
        assert_formats_to(
            "SELECT PARSE_JSON('{\"name\": \"John\", \"age\": 30}'):name::VARCHAR",
            "select parse_json('{\"name\": \"John\", \"age\": 30}'):name::varchar",
        );
    }

    #[test]
    fn try_parse_json() {
        assert_formats_to(
            "SELECT TRY_PARSE_JSON(raw_json):field FROM raw_data",
            "select try_parse_json(raw_json):field from raw_data",
        );
    }

    #[test]
    fn object_construct() {
        assert_formats_to(
            "SELECT OBJECT_CONSTRUCT('key1', value1, 'key2', value2) AS obj FROM source",
            "select object_construct('key1', value1, 'key2', value2) as obj from source",
        );
    }

    #[test]
    fn array_construct() {
        assert_formats_to(
            "SELECT ARRAY_CONSTRUCT(1, 2, 3, 4, 5) AS arr",
            "select array_construct(1, 2, 3, 4, 5) as arr",
        );
    }

    #[test]
    fn array_agg() {
        assert_formats_to(
            "SELECT department, ARRAY_AGG(employee_name) AS employees FROM staff GROUP BY department",
            "select department, array_agg(employee_name) as employees
from staff
group by department",
        );
    }

    #[test]
    fn object_agg() {
        assert_formats_to(
            "SELECT OBJECT_AGG(key, value) AS combined FROM key_value_pairs",
            "select object_agg(key, value) as combined from key_value_pairs",
        );
    }
}

// =============================================================================
// VARIANT / OBJECT / ARRAY TYPE TESTS
// =============================================================================

mod variant_types {
    use super::*;

    #[test]
    fn cast_to_variant() {
        assert_formats_to(
            "SELECT TO_VARIANT(123) AS v, TO_VARIANT('hello') AS s",
            "select to_variant(123) as v, to_variant('hello') as s",
        );
    }

    #[test]
    fn cast_to_object() {
        assert_formats_to(
            "SELECT TO_OBJECT(variant_col) AS obj FROM data",
            "select to_object(variant_col) as obj from data",
        );
    }

    #[test]
    fn cast_to_array() {
        assert_formats_to(
            "SELECT TO_ARRAY(variant_col) AS arr FROM data",
            "select to_array(variant_col) as arr from data",
        );
    }

    #[test]
    fn typeof_function() {
        assert_formats_to(
            "SELECT TYPEOF(data:field) AS field_type FROM json_table",
            "select typeof(data:field) as field_type from json_table",
        );
    }

    #[test]
    fn is_null_value() {
        assert_formats_to(
            "SELECT * FROM json_data WHERE IS_NULL_VALUE(data:nullable_field)",
            "select *
from json_data
where is_null_value(data:nullable_field)",
        );
    }

    #[test]
    fn array_size() {
        assert_formats_to(
            "SELECT ARRAY_SIZE(data:items) AS item_count FROM orders",
            "select array_size(data:items) as item_count from orders",
        );
    }

    #[test]
    fn get_path() {
        assert_formats_to(
            "SELECT GET_PATH(data, 'user.address.city') AS city FROM profiles",
            "select get_path(data, 'user.address.city') as city from profiles",
        );
    }

    #[test]
    fn object_keys() {
        assert_formats_to(
            "SELECT OBJECT_KEYS(data) AS keys FROM json_records",
            "select object_keys(data) as keys from json_records",
        );
    }

    #[test]
    fn array_slice() {
        assert_formats_to(
            "SELECT ARRAY_SLICE(data:items, 0, 5) AS first_five FROM orders",
            "select array_slice(data:items, 0, 5) as first_five from orders",
        );
    }

    #[test]
    fn array_contains() {
        assert_formats_to(
            "SELECT * FROM data WHERE ARRAY_CONTAINS('search_value'::VARIANT, tags_array)",
            "select *
from data
where array_contains('search_value'::variant, tags_array)",
        );
    }
}

// =============================================================================
// WINDOW FUNCTIONS WITH SNOWFLAKE EXTENSIONS TESTS
// =============================================================================

mod window_functions {
    use super::*;

    #[test]
    fn row_number_basic() {
        assert_formats_to(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) AS rn FROM events",
            "select id, row_number() over (order by created_at) as rn from events",
        );
    }

    #[test]
    fn row_number_partition() {
        assert_formats_to(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) AS rank FROM items",
            "select id, row_number() over (partition by category order by score desc) as rank from items",
        );
    }

    #[test]
    fn lead_lag() {
        assert_formats_to(
            "SELECT date, value, LAG(value, 1) OVER (ORDER BY date) AS prev_value, LEAD(value, 1) OVER (ORDER BY date) AS next_value FROM time_series",
            "select
  date
  , value
  , lag(value, 1) over (order by date) as prev_value
  , lead(value, 1) over (order by date) as next_value
from time_series",
        );
    }

    #[test]
    fn first_last_value() {
        assert_formats_to(
            "SELECT FIRST_VALUE(price) OVER (PARTITION BY product ORDER BY date) AS first_price, LAST_VALUE(price) OVER (PARTITION BY product ORDER BY date) AS last_price FROM prices",
            "select
  first_value(price) over (partition by product order by date) as first_price
  , last_value(price) over (partition by product order by date) as last_price
from prices",
        );
    }

    #[test]
    fn nth_value() {
        assert_formats_to(
            "SELECT NTH_VALUE(score, 3) OVER (PARTITION BY team ORDER BY game_date) AS third_score FROM games",
            "select nth_value(score, 3) over (partition by team order by game_date) as third_score from games",
        );
    }

    #[test]
    fn ntile() {
        assert_formats_to(
            "SELECT id, NTILE(4) OVER (ORDER BY score DESC) AS quartile FROM students",
            "select id, ntile(4) over (order by score desc) as quartile from students",
        );
    }

    #[test]
    fn percent_rank_cume_dist() {
        assert_formats_to(
            "SELECT name, PERCENT_RANK() OVER (ORDER BY salary) AS pct_rank, CUME_DIST() OVER (ORDER BY salary) AS cume_dist FROM employees",
            "select
  name
  , percent_rank() over (order by salary) as pct_rank
  , cume_dist() over (order by salary) as cume_dist
from employees",
        );
    }

    #[test]
    fn running_sum() {
        assert_formats_to(
            "SELECT date, amount, SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM transactions",
            "select
  date
  , amount
  , sum(amount) over (order by date rows between unbounded preceding and current row) as running_total
from transactions",
        );
    }

    #[test]
    fn sliding_window() {
        assert_formats_to(
            "SELECT date, value, AVG(value) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d FROM daily_metrics",
            "select
  date
  , value
  , avg(value) over (order by date rows between 6 preceding and current row) as moving_avg_7d
from daily_metrics",
        );
    }

    #[test]
    fn range_window() {
        assert_formats_to(
            "SELECT timestamp, value, SUM(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS hourly_sum FROM events",
            "select
  timestamp
  , value
  , sum(value) over (order by timestamp range between interval '1 hour' preceding and current row) as hourly_sum
from events",
        );
    }

    #[test]
    fn window_alias() {
        assert_formats_to(
            "SELECT id, SUM(amount) OVER w AS running_sum, AVG(amount) OVER w AS running_avg FROM sales WINDOW w AS (PARTITION BY region ORDER BY date)",
            "select
  id
  , sum(amount) over w as running_sum
  , avg(amount) over w as running_avg
from sales
window w as (partition by region order by date)",
        );
    }

    #[test]
    fn multiple_window_aliases() {
        assert_formats_to(
            "SELECT id, SUM(a) OVER w1, AVG(b) OVER w2 FROM t WINDOW w1 AS (ORDER BY x), w2 AS (ORDER BY y)",
            "select id, sum(a) over w1, avg(b) over w2
from t
window w1 as (order by x), w2 as (order by y)",
        );
    }

    #[test]
    fn conditional_aggregate_window() {
        assert_formats_to(
            "SELECT id, COUNT_IF(status = 'active') OVER (PARTITION BY group_id) AS active_count FROM records",
            "select id, count_if(status = 'active') over (partition by group_id) as active_count from records",
        );
    }

    #[test]
    fn listagg_within_group() {
        assert_formats_to(
            "SELECT department, LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) AS employee_list FROM employees GROUP BY department",
            "select department, listagg(name, ', ') within group (order by name) as employee_list
from employees
group by department",
        );
    }
}

// =============================================================================
// ADDITIONAL SNOWFLAKE-SPECIFIC SYNTAX TESTS
// =============================================================================

mod snowflake_misc {
    use super::*;

    #[test]
    fn try_cast() {
        assert_formats_to(
            "SELECT TRY_CAST(str_col AS INTEGER) AS safe_int FROM data",
            "select try_cast(str_col as integer) as safe_int from data",
        );
    }

    #[test]
    fn iff_function() {
        assert_formats_to(
            "SELECT IFF(condition, true_value, false_value) FROM table1",
            "select iff(condition, true_value, false_value) from table1",
        );
    }

    #[test]
    fn nullifzero_zeroifnull() {
        assert_formats_to(
            "SELECT NULLIFZERO(count_col), ZEROIFNULL(amount) FROM stats",
            "select nullifzero(count_col), zeroifnull(amount) from stats",
        );
    }

    #[test]
    fn equal_null() {
        assert_formats_to(
            "SELECT * FROM t1 WHERE EQUAL_NULL(col1, col2)",
            "select *
from t1
where equal_null(col1, col2)",
        );
    }

    #[test]
    fn decode() {
        assert_formats_to(
            "SELECT DECODE(status, 1, 'Active', 2, 'Inactive', 'Unknown') AS status_text FROM users",
            "select decode(status, 1, 'Active', 2, 'Inactive', 'Unknown') as status_text from users",
        );
    }

    #[test]
    fn array_intersection() {
        assert_formats_to(
            "SELECT ARRAY_INTERSECTION(arr1, arr2) AS common FROM arrays",
            "select array_intersection(arr1, arr2) as common from arrays",
        );
    }

    #[test]
    fn arrays_overlap() {
        assert_formats_to(
            "SELECT * FROM data WHERE ARRAYS_OVERLAP(tags, ARRAY_CONSTRUCT('tag1', 'tag2'))",
            "select *
from data
where arrays_overlap(tags, array_construct('tag1', 'tag2'))",
        );
    }

    #[test]
    fn dateadd_datediff() {
        // Per spec: 3 or fewer columns that fit within line width → inline
        assert_formats_to(
            "SELECT DATEADD('day', 7, current_date) AS next_week, DATEDIFF('month', start_date, end_date) AS months FROM periods",
            "select dateadd('day', 7, current_date) as next_week, datediff('month', start_date, end_date) as months from periods",
        );
    }

    #[test]
    fn time_slice() {
        assert_formats_to(
            "SELECT TIME_SLICE(event_time, 15, 'MINUTE') AS bucket FROM events",
            "select time_slice(event_time, 15, 'MINUTE') as bucket from events",
        );
    }

    #[test]
    fn date_trunc() {
        assert_formats_to(
            "SELECT DATE_TRUNC('month', order_date) AS month_start FROM orders",
            "select date_trunc('month', order_date) as month_start from orders",
        );
    }

    #[test]
    fn hash_functions() {
        // Per spec: 3 or fewer columns that fit within line width → inline
        assert_formats_to(
            "SELECT HASH(id) AS id_hash, MD5(email) AS email_hash, SHA2(password, 256) AS pw_hash FROM users",
            "select hash(id) as id_hash, md5(email) as email_hash, sha2(password, 256) as pw_hash from users",
        );
    }

    #[test]
    fn uuid_functions() {
        assert_formats_to(
            "SELECT UUID_STRING() AS new_id, UUID_STRING('v5', namespace_uuid, 'name') AS deterministic_id",
            "select uuid_string() as new_id, uuid_string('v5', namespace_uuid, 'name') as deterministic_id",
        );
    }

    #[test]
    fn regexp_functions() {
        assert_formats_to(
            "SELECT REGEXP_LIKE(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$') AS is_valid_email FROM users",
            "select regexp_like(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$') as is_valid_email from users",
        );
    }

    #[test]
    fn regexp_substr() {
        assert_formats_to(
            "SELECT REGEXP_SUBSTR(log_message, '[0-9]+') AS extracted_number FROM logs",
            "select regexp_substr(log_message, '[0-9]+') as extracted_number from logs",
        );
    }

    #[test]
    fn regexp_replace() {
        assert_formats_to(
            "SELECT REGEXP_REPLACE(phone, '[^0-9]', '') AS clean_phone FROM contacts",
            "select regexp_replace(phone, '[^0-9]', '') as clean_phone from contacts",
        );
    }

    #[test]
    fn split_to_table() {
        assert_formats_to(
            "SELECT t.value FROM tags, LATERAL SPLIT_TO_TABLE(tags.tag_list, ',') t",
            "select t.value
from tags
, lateral split_to_table(tags.tag_list, ',') t",
        );
    }

    #[test]
    fn strtok_to_array() {
        assert_formats_to(
            "SELECT STRTOK_TO_ARRAY(csv_string, ',') AS values FROM raw_data",
            "select strtok_to_array(csv_string, ',') as values from raw_data",
        );
    }

    #[test]
    fn approximate_count() {
        assert_formats_to(
            "SELECT APPROX_COUNT_DISTINCT(user_id) AS unique_users FROM events",
            "select approx_count_distinct(user_id) as unique_users from events",
        );
    }

    #[test]
    fn approx_percentile() {
        assert_formats_to(
            "SELECT APPROX_PERCENTILE(response_time, 0.95) AS p95 FROM requests",
            "select approx_percentile(response_time, 0.95) as p95 from requests",
        );
    }

    #[test]
    fn hll_functions() {
        assert_formats_to(
            "SELECT HLL(user_id) AS hll_sketch, HLL_ESTIMATE(hll_col) AS estimate FROM data",
            "select hll(user_id) as hll_sketch, hll_estimate(hll_col) as estimate from data",
        );
    }

    #[test]
    fn values_clause() {
        assert_formats_to(
            "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, letter)",
            "select *
from values
  (1, 'a')
  , (2, 'b')
  , (3, 'c')
as t(id, letter)",
        );
    }

    #[test]
    fn copy_grants() {
        assert_formats_to(
            "CREATE OR REPLACE VIEW new_view COPY GRANTS AS SELECT * FROM source_table",
            "create or replace view new_view copy grants as
select *
from source_table",
        );
    }

    #[test]
    fn clone_statement() {
        assert_formats_to(
            "CREATE TABLE new_table CLONE source_table",
            "create table new_table clone source_table",
        );
    }

    #[test]
    fn time_travel() {
        assert_formats_to(
            "SELECT * FROM my_table AT (TIMESTAMP => '2024-01-01 00:00:00'::TIMESTAMP)",
            "select *
from my_table at (timestamp => '2024-01-01 00:00:00'::timestamp)",
        );
    }

    #[test]
    fn time_travel_offset() {
        assert_formats_to(
            "SELECT * FROM my_table AT (OFFSET => -60*5)",
            "select *
from my_table at (offset => -60 * 5)",
        );
    }

    #[test]
    fn before_statement() {
        assert_formats_to(
            "SELECT * FROM my_table BEFORE (STATEMENT => '01234567-89ab-cdef-0123-456789abcdef')",
            "select *
from my_table before (statement => '01234567-89ab-cdef-0123-456789abcdef')",
        );
    }

    #[test]
    fn changes_clause() {
        assert_formats_to(
            "SELECT * FROM my_table CHANGES (INFORMATION => DEFAULT) AT (TIMESTAMP => '2024-01-01'::TIMESTAMP)",
            "select *
from my_table changes (information => default) at (timestamp => '2024-01-01'::timestamp)",
        );
    }

    #[test]
    fn changes_with_end_clause() {
        assert_formats_to(
            "SELECT * FROM my_table CHANGES(INFORMATION=>DEFAULT) AT(TIMESTAMP=>'2024-01-01'::TIMESTAMP) END(TIMESTAMP=>'2024-01-02'::TIMESTAMP)",
            "select *
from my_table changes (information => default) at (timestamp => '2024-01-01'::timestamp) end (timestamp => '2024-01-02'::timestamp)",
        );
    }

    #[test]
    fn changes_with_offset_end() {
        assert_formats_to(
            "SELECT * FROM my_table CHANGES(INFORMATION=>APPEND_ONLY) AT(OFFSET=>-86400) END(OFFSET=>0)",
            "select *
from my_table changes (information => append_only) at (offset => -86400) end (offset => 0)",
        );
    }
}

// =============================================================================
// IDEMPOTENCY TESTS FOR SNOWFLAKE FEATURES
// =============================================================================

mod snowflake_idempotency {
    use super::*;

    #[test]
    fn qualify_idempotent() {
        assert_idempotent("SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1");
    }

    #[test]
    fn flatten_idempotent() {
        assert_idempotent("SELECT f.value FROM t, LATERAL FLATTEN(input => t.arr) f");
    }

    #[test]
    fn pivot_idempotent() {
        assert_idempotent("SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))");
    }

    #[test]
    fn semi_structured_idempotent() {
        assert_idempotent("SELECT data:user:name::VARCHAR, data:items[0]:price::NUMBER FROM events");
    }

    #[test]
    fn window_function_idempotent() {
        assert_idempotent("SELECT id, SUM(val) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t");
    }

    #[test]
    fn complex_snowflake_query_idempotent() {
        assert_idempotent(
            "WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) rn FROM items) SELECT r.id, r.name, f.value:attr::VARCHAR AS attr FROM ranked r, LATERAL FLATTEN(input => r.data:tags) f WHERE r.rn <= 3 QUALIFY f.index = 0"
        );
    }
}

// =============================================================================
// STAGE REFERENCE TESTS - Snowflake stage (@) syntax
// =============================================================================

mod stage_references {
    use super::*;

    #[test]
    fn simple_stage() {
        assert_formats_to(
            "SELECT * FROM @my_stage",
            "select * from @my_stage",
        );
    }

    #[test]
    fn stage_with_path() {
        assert_formats_to(
            "SELECT * FROM @my_stage/path/to/files/",
            "select * from @my_stage/path/to/files/",
        );
    }

    #[test]
    fn stage_with_alias() {
        assert_formats_to(
            "SELECT s.* FROM @my_stage/data s",
            "select s.* from @my_stage/data s",
        );
    }

    #[test]
    fn table_stage() {
        assert_formats_to(
            "SELECT * FROM @%my_table",
            "select * from @%my_table",
        );
    }

    #[test]
    fn stage_idempotent() {
        assert_idempotent("SELECT * FROM @my_stage/path/to/data");
    }
}

// =============================================================================
// POSITIONAL COLUMN REFERENCES - $1, $2, etc. for COPY INTO and stage queries
// =============================================================================

mod positional_columns {
    use super::*;

    #[test]
    fn simple_positional() {
        assert_formats_to(
            "SELECT $1, $2, $3 FROM @my_stage",
            "select $1, $2, $3 from @my_stage",
        );
    }

    #[test]
    fn positional_with_alias() {
        assert_formats_to(
            "SELECT $1 AS col1, $2 AS col2 FROM @my_stage",
            "select $1 as col1, $2 as col2 from @my_stage",
        );
    }

    #[test]
    fn positional_in_expression() {
        assert_formats_to(
            "SELECT $1 + $2 FROM @my_stage",
            "select $1 + $2 from @my_stage",
        );
    }

    #[test]
    fn positional_idempotent() {
        assert_idempotent("SELECT $1, $2, $3 FROM @my_stage/data.csv");
    }
}

// =============================================================================
// IS TRUE / IS FALSE / IS DISTINCT FROM - Boolean comparison extensions
// =============================================================================

mod is_comparisons {
    use super::*;

    #[test]
    fn is_true() {
        assert_formats_to(
            "SELECT * FROM t WHERE col IS TRUE",
            "select *
from t
where col is true",
        );
    }

    #[test]
    fn is_not_true() {
        assert_formats_to(
            "SELECT * FROM t WHERE col IS NOT TRUE",
            "select *
from t
where col is not true",
        );
    }

    #[test]
    fn is_false() {
        assert_formats_to(
            "SELECT * FROM t WHERE col IS FALSE",
            "select *
from t
where col is false",
        );
    }

    #[test]
    fn is_not_false() {
        assert_formats_to(
            "SELECT * FROM t WHERE col IS NOT FALSE",
            "select *
from t
where col is not false",
        );
    }

    #[test]
    fn is_distinct_from() {
        assert_formats_to(
            "SELECT * FROM t WHERE a IS DISTINCT FROM b",
            "select *
from t
where a is distinct from b",
        );
    }

    #[test]
    fn is_not_distinct_from() {
        assert_formats_to(
            "SELECT * FROM t WHERE a IS NOT DISTINCT FROM b",
            "select *
from t
where a is not distinct from b",
        );
    }

    #[test]
    fn is_comparisons_idempotent() {
        assert_idempotent("SELECT * FROM t WHERE a IS TRUE AND b IS NOT DISTINCT FROM c");
    }
}

// =============================================================================
// WITH RECURSIVE - Recursive common table expressions
// =============================================================================

mod with_recursive {
    use super::*;

    #[test]
    fn simple_recursive_cte() {
        assert_formats_to(
            "WITH RECURSIVE cte AS (SELECT 1 n UNION ALL SELECT n+1 FROM cte WHERE n<10) SELECT * FROM cte",
            "with recursive cte as (
  select 1 n
  union all
  select n + 1
  from cte
  where n < 10
)
select * from cte",
        );
    }

    #[test]
    fn recursive_cte_idempotent() {
        assert_idempotent("WITH RECURSIVE nums AS (SELECT 1 n UNION ALL SELECT n+1 FROM nums WHERE n<5) SELECT * FROM nums");
    }
}

// =============================================================================
// ANY/ALL/SOME SUBQUERY TESTS - Comparison operators with subqueries
// =============================================================================

mod any_all_some {
    use super::*;

    #[test]
    fn any_with_subquery() {
        assert_formats_to(
            "SELECT * FROM t WHERE x = ANY(SELECT y FROM t2)",
            "select *
from t
where x = any((
    select y
    from t2
  ))",
        );
    }

    #[test]
    fn some_with_subquery() {
        assert_formats_to(
            "SELECT * FROM t WHERE x < SOME(SELECT y FROM t2)",
            "select *
from t
where x < some((
    select y
    from t2
  ))",
        );
    }

    #[test]
    fn any_subquery_idempotent() {
        assert_idempotent("SELECT * FROM orders WHERE amount = ANY(SELECT limit_amount FROM customers)");
    }
}
