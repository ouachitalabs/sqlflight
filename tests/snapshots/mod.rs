//! Snapshot tests using insta
//!
//! These tests use input files and compare against expected output snapshots.

use insta::assert_snapshot;
use sqlflight::format;

/// Format a SQL string and snapshot the result
fn snapshot_format(name: &str, input: &str) {
    match format(input) {
        Ok(formatted) => assert_snapshot!(name, formatted),
        Err(e) => assert_snapshot!(name, format!("ERROR: {}", e)),
    }
}

// =============================================================================
// Basic SELECT Snapshots
// =============================================================================

#[test]
fn snapshot_simple_select() {
    snapshot_format("simple_select", "SELECT id, name FROM users");
}

#[test]
fn snapshot_select_star() {
    snapshot_format("select_star", "SELECT * FROM users");
}

#[test]
fn snapshot_select_many_columns() {
    snapshot_format(
        "select_many_columns",
        "SELECT id, first_name, last_name, email, phone, address, city, state, country, zip_code, created_at, updated_at FROM users",
    );
}

#[test]
fn snapshot_select_with_aliases() {
    snapshot_format(
        "select_with_aliases",
        "SELECT u.id AS user_id, u.name AS user_name, o.id AS order_id FROM users u JOIN orders o ON u.id = o.user_id",
    );
}

// =============================================================================
// CTE Snapshots
// =============================================================================

#[test]
fn snapshot_simple_cte() {
    snapshot_format(
        "simple_cte",
        "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
    );
}

#[test]
fn snapshot_multiple_ctes() {
    snapshot_format(
        "multiple_ctes",
        "WITH cte1 AS (SELECT id FROM t1), cte2 AS (SELECT id FROM t2), cte3 AS (SELECT id FROM t3) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id JOIN cte3 ON cte2.id = cte3.id",
    );
}

#[test]
fn snapshot_nested_cte() {
    snapshot_format(
        "nested_cte",
        "WITH outer_cte AS (WITH inner_cte AS (SELECT 1 AS x) SELECT * FROM inner_cte) SELECT * FROM outer_cte",
    );
}

// =============================================================================
// JOIN Snapshots
// =============================================================================

#[test]
fn snapshot_multiple_joins() {
    snapshot_format(
        "multiple_joins",
        "SELECT u.id, u.name, o.total, p.amount FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id RIGHT JOIN refunds r ON p.id = r.payment_id",
    );
}

#[test]
fn snapshot_join_complex_condition() {
    snapshot_format(
        "join_complex_condition",
        "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND (t1.c = t2.c OR t1.d = t2.d)",
    );
}

// =============================================================================
// CASE Expression Snapshots
// =============================================================================

#[test]
fn snapshot_complex_case() {
    snapshot_format(
        "complex_case",
        "SELECT id, CASE WHEN status = 'active' AND subscription = 'premium' THEN 'Premium Active' WHEN status = 'active' AND subscription = 'basic' THEN 'Basic Active' WHEN status = 'pending' THEN 'Pending Approval' WHEN status = 'suspended' THEN 'Suspended' ELSE 'Unknown Status' END AS status_description FROM users",
    );
}

#[test]
fn snapshot_nested_case() {
    snapshot_format(
        "nested_case",
        "SELECT CASE WHEN a > 0 THEN CASE WHEN b > 0 THEN 'both positive' ELSE 'a positive' END ELSE CASE WHEN b > 0 THEN 'b positive' ELSE 'both negative' END END FROM t",
    );
}

// =============================================================================
// Subquery Snapshots
// =============================================================================

#[test]
fn snapshot_subquery_in_from() {
    snapshot_format(
        "subquery_in_from",
        "SELECT sub.id, sub.total FROM (SELECT user_id AS id, SUM(amount) AS total FROM orders GROUP BY user_id) sub WHERE sub.total > 1000",
    );
}

#[test]
fn snapshot_subquery_in_where() {
    snapshot_format(
        "subquery_in_where",
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE created_at > '2024-01-01' AND total > 100)",
    );
}

#[test]
fn snapshot_correlated_subquery() {
    snapshot_format(
        "correlated_subquery",
        "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.total > 100)",
    );
}

// =============================================================================
// Window Function Snapshots
// =============================================================================

#[test]
fn snapshot_window_function() {
    snapshot_format(
        "window_function",
        "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY hire_date) AS dept_rank FROM employees",
    );
}

#[test]
fn snapshot_multiple_window_functions() {
    snapshot_format(
        "multiple_window_functions",
        "SELECT id, SUM(amount) OVER (PARTITION BY user_id ORDER BY created_at) AS running_total, AVG(amount) OVER (PARTITION BY user_id) AS avg_amount, RANK() OVER (PARTITION BY user_id ORDER BY amount DESC) AS amount_rank FROM orders",
    );
}

// =============================================================================
// Complex Query Snapshots
// =============================================================================

#[test]
fn snapshot_complex_analytics_query() {
    snapshot_format(
        "complex_analytics_query",
        r#"WITH monthly_sales AS (
    SELECT DATE_TRUNC('month', order_date) AS month, product_id, SUM(quantity) AS total_qty, SUM(amount) AS total_amount
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY 1, 2
), ranked_products AS (
    SELECT *, RANK() OVER (PARTITION BY month ORDER BY total_amount DESC) AS rank
    FROM monthly_sales
)
SELECT r.month, p.name AS product_name, r.total_qty, r.total_amount, r.rank
FROM ranked_products r
JOIN products p ON r.product_id = p.id
WHERE r.rank <= 10
ORDER BY r.month, r.rank"#,
    );
}

#[test]
fn snapshot_data_pipeline_query() {
    snapshot_format(
        "data_pipeline_query",
        "SELECT u.id, u.email, COALESCE(o.total_orders, 0) AS total_orders, COALESCE(o.total_spent, 0) AS total_spent, CASE WHEN o.total_spent > 10000 THEN 'platinum' WHEN o.total_spent > 5000 THEN 'gold' WHEN o.total_spent > 1000 THEN 'silver' ELSE 'bronze' END AS tier FROM users u LEFT JOIN (SELECT user_id, COUNT(*) AS total_orders, SUM(amount) AS total_spent FROM orders WHERE status = 'completed' GROUP BY user_id) o ON u.id = o.user_id WHERE u.active = true ORDER BY o.total_spent DESC NULLS LAST",
    );
}

// =============================================================================
// Jinja Snapshots
// =============================================================================

#[test]
fn snapshot_jinja_ref() {
    snapshot_format(
        "jinja_ref",
        "SELECT * FROM {{ ref('users') }} WHERE active = true",
    );
}

#[test]
fn snapshot_jinja_source() {
    snapshot_format(
        "jinja_source",
        "SELECT * FROM {{ source('raw', 'users') }}",
    );
}

#[test]
fn snapshot_jinja_config() {
    snapshot_format(
        "jinja_config",
        "{{ config(materialized='table', schema='analytics') }}\n\nSELECT * FROM users",
    );
}

#[test]
fn snapshot_jinja_if_block() {
    snapshot_format(
        "jinja_if_block",
        "SELECT id, name {% if include_email %}, email {% endif %} FROM users",
    );
}

#[test]
fn snapshot_jinja_for_loop() {
    snapshot_format(
        "jinja_for_loop",
        "SELECT {% for col in columns %} {{ col }} {% if not loop.last %}, {% endif %} {% endfor %} FROM t",
    );
}

#[test]
fn snapshot_dbt_utils() {
    snapshot_format(
        "dbt_utils",
        "SELECT {{ dbt_utils.star(ref('users')) }} FROM {{ ref('users') }}",
    );
}

// =============================================================================
// Snowflake-Specific Snapshots
// =============================================================================

#[test]
fn snapshot_qualify() {
    snapshot_format(
        "qualify",
        "SELECT * FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) = 1",
    );
}

#[test]
fn snapshot_flatten() {
    snapshot_format(
        "flatten",
        "SELECT f.value:name::STRING AS name FROM events, LATERAL FLATTEN(input => payload:items) f",
    );
}

#[test]
fn snapshot_semi_structured() {
    snapshot_format(
        "semi_structured",
        "SELECT data:user:name::VARCHAR AS user_name, data:user:email::VARCHAR AS user_email, data:items[0]:product_id::INT AS first_product FROM events",
    );
}

#[test]
fn snapshot_sample() {
    snapshot_format("sample", "SELECT * FROM large_table SAMPLE (10)");
}

#[test]
fn snapshot_pivot() {
    snapshot_format(
        "pivot",
        "SELECT * FROM sales_data PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')) AS p",
    );
}

// =============================================================================
// DML Snapshots
// =============================================================================

#[test]
fn snapshot_insert_values() {
    snapshot_format(
        "insert_values",
        "INSERT INTO users (id, name, email, created_at) VALUES (1, 'John Doe', 'john@example.com', CURRENT_TIMESTAMP), (2, 'Jane Smith', 'jane@example.com', CURRENT_TIMESTAMP)",
    );
}

#[test]
fn snapshot_insert_select() {
    snapshot_format(
        "insert_select",
        "INSERT INTO users_archive SELECT * FROM users WHERE deleted_at IS NOT NULL AND deleted_at < DATEADD('month', -6, CURRENT_DATE)",
    );
}

#[test]
fn snapshot_update() {
    snapshot_format(
        "update",
        "UPDATE users SET status = 'inactive', updated_at = CURRENT_TIMESTAMP WHERE last_login < DATEADD('year', -1, CURRENT_DATE) AND status = 'active'",
    );
}

#[test]
fn snapshot_delete() {
    snapshot_format(
        "delete",
        "DELETE FROM sessions WHERE expires_at < CURRENT_TIMESTAMP OR (user_id IN (SELECT id FROM users WHERE deleted = true))",
    );
}

#[test]
fn snapshot_merge() {
    snapshot_format(
        "merge",
        "MERGE INTO target_table t USING source_table s ON t.id = s.id WHEN MATCHED AND s.deleted = true THEN DELETE WHEN MATCHED THEN UPDATE SET t.name = s.name, t.value = s.value, t.updated_at = CURRENT_TIMESTAMP WHEN NOT MATCHED THEN INSERT (id, name, value, created_at) VALUES (s.id, s.name, s.value, CURRENT_TIMESTAMP)",
    );
}

// =============================================================================
// DDL Snapshots
// =============================================================================

#[test]
fn snapshot_create_table() {
    snapshot_format(
        "create_table",
        "CREATE TABLE users (id INT NOT NULL, name VARCHAR(255) NOT NULL, email VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))",
    );
}

#[test]
fn snapshot_create_view() {
    snapshot_format(
        "create_view",
        "CREATE OR REPLACE VIEW active_users AS SELECT id, name, email FROM users WHERE active = true AND deleted_at IS NULL",
    );
}
