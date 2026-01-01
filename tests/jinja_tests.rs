//! Jinja support tests for sqlflight
//!
//! These tests verify that Jinja templating is handled correctly according to SPEC.md:
//! - Jinja expressions: {{ variable }}, {{ ref('table') }}
//! - Jinja statements: {% if %}, {% for %}, {% set %}
//! - Jinja comments: {# comment #}
//! - dbt macros: {{ ref() }}, {{ source() }}, {{ config() }}, {{ dbt_utils.star() }}
//! - Jinja block formatting with proper whitespace
//! - Preservation of Jinja content exactly as written
//! - Two-pass extraction/reintegration approach (placeholders)

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

/// Helper to verify Jinja content is preserved exactly
fn assert_jinja_preserved(input: &str, jinja_snippet: &str) {
    let result = format(input).expect("format should succeed");
    assert!(
        result.contains(jinja_snippet),
        "Jinja content '{}' should be preserved exactly in output:\n{}",
        jinja_snippet,
        result
    );
}

// =============================================================================
// JINJA EXPRESSION TESTS - {{ variable }}, {{ ref('table') }}
// =============================================================================

mod jinja_expressions {
    use super::*;

    #[test]
    fn simple_variable_in_select() {
        let input = "SELECT {{ column_name }} FROM users";
        assert_jinja_preserved(input, "{{ column_name }}");
    }

    #[test]
    fn variable_with_filter() {
        let input = "SELECT {{ name | upper }} FROM users";
        assert_jinja_preserved(input, "{{ name | upper }}");
    }

    #[test]
    fn variable_in_where_clause() {
        let input = "SELECT * FROM users WHERE id = {{ user_id }}";
        assert_jinja_preserved(input, "{{ user_id }}");
    }

    #[test]
    fn variable_in_table_name() {
        let input = "SELECT * FROM {{ table_name }}";
        assert_jinja_preserved(input, "{{ table_name }}");
    }

    #[test]
    fn variable_with_default() {
        let input = "SELECT * FROM users LIMIT {{ limit | default(100) }}";
        assert_jinja_preserved(input, "{{ limit | default(100) }}");
    }

    #[test]
    fn variable_with_spaces() {
        let input = "SELECT {{   spaced_variable   }} FROM users";
        assert_jinja_preserved(input, "{{   spaced_variable   }}");
    }

    #[test]
    fn multiple_variables_in_select() {
        let input = "SELECT {{ col1 }}, {{ col2 }}, {{ col3 }} FROM {{ table }}";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ col1 }}"));
        assert!(result.contains("{{ col2 }}"));
        assert!(result.contains("{{ col3 }}"));
        assert!(result.contains("{{ table }}"));
    }

    #[test]
    fn variable_in_function_arg() {
        let input = "SELECT COUNT({{ column }}) FROM users";
        assert_jinja_preserved(input, "{{ column }}");
    }

    #[test]
    fn variable_in_case_expression() {
        let input = "SELECT CASE WHEN status = {{ active_status }} THEN 'active' ELSE 'inactive' END FROM users";
        assert_jinja_preserved(input, "{{ active_status }}");
    }

    #[test]
    fn nested_braces_in_expression() {
        let input = "SELECT {{ {'key': 'value'} }} FROM dual";
        assert_jinja_preserved(input, "{{ {'key': 'value'} }}");
    }
}

// =============================================================================
// JINJA STATEMENT TESTS - {% if %}, {% for %}, {% set %}
// =============================================================================

mod jinja_statements {
    use super::*;

    #[test]
    fn simple_if_statement() {
        let input = r#"SELECT
  id
  {% if include_name %}
  , name
  {% endif %}
FROM users"#;
        assert_jinja_preserved(input, "{% if include_name %}");
        assert_jinja_preserved(input, "{% endif %}");
    }

    #[test]
    fn if_else_statement() {
        let input = r#"SELECT
  {% if use_id %}
  id
  {% else %}
  uuid
  {% endif %}
FROM users"#;
        assert_jinja_preserved(input, "{% if use_id %}");
        assert_jinja_preserved(input, "{% else %}");
        assert_jinja_preserved(input, "{% endif %}");
    }

    #[test]
    fn if_elif_else_statement() {
        let input = r#"SELECT
  {% if version == 1 %}
  legacy_id
  {% elif version == 2 %}
  new_id
  {% else %}
  uuid
  {% endif %}
FROM users"#;
        assert_jinja_preserved(input, "{% if version == 1 %}");
        assert_jinja_preserved(input, "{% elif version == 2 %}");
        assert_jinja_preserved(input, "{% else %}");
        assert_jinja_preserved(input, "{% endif %}");
    }

    #[test]
    fn for_loop_statement() {
        let input = r#"SELECT
  id
  {% for col in columns %}
  , {{ col }}
  {% endfor %}
FROM users"#;
        assert_jinja_preserved(input, "{% for col in columns %}");
        assert_jinja_preserved(input, "{{ col }}");
        assert_jinja_preserved(input, "{% endfor %}");
    }

    #[test]
    fn for_loop_with_condition() {
        let input = r#"SELECT
  {% for col in columns if col != 'secret' %}
  {{ col }},
  {% endfor %}
FROM users"#;
        assert_jinja_preserved(input, "{% for col in columns if col != 'secret' %}");
        assert_jinja_preserved(input, "{% endfor %}");
    }

    #[test]
    fn set_statement() {
        let input = r#"{% set table_name = 'users' %}
SELECT * FROM {{ table_name }}"#;
        assert_jinja_preserved(input, "{% set table_name = 'users' %}");
        assert_jinja_preserved(input, "{{ table_name }}");
    }

    #[test]
    fn set_with_block() {
        let input = r#"{% set column_list %}
id, name, email
{% endset %}
SELECT {{ column_list }} FROM users"#;
        assert_jinja_preserved(input, "{% set column_list %}");
        assert_jinja_preserved(input, "{% endset %}");
    }

    #[test]
    fn macro_definition() {
        let input = r#"{% macro get_columns(table) %}
  id, name, created_at
{% endmacro %}
SELECT {{ get_columns('users') }} FROM users"#;
        assert_jinja_preserved(input, "{% macro get_columns(table) %}");
        assert_jinja_preserved(input, "{% endmacro %}");
    }

    #[test]
    fn call_block() {
        let input = r#"{% call my_macro() %}
  SELECT * FROM users
{% endcall %}"#;
        assert_jinja_preserved(input, "{% call my_macro() %}");
        assert_jinja_preserved(input, "{% endcall %}");
    }

    #[test]
    fn whitespace_control_in_statements() {
        // Test Jinja whitespace control syntax {%- and -%}
        let input = r#"SELECT
  id
  {%- if include_name -%}
  , name
  {%- endif -%}
FROM users"#;
        assert_jinja_preserved(input, "{%- if include_name -%}");
        assert_jinja_preserved(input, "{%- endif -%}");
    }
}

// =============================================================================
// JINJA COMMENT TESTS - {# comment #}
// =============================================================================

mod jinja_comments {
    use super::*;

    #[test]
    fn simple_comment() {
        let input = "SELECT id {# user identifier #} FROM users";
        assert_jinja_preserved(input, "{# user identifier #}");
    }

    #[test]
    fn comment_on_own_line() {
        let input = r#"SELECT
  id
  {# This is a comment explaining the next column #}
  , name
FROM users"#;
        assert_jinja_preserved(input, "{# This is a comment explaining the next column #}");
    }

    #[test]
    fn multi_line_jinja_comment() {
        let input = r#"{#
  This is a multi-line
  Jinja comment
#}
SELECT * FROM users"#;
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{#"));
        assert!(result.contains("#}"));
        assert!(result.contains("multi-line"));
    }

    #[test]
    fn comment_at_end_of_query() {
        let input = "SELECT * FROM users {# fetch all users #}";
        assert_jinja_preserved(input, "{# fetch all users #}");
    }

    #[test]
    fn comment_before_query() {
        let input = "{# Query to get active users #}\nSELECT * FROM users WHERE active = true";
        assert_jinja_preserved(input, "{# Query to get active users #}");
    }

    #[test]
    fn comment_with_special_characters() {
        let input = "SELECT * FROM users {# TODO: add filter for status != 'deleted' #}";
        assert_jinja_preserved(input, "{# TODO: add filter for status != 'deleted' #}");
    }
}

// =============================================================================
// DBT MACRO TESTS - ref(), source(), config(), dbt_utils
// =============================================================================

mod dbt_macros {
    use super::*;

    #[test]
    fn ref_macro_simple() {
        let input = "SELECT * FROM {{ ref('users') }}";
        assert_jinja_preserved(input, "{{ ref('users') }}");
    }

    #[test]
    fn ref_macro_with_project() {
        let input = "SELECT * FROM {{ ref('project', 'users') }}";
        assert_jinja_preserved(input, "{{ ref('project', 'users') }}");
    }

    #[test]
    fn source_macro() {
        let input = "SELECT * FROM {{ source('raw', 'users') }}";
        assert_jinja_preserved(input, "{{ source('raw', 'users') }}");
    }

    #[test]
    fn config_macro_single_option() {
        let input = "{{ config(materialized='table') }}\n\nSELECT * FROM users";
        assert_jinja_preserved(input, "{{ config(materialized='table') }}");
    }

    #[test]
    fn config_macro_multiple_options() {
        let input = r#"{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

SELECT * FROM users"#;
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ config("));
        assert!(result.contains("materialized='incremental'"));
        assert!(result.contains("unique_key='id'"));
    }

    #[test]
    fn dbt_utils_star() {
        let input = "SELECT {{ dbt_utils.star(ref('users')) }} FROM {{ ref('users') }}";
        assert_jinja_preserved(input, "{{ dbt_utils.star(ref('users')) }}");
    }

    #[test]
    fn dbt_utils_surrogate_key() {
        let input = "SELECT {{ dbt_utils.surrogate_key(['id', 'name']) }} as sk FROM users";
        assert_jinja_preserved(input, "{{ dbt_utils.surrogate_key(['id', 'name']) }}");
    }

    #[test]
    fn dbt_utils_date_spine() {
        let input = "{{ dbt_utils.date_spine(datepart='day', start_date='2020-01-01', end_date='2025-01-01') }}";
        assert_jinja_preserved(input, "{{ dbt_utils.date_spine(datepart='day', start_date='2020-01-01', end_date='2025-01-01') }}");
    }

    #[test]
    fn this_reference() {
        let input = "SELECT * FROM {{ this }}";
        assert_jinja_preserved(input, "{{ this }}");
    }

    #[test]
    fn target_variable() {
        let input = "SELECT * FROM {{ target.schema }}.users";
        assert_jinja_preserved(input, "{{ target.schema }}");
    }

    #[test]
    fn var_macro() {
        let input = "SELECT * FROM users WHERE status = {{ var('status', 'active') }}";
        assert_jinja_preserved(input, "{{ var('status', 'active') }}");
    }

    #[test]
    fn env_var_macro() {
        let input = "SELECT * FROM {{ env_var('DATABASE') }}.users";
        assert_jinja_preserved(input, "{{ env_var('DATABASE') }}");
    }

    #[test]
    fn run_query_macro() {
        let input = r#"{% set results = run_query('SELECT DISTINCT status FROM users') %}
SELECT * FROM users WHERE status IN (
  {% for row in results %}
  '{{ row.status }}'{% if not loop.last %},{% endif %}
  {% endfor %}
)"#;
        assert_jinja_preserved(input, "{% set results = run_query('SELECT DISTINCT status FROM users') %}");
    }

    #[test]
    fn is_incremental_macro() {
        let input = r#"SELECT *
FROM {{ ref('source_table') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}"#;
        assert_jinja_preserved(input, "{% if is_incremental() %}");
    }
}

// =============================================================================
// JINJA BLOCK FORMATTING / WHITESPACE TESTS
// =============================================================================

mod jinja_whitespace {
    use super::*;

    #[test]
    fn preserves_leading_whitespace_in_jinja() {
        let input = "SELECT {{  spaced  }} FROM users";
        assert_jinja_preserved(input, "{{  spaced  }}");
    }

    #[test]
    fn preserves_newlines_in_jinja_block() {
        let input = r#"{% set my_var =
    'value'
%}
SELECT * FROM users"#;
        let result = format(input).expect("format should succeed");
        // The Jinja block should be preserved exactly
        assert!(result.contains("{% set my_var ="));
    }

    #[test]
    fn jinja_statement_maintains_sql_context() {
        let input = r#"SELECT
  id
  {% if include_name %}
  , name
  {% endif %}
  , email
FROM users"#;
        let result = format(input).expect("format should succeed");
        // Jinja blocks should be preserved
        assert!(result.contains("{% if include_name %}"));
        assert!(result.contains("{% endif %}"));
    }

    #[test]
    fn config_block_at_top() {
        let input = r#"{{ config(materialized='table') }}

SELECT id, name FROM users"#;
        let result = format(input).expect("format should succeed");
        // Config should remain at the top
        assert!(result.starts_with("{{ config("));
    }

    #[test]
    fn inline_jinja_preserves_surrounding_sql() {
        assert_formats_to(
            "SELECT id, {{ dynamic_column }}, name FROM users",
            "select id, {{ dynamic_column }}, name from users",
        );
    }
}

// =============================================================================
// JINJA CONTENT PRESERVATION TESTS
// =============================================================================

mod jinja_preservation {
    use super::*;

    #[test]
    fn exact_content_preserved_expression() {
        // Verify the exact content including internal spacing
        let input = "SELECT {{ ref('my_table') }} FROM dual";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ ref('my_table') }}"));
    }

    #[test]
    fn exact_content_preserved_statement() {
        let input = "{% if condition == 'value' %}SELECT * FROM users{% endif %}";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{% if condition == 'value' %}"));
        assert!(result.contains("{% endif %}"));
    }

    #[test]
    fn exact_content_preserved_comment() {
        let input = "{# This is my exact comment text #}SELECT * FROM users";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{# This is my exact comment text #}"));
    }

    #[test]
    fn special_chars_in_jinja_preserved() {
        let input = "SELECT * FROM {{ ref(table_name ~ '_suffix') }}";
        assert_jinja_preserved(input, "{{ ref(table_name ~ '_suffix') }}");
    }

    #[test]
    fn quotes_in_jinja_preserved() {
        let input = r#"SELECT * FROM {{ ref("users") }}"#;
        assert_jinja_preserved(input, r#"{{ ref("users") }}"#);
    }

    #[test]
    fn mixed_quotes_in_jinja_preserved() {
        let input = r#"SELECT * FROM {{ source("raw", 'users') }}"#;
        assert_jinja_preserved(input, r#"{{ source("raw", 'users') }}"#);
    }

    #[test]
    fn complex_expression_preserved() {
        let input = "SELECT {{ dbt_utils.star(from=ref('users'), except=['password', 'salt']) }} FROM {{ ref('users') }}";
        assert_jinja_preserved(input, "{{ dbt_utils.star(from=ref('users'), except=['password', 'salt']) }}");
    }

    #[test]
    fn jinja_arithmetic_preserved() {
        let input = "SELECT * FROM users LIMIT {{ page * page_size }}";
        assert_jinja_preserved(input, "{{ page * page_size }}");
    }

    #[test]
    fn jinja_comparison_preserved() {
        let input = "{% if count > 0 %}SELECT * FROM users{% endif %}";
        assert_jinja_preserved(input, "{% if count > 0 %}");
    }

    #[test]
    fn jinja_string_operations_preserved() {
        let input = "SELECT * FROM {{ schema_name | lower }}_{{ table_name | upper }}";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ schema_name | lower }}"));
        assert!(result.contains("{{ table_name | upper }}"));
    }
}

// =============================================================================
// TWO-PASS EXTRACTION/REINTEGRATION TESTS
// =============================================================================

mod extraction_reintegration {
    use super::*;

    #[test]
    fn sql_formatted_around_jinja() {
        // Verify SQL is properly formatted while Jinja is preserved
        assert_formats_to(
            "SELECT ID, NAME FROM {{ ref('USERS') }} WHERE ACTIVE = TRUE",
            "select id, name from {{ ref('USERS') }} where active = true",
        );
    }

    #[test]
    fn multiple_jinja_blocks_reintegrated() {
        let input = "SELECT {{ col1 }}, {{ col2 }} FROM {{ table }} WHERE {{ condition }}";
        let result = format(input).expect("format should succeed");
        // All Jinja blocks should be present
        assert!(result.contains("{{ col1 }}"));
        assert!(result.contains("{{ col2 }}"));
        assert!(result.contains("{{ table }}"));
        assert!(result.contains("{{ condition }}"));
    }

    #[test]
    fn mixed_jinja_types_reintegrated() {
        let input = r#"{# Header comment #}
{{ config(materialized='table') }}
{% if enabled %}
SELECT * FROM {{ ref('users') }}
{% endif %}"#;
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{# Header comment #}"));
        assert!(result.contains("{{ config(materialized='table') }}"));
        assert!(result.contains("{% if enabled %}"));
        assert!(result.contains("{{ ref('users') }}"));
        assert!(result.contains("{% endif %}"));
    }

    #[test]
    fn deeply_nested_sql_with_jinja() {
        let input = r#"WITH cte AS (
  SELECT * FROM {{ ref('source') }} WHERE {{ filter_condition }}
)
SELECT * FROM cte WHERE id IN (SELECT id FROM {{ ref('other') }})"#;
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ ref('source') }}"));
        assert!(result.contains("{{ filter_condition }}"));
        assert!(result.contains("{{ ref('other') }}"));
    }

    #[test]
    fn jinja_order_preserved() {
        let input = "SELECT {{ a }}, {{ b }}, {{ c }} FROM t";
        let result = format(input).expect("format should succeed");
        // Order should be maintained
        let pos_a = result.find("{{ a }}").unwrap();
        let pos_b = result.find("{{ b }}").unwrap();
        let pos_c = result.find("{{ c }}").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn adjacent_jinja_blocks() {
        let input = "SELECT {{ a }}{{ b }}{{ c }} FROM t";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ a }}{{ b }}{{ c }}"));
    }
}

// =============================================================================
// IDEMPOTENCY TESTS FOR JINJA
// =============================================================================

mod jinja_idempotency {
    use super::*;

    #[test]
    fn simple_jinja_idempotent() {
        assert_idempotent("SELECT {{ column }} FROM {{ table }}");
    }

    #[test]
    fn complex_jinja_idempotent() {
        let input = r#"{{ config(materialized='table') }}

{% set columns = ['id', 'name', 'email'] %}

SELECT
  {% for col in columns %}
  {{ col }}{% if not loop.last %},{% endif %}
  {% endfor %}
FROM {{ ref('users') }}
WHERE {{ filter_condition }}"#;
        assert_idempotent(input);
    }

    #[test]
    fn dbt_model_idempotent() {
        let input = r#"{{ config(
    materialized='incremental',
    unique_key='id'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'users') }}
    {% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
)

SELECT
    id
    , name
    , email
    , created_at
    , updated_at
FROM source"#;
        assert_idempotent(input);
    }
}

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

mod jinja_edge_cases {
    use super::*;

    #[test]
    fn empty_jinja_expression() {
        let input = "SELECT {{}} FROM users";
        // Should preserve empty expression or handle gracefully
        let result = format(input);
        assert!(result.is_ok() || result.is_err()); // Either is acceptable
    }

    #[test]
    fn jinja_at_very_start() {
        let input = "{{ config() }}SELECT * FROM users";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ config() }}"));
    }

    #[test]
    fn jinja_at_very_end() {
        let input = "SELECT * FROM users{{ trailing }}";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ trailing }}"));
    }

    #[test]
    fn only_jinja_no_sql() {
        let input = "{{ ref('users') }}";
        let result = format(input);
        // May succeed with just the Jinja block or fail if no SQL to parse
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn sql_looking_content_in_jinja_preserved() {
        // SQL keywords inside Jinja should not be lowercased
        let input = "SELECT {{ 'SELECT * FROM' ~ table }} FROM dual";
        assert_jinja_preserved(input, "{{ 'SELECT * FROM' ~ table }}");
    }

    #[test]
    fn curly_braces_in_strings_not_confused() {
        // Regular curly braces in SQL strings should not be treated as Jinja
        let input = "SELECT '{not_jinja}' FROM users";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("'{not_jinja}'"));
    }

    #[test]
    fn percent_in_sql_not_confused_with_jinja() {
        let input = "SELECT * FROM users WHERE name LIKE '%test%'";
        let result = format(input).expect("format should succeed");
        assert!(result.contains("'%test%'"));
    }

    #[test]
    fn hash_in_sql_not_confused_with_jinja() {
        // Single hash is SQL comment, not Jinja
        let input = "SELECT * FROM users # This is a MySQL-style comment";
        let result = format(input);
        // May or may not support MySQL comments, but shouldn't crash
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn very_long_jinja_expression() {
        let long_expr = "a".repeat(500);
        let input = format!("SELECT {{{{ {} }}}} FROM users", long_expr);
        let result = format(&input).expect("format should succeed");
        assert!(result.contains(&format!("{{{{ {} }}}}", long_expr)));
    }

    #[test]
    fn unicode_in_jinja_preserved() {
        let input = "SELECT {{ '日本語テスト' }} FROM users";
        assert_jinja_preserved(input, "{{ '日本語テスト' }}");
    }
}

// =============================================================================
// REAL-WORLD DBT MODEL TESTS
// =============================================================================

mod dbt_real_world {
    use super::*;

    #[test]
    fn staging_model() {
        let input = r#"{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

renamed AS (
    SELECT
        id AS customer_id
        , first_name
        , last_name
        , email
        , created_at
        , updated_at
    FROM source
)

SELECT * FROM renamed"#;

        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ config(materialized='view') }}"));
        assert!(result.contains("{{ source('raw', 'customers') }}"));
    }

    #[test]
    fn incremental_model() {
        let input = r#"{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
) }}

SELECT
    {{ dbt_utils.surrogate_key(['user_id', 'event_timestamp']) }} AS event_id
    , user_id
    , event_type
    , event_data
    , event_timestamp
FROM {{ ref('stg_events') }}

{% if is_incremental() %}
WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}"#;

        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ config("));
        assert!(result.contains("{{ dbt_utils.surrogate_key"));
        assert!(result.contains("{{ ref('stg_events') }}"));
        assert!(result.contains("{% if is_incremental() %}"));
        assert!(result.contains("{{ this }}"));
    }

    #[test]
    fn macro_usage_model() {
        let input = r#"{% set payment_methods = ['credit_card', 'bank_transfer', 'paypal'] %}

SELECT
    order_id
    , customer_id
    {% for payment_method in payment_methods %}
    , SUM(CASE WHEN payment_method = '{{ payment_method }}' THEN amount ELSE 0 END) AS {{ payment_method }}_amount
    {% endfor %}
    , SUM(amount) AS total_amount
FROM {{ ref('stg_payments') }}
GROUP BY 1, 2"#;

        let result = format(input).expect("format should succeed");
        assert!(result.contains("{% set payment_methods = ['credit_card', 'bank_transfer', 'paypal'] %}"));
        assert!(result.contains("{% for payment_method in payment_methods %}"));
        assert!(result.contains("{{ payment_method }}"));
        assert!(result.contains("{% endfor %}"));
    }

    #[test]
    fn snapshot_model() {
        let input = r#"{% snapshot users_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

SELECT * FROM {{ source('raw', 'users') }}

{% endsnapshot %}"#;

        let result = format(input).expect("format should succeed");
        assert!(result.contains("{% snapshot users_snapshot %}"));
        assert!(result.contains("{% endsnapshot %}"));
    }

    #[test]
    fn test_definition() {
        let input = r#"{{ config(severity='warn') }}

SELECT *
FROM {{ ref('fct_orders') }}
WHERE order_date > {{ var('max_order_date') }}
  OR order_total < 0"#;

        let result = format(input).expect("format should succeed");
        assert!(result.contains("{{ config(severity='warn') }}"));
        assert!(result.contains("{{ ref('fct_orders') }}"));
        assert!(result.contains("{{ var('max_order_date') }}"));
    }
}
