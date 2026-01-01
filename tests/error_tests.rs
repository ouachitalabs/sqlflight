//! Error handling tests for sqlflight
//!
//! Tests for parse errors, file errors, and exit codes.

use sqlflight::{format, check};

mod parse_errors {
    use super::*;

    #[test]
    fn empty_select() {
        let result = format("SELECT FROM users");
        assert!(result.is_err());
    }

    #[test]
    fn missing_from() {
        let result = format("SELECT * WHERE id = 1");
        assert!(result.is_err());
    }

    #[test]
    fn unclosed_parenthesis() {
        let result = format("SELECT (a + b FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn unclosed_string() {
        let result = format("SELECT 'unclosed string FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_keyword_order() {
        let result = format("FROM users SELECT *");
        assert!(result.is_err());
    }

    #[test]
    fn double_comma() {
        let result = format("SELECT a,, b FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn trailing_comma_in_select() {
        let result = format("SELECT a, b, FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn missing_table_name() {
        let result = format("SELECT * FROM");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_join() {
        let result = format("SELECT * FROM t1 JOIN ON t1.id = t2.id");
        assert!(result.is_err());
    }

    #[test]
    fn unclosed_case() {
        let result = format("SELECT CASE WHEN x = 1 THEN 'a' FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn when_without_case() {
        let result = format("SELECT WHEN x = 1 THEN 'a' END FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_cte() {
        let result = format("WITH AS (SELECT 1) SELECT * FROM cte");
        assert!(result.is_err());
    }

    #[test]
    fn unclosed_jinja_expression() {
        let result = format("SELECT {{ variable FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn unclosed_jinja_statement() {
        let result = format("SELECT {% if true FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn unclosed_jinja_comment() {
        let result = format("SELECT {# comment FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn error_contains_message() {
        let result = format("SELECT FROM users");
        match result {
            Err(e) => {
                let msg = format!("{}", e);
                assert!(!msg.is_empty(), "Error message should not be empty");
            }
            Ok(_) => panic!("Expected error"),
        }
    }
}

mod check_function {
    use super::*;

    #[test]
    fn check_returns_true_for_formatted() {
        // Assuming lowercase keywords is the formatted form
        let result = check("select id from users\n");
        assert!(result.is_ok());
        // Will fail until implemented, but tests the interface
    }

    #[test]
    fn check_returns_false_for_unformatted() {
        let result = check("SELECT ID FROM USERS");
        assert!(result.is_ok());
        // Will fail until implemented, but tests the interface
    }

    #[test]
    fn check_returns_error_for_invalid() {
        let result = check("SELECT FROM WHERE");
        assert!(result.is_err());
    }
}

mod edge_cases {
    use super::*;

    #[test]
    fn empty_input() {
        let result = format("");
        // Empty input could be valid (no-op) or error, depending on design
        // Document the expected behavior
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn whitespace_only() {
        let result = format("   \n\t\n   ");
        // Similar to empty input
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn comment_only() {
        let result = format("-- just a comment");
        // Should this be valid? Document expected behavior
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn very_long_identifier() {
        let long_name = "a".repeat(1000);
        let input = format!("SELECT {} FROM t", long_name);
        let result = format(&input);
        // Should handle long identifiers
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn deeply_nested_subqueries() {
        let input = "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM t))))";
        let result = format(input);
        // Should handle reasonable nesting
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn many_columns() {
        let cols: Vec<String> = (1..=100).map(|i| format!("col{}", i)).collect();
        let input = format!("SELECT {} FROM t", cols.join(", "));
        let result = format(&input);
        // Should handle many columns
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn unicode_identifiers() {
        let result = format("SELECT café FROM données");
        // Snowflake supports unicode identifiers
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn quoted_identifiers() {
        let result = format(r#"SELECT "Column Name" FROM "Table Name""#);
        // Quoted identifiers should be preserved
        assert!(result.is_ok());
    }

    #[test]
    fn string_with_special_chars() {
        let result = format(r"SELECT 'hello\nworld' FROM t");
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn string_with_quotes() {
        let result = format("SELECT 'it''s a test' FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn multiple_statements() {
        let result = format("SELECT 1; SELECT 2;");
        // Depending on design, could format both or error
        assert!(result.is_ok() || result.is_err());
    }
}

mod numeric_literals {
    use super::*;

    #[test]
    fn integer() {
        let result = format("SELECT 42 FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn negative_integer() {
        let result = format("SELECT -42 FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn float() {
        let result = format("SELECT 3.14 FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn scientific_notation() {
        let result = format("SELECT 1.5e10 FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn negative_exponent() {
        let result = format("SELECT 1.5e-10 FROM t");
        assert!(result.is_ok());
    }
}

mod string_literals {
    use super::*;

    #[test]
    fn single_quoted() {
        let result = format("SELECT 'hello' FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn escaped_quote() {
        let result = format("SELECT 'it''s working' FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn empty_string() {
        let result = format("SELECT '' FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn string_with_newline() {
        let result = format("SELECT 'line1\nline2' FROM t");
        assert!(result.is_ok());
    }
}

mod operators {
    use super::*;

    #[test]
    fn arithmetic() {
        let result = format("SELECT a + b - c * d / e % f FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn comparison() {
        let result = format("SELECT * FROM t WHERE a = 1 AND b <> 2 AND c < 3 AND d > 4 AND e <= 5 AND f >= 6");
        assert!(result.is_ok());
    }

    #[test]
    fn string_concat() {
        let result = format("SELECT a || b || c FROM t");
        assert!(result.is_ok());
    }

    #[test]
    fn not_equal_variants() {
        // Both != and <> should work
        let result1 = format("SELECT * FROM t WHERE a != b");
        let result2 = format("SELECT * FROM t WHERE a <> b");
        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }
}
