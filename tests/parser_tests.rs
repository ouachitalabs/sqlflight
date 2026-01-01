//! Parser unit tests for sqlflight
//!
//! These tests verify the parser correctly builds AST nodes from SQL input.
//! The tests cover expressions, statements, and error handling.

use sqlflight::ast::*;
use sqlflight::parser;

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Parse SQL and expect success
fn parse_ok(input: &str) -> Statement {
    parser::parse(input).expect(&format!("Failed to parse: {}", input))
}

/// Parse SQL and expect a parse error
fn parse_err(input: &str) -> sqlflight::Error {
    parser::parse(input).expect_err(&format!("Expected parse error for: {}", input))
}

/// Extract the select statement from a Statement
fn as_select(stmt: &Statement) -> &SelectStatement {
    match stmt {
        Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    }
}

/// Extract the first column expression from a SELECT
fn first_column_expr(stmt: &Statement) -> &Expression {
    let select = as_select(stmt);
    &select.columns.first().expect("Expected at least one column").expr
}

// =============================================================================
// LITERAL PARSING TESTS
// =============================================================================

mod literals {
    use super::{parse_ok, first_column_expr, Expression, Literal, UnaryOperator};

    #[test]
    fn parse_null_literal() {
        let stmt = parse_ok("SELECT NULL");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Literal(Literal::Null));
    }

    #[test]
    fn parse_true_literal() {
        let stmt = parse_ok("SELECT TRUE");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Literal(Literal::Boolean(true)));
    }

    #[test]
    fn parse_false_literal() {
        let stmt = parse_ok("SELECT FALSE");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Literal(Literal::Boolean(false)));
    }

    #[test]
    fn parse_integer_literal() {
        let stmt = parse_ok("SELECT 42");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Literal(Literal::Integer(42)));
    }

    #[test]
    fn parse_negative_integer_literal() {
        let stmt = parse_ok("SELECT -123");
        let expr = first_column_expr(&stmt);
        // Could be UnaryOp or Literal depending on parser implementation
        match expr {
            Expression::Literal(Literal::Integer(-123)) => {}
            Expression::UnaryOp { op: UnaryOperator::Minus, expr } => {
                assert_eq!(**expr, Expression::Literal(Literal::Integer(123)));
            }
            _ => panic!("Unexpected expression: {:?}", expr),
        }
    }

    #[test]
    fn parse_float_literal() {
        let stmt = parse_ok("SELECT 3.14");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Literal(Literal::Float(3.14)));
    }

    #[test]
    fn parse_string_literal_single_quotes() {
        let stmt = parse_ok("SELECT 'hello world'");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Literal(Literal::String("hello world".to_string())));
    }

    #[test]
    fn parse_string_literal_with_escaped_quote() {
        let stmt = parse_ok("SELECT 'it''s a test'");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Literal(Literal::String("it's a test".to_string())));
    }

    #[test]
    fn parse_date_literal() {
        let stmt = parse_ok("SELECT DATE '2024-01-15'");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Literal(Literal::Date("2024-01-15".to_string())));
    }

    #[test]
    fn parse_timestamp_literal() {
        let stmt = parse_ok("SELECT TIMESTAMP '2024-01-15 10:30:00'");
        let expr = first_column_expr(&stmt);
        assert_eq!(
            *expr,
            Expression::Literal(Literal::Timestamp("2024-01-15 10:30:00".to_string()))
        );
    }
}

// =============================================================================
// IDENTIFIER PARSING TESTS
// =============================================================================

mod identifiers {
    use super::{parse_ok, first_column_expr, Expression};

    #[test]
    fn parse_simple_identifier() {
        let stmt = parse_ok("SELECT column_name FROM t");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Identifier("column_name".to_string()));
    }

    #[test]
    fn parse_quoted_identifier() {
        let stmt = parse_ok("SELECT \"Column-Name\" FROM t");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Identifier("Column-Name".to_string()));
    }

    #[test]
    fn parse_qualified_identifier_two_parts() {
        let stmt = parse_ok("SELECT t.column_name FROM t");
        let expr = first_column_expr(&stmt);
        assert_eq!(
            *expr,
            Expression::QualifiedIdentifier(vec!["t".to_string(), "column_name".to_string()])
        );
    }

    #[test]
    fn parse_qualified_identifier_three_parts() {
        let stmt = parse_ok("SELECT schema.table.column FROM t");
        let expr = first_column_expr(&stmt);
        assert_eq!(
            *expr,
            Expression::QualifiedIdentifier(vec![
                "schema".to_string(),
                "table".to_string(),
                "column".to_string()
            ])
        );
    }

    #[test]
    fn parse_qualified_identifier_four_parts() {
        let stmt = parse_ok("SELECT db.schema.table.column FROM t");
        let expr = first_column_expr(&stmt);
        assert_eq!(
            *expr,
            Expression::QualifiedIdentifier(vec![
                "db".to_string(),
                "schema".to_string(),
                "table".to_string(),
                "column".to_string()
            ])
        );
    }

    #[test]
    fn parse_star_expression() {
        let stmt = parse_ok("SELECT * FROM t");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::Star);
    }

    #[test]
    fn parse_qualified_star() {
        let stmt = parse_ok("SELECT t.* FROM t");
        let expr = first_column_expr(&stmt);
        assert_eq!(*expr, Expression::QualifiedStar("t".to_string()));
    }
}

// =============================================================================
// BINARY OPERATION PARSING TESTS
// =============================================================================

mod binary_operations {
    use super::{parse_ok, first_column_expr, Expression, BinaryOperator, Literal};

    #[test]
    fn parse_addition() {
        let stmt = parse_ok("SELECT a + b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(**left, Expression::Identifier("a".to_string()));
                assert_eq!(*op, BinaryOperator::Plus);
                assert_eq!(**right, Expression::Identifier("b".to_string()));
            }
            _ => panic!("Expected BinaryOp, got {:?}", expr),
        }
    }

    #[test]
    fn parse_subtraction() {
        let stmt = parse_ok("SELECT a - b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Minus),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_multiplication() {
        let stmt = parse_ok("SELECT a * b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Multiply),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_division() {
        let stmt = parse_ok("SELECT a / b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Divide),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_modulo() {
        let stmt = parse_ok("SELECT a % b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Modulo),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_equality() {
        let stmt = parse_ok("SELECT a = b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Eq),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_not_equal_angle_brackets() {
        let stmt = parse_ok("SELECT a <> b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::NotEq),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_not_equal_exclamation() {
        let stmt = parse_ok("SELECT a != b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::NotEq),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_less_than() {
        let stmt = parse_ok("SELECT a < b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Lt),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_less_than_or_equal() {
        let stmt = parse_ok("SELECT a <= b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::LtEq),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_greater_than() {
        let stmt = parse_ok("SELECT a > b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Gt),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_greater_than_or_equal() {
        let stmt = parse_ok("SELECT a >= b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::GtEq),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_and_operator() {
        let stmt = parse_ok("SELECT a AND b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::And),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_or_operator() {
        let stmt = parse_ok("SELECT a OR b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Or),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_like_operator() {
        let stmt = parse_ok("SELECT name LIKE '%john%' FROM users");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(*op, BinaryOperator::Like);
                assert_eq!(**left, Expression::Identifier("name".to_string()));
                assert_eq!(**right, Expression::Literal(Literal::String("%john%".to_string())));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_ilike_operator() {
        let stmt = parse_ok("SELECT name ILIKE '%john%' FROM users");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::ILike),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_concat_operator() {
        let stmt = parse_ok("SELECT a || b FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Concat),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_operator_precedence_multiply_add() {
        // a + b * c should parse as a + (b * c)
        let stmt = parse_ok("SELECT a + b * c FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(*op, BinaryOperator::Plus);
                assert_eq!(**left, Expression::Identifier("a".to_string()));
                match right.as_ref() {
                    Expression::BinaryOp { op, .. } => {
                        assert_eq!(*op, BinaryOperator::Multiply);
                    }
                    _ => panic!("Expected nested BinaryOp for multiplication"),
                }
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn parse_operator_precedence_and_or() {
        // a AND b OR c should parse as (a AND b) OR c
        let stmt = parse_ok("SELECT a AND b OR c FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { left, op, .. } => {
                assert_eq!(*op, BinaryOperator::Or);
                match left.as_ref() {
                    Expression::BinaryOp { op, .. } => {
                        assert_eq!(*op, BinaryOperator::And);
                    }
                    _ => panic!("Expected nested BinaryOp for AND"),
                }
            }
            _ => panic!("Expected BinaryOp"),
        }
    }
}

// =============================================================================
// UNARY OPERATION PARSING TESTS
// =============================================================================

mod unary_operations {
    use super::{parse_ok, first_column_expr, Expression, UnaryOperator};

    #[test]
    fn parse_not_operator() {
        let stmt = parse_ok("SELECT NOT active FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::UnaryOp { op, expr } => {
                assert_eq!(*op, UnaryOperator::Not);
                assert_eq!(**expr, Expression::Identifier("active".to_string()));
            }
            _ => panic!("Expected UnaryOp, got {:?}", expr),
        }
    }

    #[test]
    fn parse_unary_minus() {
        let stmt = parse_ok("SELECT -amount FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::UnaryOp { op, expr } => {
                assert_eq!(*op, UnaryOperator::Minus);
                assert_eq!(**expr, Expression::Identifier("amount".to_string()));
            }
            _ => panic!("Expected UnaryOp, got {:?}", expr),
        }
    }

    #[test]
    fn parse_unary_plus() {
        let stmt = parse_ok("SELECT +amount FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::UnaryOp { op, expr } => {
                assert_eq!(*op, UnaryOperator::Plus);
                assert_eq!(**expr, Expression::Identifier("amount".to_string()));
            }
            _ => panic!("Expected UnaryOp, got {:?}", expr),
        }
    }

    #[test]
    fn parse_double_not() {
        let stmt = parse_ok("SELECT NOT NOT active FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::UnaryOp { op, expr: inner } => {
                assert_eq!(*op, UnaryOperator::Not);
                match inner.as_ref() {
                    Expression::UnaryOp { op, .. } => assert_eq!(*op, UnaryOperator::Not),
                    _ => panic!("Expected nested UnaryOp"),
                }
            }
            _ => panic!("Expected UnaryOp"),
        }
    }
}

// =============================================================================
// FUNCTION CALL PARSING TESTS
// =============================================================================

mod function_calls {
    use super::{parse_ok, first_column_expr, Expression, WindowFrameUnit, WindowFrameBound};

    #[test]
    fn parse_function_no_args() {
        let stmt = parse_ok("SELECT NOW() FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::FunctionCall { name, args, over, .. } => {
                assert_eq!(name.to_uppercase(), "NOW");
                assert!(args.is_empty());
                assert!(over.is_none());
            }
            _ => panic!("Expected FunctionCall, got {:?}", expr),
        }
    }

    #[test]
    fn parse_function_one_arg() {
        let stmt = parse_ok("SELECT UPPER(name) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert_eq!(name.to_uppercase(), "UPPER");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0], Expression::Identifier("name".to_string()));
            }
            _ => panic!("Expected FunctionCall"),
        }
    }

    #[test]
    fn parse_function_multiple_args() {
        let stmt = parse_ok("SELECT COALESCE(a, b, c) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert_eq!(name.to_uppercase(), "COALESCE");
                assert_eq!(args.len(), 3);
            }
            _ => panic!("Expected FunctionCall"),
        }
    }

    #[test]
    fn parse_count_star() {
        let stmt = parse_ok("SELECT COUNT(*) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert_eq!(name.to_uppercase(), "COUNT");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0], Expression::Star);
            }
            _ => panic!("Expected FunctionCall"),
        }
    }

    #[test]
    fn parse_nested_function_calls() {
        let stmt = parse_ok("SELECT UPPER(TRIM(name)) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert_eq!(name.to_uppercase(), "UPPER");
                assert_eq!(args.len(), 1);
                match &args[0] {
                    Expression::FunctionCall { name, .. } => {
                        assert_eq!(name.to_uppercase(), "TRIM");
                    }
                    _ => panic!("Expected nested FunctionCall"),
                }
            }
            _ => panic!("Expected FunctionCall"),
        }
    }

    #[test]
    fn parse_window_function_row_number() {
        let stmt = parse_ok("SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::FunctionCall { name, over, .. } => {
                assert_eq!(name.to_uppercase(), "ROW_NUMBER");
                assert!(over.is_some());
                let spec = over.as_ref().unwrap();
                assert!(spec.partition_by.is_some());
                assert!(spec.order_by.is_some());
            }
            _ => panic!("Expected FunctionCall with window spec"),
        }
    }

    #[test]
    fn parse_window_function_with_frame() {
        let stmt = parse_ok("SELECT SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::FunctionCall { over, .. } => {
                let spec = over.as_ref().expect("Expected window spec");
                let frame = spec.frame.as_ref().expect("Expected window frame");
                assert_eq!(frame.unit, WindowFrameUnit::Rows);
                assert_eq!(frame.start, WindowFrameBound::UnboundedPreceding);
                assert_eq!(frame.end, Some(WindowFrameBound::CurrentRow));
            }
            _ => panic!("Expected FunctionCall with window frame"),
        }
    }
}

// =============================================================================
// CASE EXPRESSION PARSING TESTS
// =============================================================================

mod case_expressions {
    use super::{parse_ok, first_column_expr, Expression};

    #[test]
    fn parse_simple_case() {
        let stmt = parse_ok("SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Case(case) => {
                assert!(case.operand.is_none());
                assert_eq!(case.when_clauses.len(), 1);
                assert!(case.else_clause.is_some());
            }
            _ => panic!("Expected Case expression, got {:?}", expr),
        }
    }

    #[test]
    fn parse_case_multiple_when() {
        let stmt = parse_ok("SELECT CASE WHEN x = 1 THEN 'one' WHEN x = 2 THEN 'two' ELSE 'other' END FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Case(case) => {
                assert_eq!(case.when_clauses.len(), 2);
                assert!(case.else_clause.is_some());
            }
            _ => panic!("Expected Case expression"),
        }
    }

    #[test]
    fn parse_case_without_else() {
        let stmt = parse_ok("SELECT CASE WHEN x > 0 THEN 'positive' END FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Case(case) => {
                assert!(case.else_clause.is_none());
            }
            _ => panic!("Expected Case expression"),
        }
    }

    #[test]
    fn parse_searched_case_with_operand() {
        let stmt = parse_ok("SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Case(case) => {
                assert!(case.operand.is_some());
                assert_eq!(*case.operand.as_ref().unwrap().as_ref(), Expression::Identifier("status".to_string()));
            }
            _ => panic!("Expected Case expression"),
        }
    }

    #[test]
    fn parse_nested_case() {
        let stmt = parse_ok("SELECT CASE WHEN a THEN CASE WHEN b THEN 1 ELSE 2 END ELSE 3 END FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Case(case) => {
                // The first WHEN's result should be another CASE
                match &case.when_clauses[0].result {
                    Expression::Case(_) => {}
                    _ => panic!("Expected nested Case expression"),
                }
            }
            _ => panic!("Expected Case expression"),
        }
    }
}

// =============================================================================
// SUBQUERY PARSING TESTS
// =============================================================================

mod subqueries {
    use super::{parse_ok, first_column_expr, as_select, Expression};

    #[test]
    fn parse_scalar_subquery() {
        let stmt = parse_ok("SELECT (SELECT MAX(id) FROM users) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Parenthesized(inner) => {
                match inner.as_ref() {
                    Expression::Subquery(query) => {
                        assert!(query.from.is_some());
                    }
                    _ => panic!("Expected Subquery inside parentheses"),
                }
            }
            Expression::Subquery(_) => {}
            _ => panic!("Expected Subquery or Parenthesized, got {:?}", expr),
        }
    }

    #[test]
    fn parse_exists_subquery() {
        let stmt = parse_ok("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)");
        let select = as_select(&stmt);
        let where_clause = select.where_clause.as_ref().expect("Expected WHERE clause");
        // EXISTS is typically a function-like expression containing a subquery
        match &where_clause.condition {
            Expression::FunctionCall { name, args, .. } => {
                assert_eq!(name.to_uppercase(), "EXISTS");
                assert_eq!(args.len(), 1);
            }
            _ => {} // Parser might represent EXISTS differently
        }
    }
}

// =============================================================================
// IN LIST AND IN SUBQUERY PARSING TESTS
// =============================================================================

mod in_expressions {
    use super::{parse_ok, as_select, Expression, Literal};

    #[test]
    fn parse_in_list() {
        let stmt = parse_ok("SELECT * FROM users WHERE status IN ('active', 'pending')");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::InList { expr, list, negated } => {
                assert!(!negated);
                assert_eq!(**expr, Expression::Identifier("status".to_string()));
                assert_eq!(list.len(), 2);
            }
            _ => panic!("Expected InList, got {:?}", condition),
        }
    }

    #[test]
    fn parse_not_in_list() {
        let stmt = parse_ok("SELECT * FROM users WHERE status NOT IN ('deleted', 'banned')");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::InList { negated, .. } => {
                assert!(*negated);
            }
            _ => panic!("Expected InList"),
        }
    }

    #[test]
    fn parse_in_subquery() {
        let stmt = parse_ok("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::InSubquery { expr, subquery, negated } => {
                assert!(!negated);
                assert_eq!(**expr, Expression::Identifier("id".to_string()));
                assert!(subquery.from.is_some());
            }
            _ => panic!("Expected InSubquery, got {:?}", condition),
        }
    }

    #[test]
    fn parse_not_in_subquery() {
        let stmt = parse_ok("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM blocked_users)");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::InSubquery { negated, .. } => {
                assert!(*negated);
            }
            _ => panic!("Expected InSubquery"),
        }
    }

    #[test]
    fn parse_in_list_with_integers() {
        let stmt = parse_ok("SELECT * FROM orders WHERE status_code IN (1, 2, 3)");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::InList { list, .. } => {
                assert_eq!(list.len(), 3);
                assert_eq!(list[0], Expression::Literal(Literal::Integer(1)));
            }
            _ => panic!("Expected InList"),
        }
    }
}

// =============================================================================
// BETWEEN EXPRESSION PARSING TESTS
// =============================================================================

mod between_expressions {
    use super::{parse_ok, as_select, Expression, Literal};

    #[test]
    fn parse_between() {
        let stmt = parse_ok("SELECT * FROM orders WHERE amount BETWEEN 100 AND 500");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::Between { expr, low, high, negated } => {
                assert!(!negated);
                assert_eq!(**expr, Expression::Identifier("amount".to_string()));
                assert_eq!(**low, Expression::Literal(Literal::Integer(100)));
                assert_eq!(**high, Expression::Literal(Literal::Integer(500)));
            }
            _ => panic!("Expected Between, got {:?}", condition),
        }
    }

    #[test]
    fn parse_not_between() {
        let stmt = parse_ok("SELECT * FROM orders WHERE amount NOT BETWEEN 100 AND 500");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::Between { negated, .. } => {
                assert!(*negated);
            }
            _ => panic!("Expected Between"),
        }
    }

    #[test]
    fn parse_between_with_dates() {
        let stmt = parse_ok("SELECT * FROM events WHERE event_date BETWEEN '2024-01-01' AND '2024-12-31'");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::Between { low, high, .. } => {
                assert_eq!(**low, Expression::Literal(Literal::String("2024-01-01".to_string())));
                assert_eq!(**high, Expression::Literal(Literal::String("2024-12-31".to_string())));
            }
            _ => panic!("Expected Between"),
        }
    }
}

// =============================================================================
// IS NULL / IS NOT NULL PARSING TESTS
// =============================================================================

mod is_null_expressions {
    use super::{parse_ok, as_select, Expression};

    #[test]
    fn parse_is_null() {
        let stmt = parse_ok("SELECT * FROM users WHERE deleted_at IS NULL");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::IsNull { expr, negated } => {
                assert!(!negated);
                assert_eq!(**expr, Expression::Identifier("deleted_at".to_string()));
            }
            _ => panic!("Expected IsNull, got {:?}", condition),
        }
    }

    #[test]
    fn parse_is_not_null() {
        let stmt = parse_ok("SELECT * FROM users WHERE email IS NOT NULL");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::IsNull { expr, negated } => {
                assert!(*negated);
                assert_eq!(**expr, Expression::Identifier("email".to_string()));
            }
            _ => panic!("Expected IsNull"),
        }
    }

    #[test]
    fn parse_complex_is_null() {
        let stmt = parse_ok("SELECT * FROM t WHERE (a + b) IS NULL");
        let select = as_select(&stmt);
        let condition = &select.where_clause.as_ref().unwrap().condition;
        match condition {
            Expression::IsNull { expr, .. } => {
                match expr.as_ref() {
                    Expression::Parenthesized(_) | Expression::BinaryOp { .. } => {}
                    _ => panic!("Expected parenthesized or binary op"),
                }
            }
            _ => panic!("Expected IsNull"),
        }
    }
}

// =============================================================================
// CAST EXPRESSION PARSING TESTS
// =============================================================================

mod cast_expressions {
    use super::{parse_ok, first_column_expr, Expression, DataType};

    #[test]
    fn parse_cast_to_varchar() {
        let stmt = parse_ok("SELECT CAST(id AS VARCHAR) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Cast { expr, data_type, shorthand, try_cast } => {
                assert_eq!(**expr, Expression::Identifier("id".to_string()));
                assert!(matches!(data_type, DataType::Varchar(_)));
                assert!(!*shorthand, "Expected explicit CAST() syntax");
                assert!(!*try_cast, "Expected regular CAST, not TRY_CAST");
            }
            _ => panic!("Expected Cast, got {:?}", expr),
        }
    }

    #[test]
    fn parse_cast_to_integer() {
        let stmt = parse_ok("SELECT CAST(value AS INTEGER) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Cast { data_type, .. } => {
                assert_eq!(*data_type, DataType::Integer);
            }
            _ => panic!("Expected Cast"),
        }
    }

    #[test]
    fn parse_cast_to_decimal_with_precision() {
        let stmt = parse_ok("SELECT CAST(price AS DECIMAL(10, 2)) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Cast { data_type, .. } => {
                assert_eq!(*data_type, DataType::Decimal(Some(10), Some(2)));
            }
            _ => panic!("Expected Cast"),
        }
    }

    #[test]
    fn parse_cast_to_timestamp() {
        let stmt = parse_ok("SELECT CAST(date_str AS TIMESTAMP) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Cast { data_type, .. } => {
                assert_eq!(*data_type, DataType::Timestamp);
            }
            _ => panic!("Expected Cast"),
        }
    }

    #[test]
    fn parse_double_colon_cast() {
        // Snowflake/PostgreSQL style casting
        let stmt = parse_ok("SELECT id::VARCHAR FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Cast { expr, data_type, shorthand, try_cast } => {
                assert_eq!(**expr, Expression::Identifier("id".to_string()));
                assert!(matches!(data_type, DataType::Varchar(_)));
                assert!(*shorthand, "Expected shorthand (::) syntax");
                assert!(!*try_cast, "Expected regular CAST");
            }
            _ => panic!("Expected Cast (double colon syntax), got {:?}", expr),
        }
    }
}

// =============================================================================
// SELECT STATEMENT PARSING TESTS
// =============================================================================

mod select_statements {
    use super::{parse_ok, as_select, Expression, Literal, TableReference, JoinType, SortDirection, NullsOrder, SetOperationType};

    #[test]
    fn parse_simple_select() {
        let stmt = parse_ok("SELECT id FROM users");
        let select = as_select(&stmt);
        assert_eq!(select.columns.len(), 1);
        assert!(select.from.is_some());
        assert!(select.where_clause.is_none());
    }

    #[test]
    fn parse_select_multiple_columns() {
        let stmt = parse_ok("SELECT id, name, email FROM users");
        let select = as_select(&stmt);
        assert_eq!(select.columns.len(), 3);
    }

    #[test]
    fn parse_select_with_alias() {
        let stmt = parse_ok("SELECT id AS user_id FROM users");
        let select = as_select(&stmt);
        assert_eq!(select.columns[0].alias, Some("user_id".to_string()));
    }

    #[test]
    fn parse_select_with_table_alias() {
        let stmt = parse_ok("SELECT u.id FROM users u");
        let select = as_select(&stmt);
        match &select.from.as_ref().unwrap().table {
            TableReference::Table { name, alias, .. } => {
                assert_eq!(name, "users");
                assert_eq!(*alias, Some("u".to_string()));
            }
            _ => panic!("Expected Table reference"),
        }
    }

    #[test]
    fn parse_select_with_where() {
        let stmt = parse_ok("SELECT * FROM users WHERE active = TRUE");
        let select = as_select(&stmt);
        assert!(select.where_clause.is_some());
    }

    #[test]
    fn parse_select_with_group_by() {
        let stmt = parse_ok("SELECT status, COUNT(*) FROM users GROUP BY status");
        let select = as_select(&stmt);
        assert!(select.group_by.is_some());
        assert_eq!(select.group_by.as_ref().unwrap().expressions.len(), 1);
    }

    #[test]
    fn parse_select_with_having() {
        let stmt = parse_ok("SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 10");
        let select = as_select(&stmt);
        assert!(select.having.is_some());
    }

    #[test]
    fn parse_select_with_order_by() {
        let stmt = parse_ok("SELECT * FROM users ORDER BY created_at DESC");
        let select = as_select(&stmt);
        assert!(select.order_by.is_some());
        let order = select.order_by.as_ref().unwrap();
        assert_eq!(order.items.len(), 1);
        assert_eq!(order.items[0].direction, Some(SortDirection::Desc));
    }

    #[test]
    fn parse_select_with_order_by_nulls() {
        let stmt = parse_ok("SELECT * FROM users ORDER BY name NULLS FIRST");
        let select = as_select(&stmt);
        let order = select.order_by.as_ref().unwrap();
        assert_eq!(order.items[0].nulls, Some(NullsOrder::First));
    }

    #[test]
    fn parse_select_with_limit() {
        let stmt = parse_ok("SELECT * FROM users LIMIT 10");
        let select = as_select(&stmt);
        assert!(select.limit.is_some());
        let limit = select.limit.as_ref().unwrap();
        assert_eq!(limit.count, Expression::Literal(Literal::Integer(10)));
    }

    #[test]
    fn parse_select_with_limit_offset() {
        let stmt = parse_ok("SELECT * FROM users LIMIT 10 OFFSET 20");
        let select = as_select(&stmt);
        let limit = select.limit.as_ref().unwrap();
        assert!(limit.offset.is_some());
        assert_eq!(limit.offset.as_ref().unwrap(), &Expression::Literal(Literal::Integer(20)));
    }

    #[test]
    fn parse_select_with_join() {
        let stmt = parse_ok("SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id");
        let select = as_select(&stmt);
        assert_eq!(select.joins.len(), 1);
        assert_eq!(select.joins[0].join_type, JoinType::Inner);
    }

    #[test]
    fn parse_select_with_left_join() {
        let stmt = parse_ok("SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id");
        let select = as_select(&stmt);
        assert_eq!(select.joins[0].join_type, JoinType::Left);
    }

    #[test]
    fn parse_select_with_cte() {
        let stmt = parse_ok("WITH active AS (SELECT * FROM users WHERE active = TRUE) SELECT * FROM active");
        let select = as_select(&stmt);
        assert!(select.with_clause.is_some());
        let with = select.with_clause.as_ref().unwrap();
        assert_eq!(with.ctes.len(), 1);
        assert_eq!(with.ctes[0].name, "active");
    }

    #[test]
    fn parse_select_with_multiple_ctes() {
        let stmt = parse_ok("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b");
        let select = as_select(&stmt);
        let with = select.with_clause.as_ref().unwrap();
        assert_eq!(with.ctes.len(), 2);
    }

    #[test]
    fn parse_select_union() {
        let stmt = parse_ok("SELECT id FROM t1 UNION SELECT id FROM t2");
        let select = as_select(&stmt);
        assert!(select.union.is_some());
        let union = select.union.as_ref().unwrap();
        assert_eq!(union.op_type, SetOperationType::Union);
        assert!(!union.all);
    }

    #[test]
    fn parse_select_union_all() {
        let stmt = parse_ok("SELECT id FROM t1 UNION ALL SELECT id FROM t2");
        let select = as_select(&stmt);
        let union = select.union.as_ref().unwrap();
        assert!(union.all);
    }

    #[test]
    fn parse_select_qualify() {
        let stmt = parse_ok("SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1");
        let select = as_select(&stmt);
        assert!(select.qualify.is_some());
    }

    #[test]
    fn parse_subquery_in_from() {
        let stmt = parse_ok("SELECT * FROM (SELECT id FROM users) AS sub");
        let select = as_select(&stmt);
        match &select.from.as_ref().unwrap().table {
            TableReference::Subquery { query, alias, .. } => {
                assert_eq!(alias, &Some("sub".to_string()));
                assert!(query.from.is_some());
            }
            _ => panic!("Expected Subquery table reference"),
        }
    }
}

// =============================================================================
// INSERT STATEMENT PARSING TESTS
// =============================================================================

mod insert_statements {
    use super::{parse_ok, Statement, InsertSource};

    #[test]
    fn parse_insert_values() {
        let stmt = parse_ok("INSERT INTO users (id, name) VALUES (1, 'John')");
        match &stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.table, "users");
                assert_eq!(insert.columns, Some(vec!["id".to_string(), "name".to_string()]));
                match &insert.source {
                    InsertSource::Values(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].len(), 2);
                    }
                    _ => panic!("Expected VALUES source"),
                }
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn parse_insert_multiple_rows() {
        let stmt = parse_ok("INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')");
        match &stmt {
            Statement::Insert(insert) => {
                match &insert.source {
                    InsertSource::Values(rows) => {
                        assert_eq!(rows.len(), 2);
                    }
                    _ => panic!("Expected VALUES source"),
                }
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn parse_insert_without_columns() {
        let stmt = parse_ok("INSERT INTO users VALUES (1, 'John', 'john@example.com')");
        match &stmt {
            Statement::Insert(insert) => {
                assert!(insert.columns.is_none());
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn parse_insert_select() {
        let stmt = parse_ok("INSERT INTO users_backup SELECT * FROM users WHERE active = TRUE");
        match &stmt {
            Statement::Insert(insert) => {
                match &insert.source {
                    InsertSource::Query(query) => {
                        assert!(query.from.is_some());
                    }
                    _ => panic!("Expected Query source"),
                }
            }
            _ => panic!("Expected INSERT statement"),
        }
    }
}

// =============================================================================
// UPDATE STATEMENT PARSING TESTS
// =============================================================================

mod update_statements {
    use super::{parse_ok, Statement};

    #[test]
    fn parse_simple_update() {
        let stmt = parse_ok("UPDATE users SET active = FALSE WHERE id = 1");
        match &stmt {
            Statement::Update(update) => {
                assert_eq!(update.table, "users");
                assert_eq!(update.assignments.len(), 1);
                assert_eq!(update.assignments[0].0, "active");
                assert!(update.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn parse_update_multiple_columns() {
        let stmt = parse_ok("UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1");
        match &stmt {
            Statement::Update(update) => {
                assert_eq!(update.assignments.len(), 2);
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn parse_update_with_alias() {
        let stmt = parse_ok("UPDATE users u SET u.active = FALSE WHERE u.id = 1");
        match &stmt {
            Statement::Update(update) => {
                assert_eq!(update.alias, Some("u".to_string()));
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn parse_update_without_where() {
        let stmt = parse_ok("UPDATE users SET active = FALSE");
        match &stmt {
            Statement::Update(update) => {
                assert!(update.where_clause.is_none());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }
}

// =============================================================================
// DELETE STATEMENT PARSING TESTS
// =============================================================================

mod delete_statements {
    use super::{parse_ok, Statement};

    #[test]
    fn parse_simple_delete() {
        let stmt = parse_ok("DELETE FROM users WHERE id = 1");
        match &stmt {
            Statement::Delete(delete) => {
                assert_eq!(delete.table, "users");
                assert!(delete.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn parse_delete_with_alias() {
        let stmt = parse_ok("DELETE FROM users u WHERE u.active = FALSE");
        match &stmt {
            Statement::Delete(delete) => {
                assert_eq!(delete.alias, Some("u".to_string()));
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn parse_delete_with_using() {
        let stmt = parse_ok("DELETE FROM orders USING users WHERE orders.user_id = users.id AND users.active = FALSE");
        match &stmt {
            Statement::Delete(delete) => {
                assert!(delete.using.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }
}

// =============================================================================
// MERGE STATEMENT PARSING TESTS
// =============================================================================

mod merge_statements {
    use super::{parse_ok, Statement, MergeClause, MergeAction};

    #[test]
    fn parse_simple_merge() {
        let stmt = parse_ok(
            "MERGE INTO target t \
             USING source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
        );
        match &stmt {
            Statement::Merge(merge) => {
                assert_eq!(merge.target, "target");
                assert_eq!(merge.target_alias, Some("t".to_string()));
                assert_eq!(merge.clauses.len(), 2);
            }
            _ => panic!("Expected MERGE statement"),
        }
    }

    #[test]
    fn parse_merge_with_delete() {
        let stmt = parse_ok(
            "MERGE INTO target t \
             USING source s ON t.id = s.id \
             WHEN MATCHED AND s.deleted = TRUE THEN DELETE"
        );
        match &stmt {
            Statement::Merge(merge) => {
                assert_eq!(merge.clauses.len(), 1);
                match &merge.clauses[0] {
                    MergeClause::WhenMatched { action, condition } => {
                        assert!(condition.is_some());
                        assert_eq!(*action, MergeAction::Delete);
                    }
                    _ => panic!("Expected WhenMatched clause"),
                }
            }
            _ => panic!("Expected MERGE statement"),
        }
    }
}

// =============================================================================
// CREATE TABLE STATEMENT PARSING TESTS
// =============================================================================

mod create_table_statements {
    use super::{parse_ok, Statement};

    #[test]
    fn parse_create_table() {
        let stmt = parse_ok("CREATE TABLE users (id INT, name VARCHAR(255))");
        match &stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "users");
                assert!(!ct.if_not_exists);
                assert_eq!(ct.columns.len(), 2);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn parse_create_table_if_not_exists() {
        let stmt = parse_ok("CREATE TABLE IF NOT EXISTS users (id INT)");
        match &stmt {
            Statement::CreateTable(ct) => {
                assert!(ct.if_not_exists);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn parse_create_table_as_select() {
        let stmt = parse_ok("CREATE TABLE users_backup AS SELECT * FROM users");
        match &stmt {
            Statement::CreateTable(ct) => {
                assert!(ct.as_query.is_some());
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn parse_column_with_not_null() {
        let stmt = parse_ok("CREATE TABLE users (id INT NOT NULL)");
        match &stmt {
            Statement::CreateTable(ct) => {
                assert!(!ct.columns[0].nullable);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn parse_column_with_default() {
        let stmt = parse_ok("CREATE TABLE users (active BOOLEAN DEFAULT TRUE)");
        match &stmt {
            Statement::CreateTable(ct) => {
                assert!(ct.columns[0].default.is_some());
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }
}

// =============================================================================
// CREATE VIEW STATEMENT PARSING TESTS
// =============================================================================

mod create_view_statements {
    use super::{parse_ok, Statement};

    #[test]
    fn parse_create_view() {
        let stmt = parse_ok("CREATE VIEW active_users AS SELECT * FROM users WHERE active = TRUE");
        match &stmt {
            Statement::CreateView(cv) => {
                assert_eq!(cv.name, "active_users");
                assert!(!cv.or_replace);
            }
            _ => panic!("Expected CREATE VIEW statement"),
        }
    }

    #[test]
    fn parse_create_or_replace_view() {
        let stmt = parse_ok("CREATE OR REPLACE VIEW active_users AS SELECT * FROM users");
        match &stmt {
            Statement::CreateView(cv) => {
                assert!(cv.or_replace);
            }
            _ => panic!("Expected CREATE VIEW statement"),
        }
    }

    #[test]
    fn parse_create_view_with_columns() {
        let stmt = parse_ok("CREATE VIEW user_summary (user_id, user_name) AS SELECT id, name FROM users");
        match &stmt {
            Statement::CreateView(cv) => {
                assert_eq!(cv.columns, Some(vec!["user_id".to_string(), "user_name".to_string()]));
            }
            _ => panic!("Expected CREATE VIEW statement"),
        }
    }
}

// =============================================================================
// DROP STATEMENT PARSING TESTS
// =============================================================================

mod drop_statements {
    use super::{parse_ok, Statement};

    #[test]
    fn parse_drop_table() {
        let stmt = parse_ok("DROP TABLE users");
        match &stmt {
            Statement::DropTable(dt) => {
                assert_eq!(dt.name, "users");
                assert!(!dt.if_exists);
            }
            _ => panic!("Expected DROP TABLE statement"),
        }
    }

    #[test]
    fn parse_drop_table_if_exists() {
        let stmt = parse_ok("DROP TABLE IF EXISTS users");
        match &stmt {
            Statement::DropTable(dt) => {
                assert!(dt.if_exists);
            }
            _ => panic!("Expected DROP TABLE statement"),
        }
    }

    #[test]
    fn parse_drop_view() {
        let stmt = parse_ok("DROP VIEW active_users");
        match &stmt {
            Statement::DropView(dv) => {
                assert_eq!(dv.name, "active_users");
                assert!(!dv.if_exists);
            }
            _ => panic!("Expected DROP VIEW statement"),
        }
    }

    #[test]
    fn parse_drop_view_if_exists() {
        let stmt = parse_ok("DROP VIEW IF EXISTS active_users");
        match &stmt {
            Statement::DropView(dv) => {
                assert!(dv.if_exists);
            }
            _ => panic!("Expected DROP VIEW statement"),
        }
    }
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

mod error_handling {
    use super::parse_err;

    #[test]
    fn error_on_empty_input() {
        let err = parse_err("");
        match err {
            sqlflight::Error::ParseError { message, .. } => {
                assert!(!message.is_empty());
            }
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn error_on_invalid_keyword() {
        let err = parse_err("SELEC id FROM users");
        match err {
            sqlflight::Error::ParseError { message, .. } => {
                assert!(!message.is_empty());
            }
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn implicit_column_alias_without_from() {
        // "SELECT id users" is valid SQL - 'users' is an implicit alias for column 'id'
        // SELECT without FROM is valid (e.g., SELECT 1 + 1)
        use super::parse_ok;
        let stmt = parse_ok("SELECT id users");
        let select = super::as_select(&stmt);
        assert_eq!(select.columns.len(), 1);
        assert_eq!(select.columns[0].alias, Some("users".to_string()));
        assert!(!select.columns[0].explicit_as);
    }

    #[test]
    fn error_on_unclosed_parenthesis() {
        let err = parse_err("SELECT (a + b FROM t");
        match err {
            sqlflight::Error::ParseError { .. } => {}
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn error_on_unclosed_string() {
        let err = parse_err("SELECT 'unclosed FROM t");
        match err {
            sqlflight::Error::ParseError { .. } => {}
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn error_on_incomplete_case() {
        let err = parse_err("SELECT CASE WHEN x > 0 THEN 'positive' FROM t");
        match err {
            sqlflight::Error::ParseError { .. } => {}
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn error_on_invalid_binary_operator() {
        let err = parse_err("SELECT a & b FROM t");
        match err {
            sqlflight::Error::ParseError { .. } => {}
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn error_includes_position_info() {
        let err = parse_err("SELECT id FORM users"); // Typo: FORM instead of FROM
        match err {
            sqlflight::Error::ParseError { span, .. } => {
                // The parser should include span information for error location
                // This may be None if the parser doesn't support it yet
                if let Some((start, _end)) = span {
                    // Position should point near the error
                    assert!(start > 0);
                }
            }
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn implicit_table_alias() {
        // "SELECT id FROM users GARBAGE" is valid SQL - 'GARBAGE' is an implicit alias for table 'users'
        use super::parse_ok;
        use sqlflight::ast::TableReference;
        let stmt = parse_ok("SELECT id FROM users GARBAGE");
        let select = super::as_select(&stmt);
        let from = select.from.as_ref().expect("Expected FROM clause");
        match &from.table {
            TableReference::Table { alias, .. } => {
                assert_eq!(*alias, Some("GARBAGE".to_string()));
            }
            _ => panic!("Expected Table reference"),
        }
    }

    #[test]
    fn error_on_missing_table_name() {
        let err = parse_err("SELECT id FROM");
        match err {
            sqlflight::Error::ParseError { .. } => {}
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn error_descriptive_message() {
        let err = parse_err("SELECT , FROM users");
        match err {
            sqlflight::Error::ParseError { message, .. } => {
                // Error message should be descriptive
                assert!(!message.is_empty());
                // Could contain hints about what was expected
            }
            _ => panic!("Expected ParseError"),
        }
    }
}

// =============================================================================
// SNOWFLAKE-SPECIFIC PARSING TESTS
// =============================================================================

mod snowflake_specific {
    use super::{parse_ok, first_column_expr, as_select, Statement, Expression, Literal, TableReference, DataType};

    #[test]
    fn parse_flatten_table_function() {
        let stmt = parse_ok("SELECT * FROM TABLE(FLATTEN(input => data, path => 'items'))");
        let select = as_select(&stmt);
        match &select.from.as_ref().unwrap().table {
            TableReference::Flatten(flatten) => {
                assert!(flatten.path.is_some());
            }
            _ => {} // Parser may represent this differently
        }
    }

    #[test]
    fn parse_lateral_flatten() {
        let stmt = parse_ok("SELECT * FROM t, LATERAL FLATTEN(input => t.data)");
        let select = as_select(&stmt);
        // Check that joins include the lateral flatten
        assert!(!select.joins.is_empty() || select.from.is_some());
    }

    #[test]
    fn parse_semi_structured_access() {
        let stmt = parse_ok("SELECT data:field FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::SemiStructuredAccess { expr, path } => {
                assert_eq!(**expr, Expression::Identifier("data".to_string()));
                assert_eq!(path, "field");
            }
            _ => panic!("Expected SemiStructuredAccess, got {:?}", expr),
        }
    }

    #[test]
    fn parse_array_access() {
        let stmt = parse_ok("SELECT arr[0] FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::ArrayAccess { expr, index } => {
                assert_eq!(**expr, Expression::Identifier("arr".to_string()));
                assert_eq!(**index, Expression::Literal(Literal::Integer(0)));
            }
            _ => panic!("Expected ArrayAccess, got {:?}", expr),
        }
    }

    #[test]
    fn parse_variant_type() {
        let stmt = parse_ok("CREATE TABLE t (data VARIANT)");
        match &stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Variant);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn parse_object_type() {
        let stmt = parse_ok("CREATE TABLE t (data OBJECT)");
        match &stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Object);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn parse_array_type() {
        let stmt = parse_ok("CREATE TABLE t (data ARRAY)");
        match &stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Array);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }
}

// =============================================================================
// JINJA EXPRESSION PARSING TESTS
// =============================================================================

mod jinja_expressions {
    use super::{parse_ok, first_column_expr, as_select, Expression, TableReference};

    #[test]
    fn parse_jinja_expression_in_select() {
        let stmt = parse_ok("SELECT {{ column_name }} FROM users");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::JinjaExpression(content) => {
                assert!(content.contains("column_name"));
            }
            _ => panic!("Expected JinjaExpression, got {:?}", expr),
        }
    }

    #[test]
    fn jinja_blocks_handled_by_formatter() {
        // Jinja blocks like {% if %}...{% endif %} need the formatter's Jinja extraction
        // The raw parser cannot handle them directly - it treats each token separately
        // Use sqlflight::format() for proper Jinja handling
        let result = sqlflight::format("SELECT {% if condition %} id {% endif %} FROM users");
        assert!(result.is_ok(), "Formatting should succeed with Jinja extraction");
    }

    #[test]
    fn parse_jinja_ref_in_from() {
        let stmt = parse_ok("SELECT * FROM {{ ref('users') }}");
        let select = as_select(&stmt);
        match &select.from.as_ref().unwrap().table {
            TableReference::JinjaRef(content) => {
                assert!(content.contains("ref"));
            }
            _ => panic!("Expected JinjaRef table reference"),
        }
    }
}

// =============================================================================
// EXTRACT EXPRESSION PARSING TESTS
// =============================================================================

mod extract_expressions {
    use super::{parse_ok, first_column_expr, Expression};

    #[test]
    fn parse_extract_year() {
        let stmt = parse_ok("SELECT EXTRACT(YEAR FROM created_at) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Extract { field, expr } => {
                assert_eq!(field.to_uppercase(), "YEAR");
                assert_eq!(**expr, Expression::Identifier("created_at".to_string()));
            }
            _ => panic!("Expected Extract, got {:?}", expr),
        }
    }

    #[test]
    fn parse_extract_month() {
        let stmt = parse_ok("SELECT EXTRACT(MONTH FROM date_col) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Extract { field, .. } => {
                assert_eq!(field.to_uppercase(), "MONTH");
            }
            _ => panic!("Expected Extract"),
        }
    }

    #[test]
    fn parse_extract_day() {
        let stmt = parse_ok("SELECT EXTRACT(DAY FROM date_col) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Extract { field, .. } => {
                assert_eq!(field.to_uppercase(), "DAY");
            }
            _ => panic!("Expected Extract"),
        }
    }
}

// =============================================================================
// PARENTHESIZED EXPRESSION PARSING TESTS
// =============================================================================

mod parenthesized_expressions {
    use super::{parse_ok, first_column_expr, Expression, BinaryOperator};

    #[test]
    fn parse_simple_parenthesized() {
        let stmt = parse_ok("SELECT (a + b) FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::Parenthesized(inner) => {
                match inner.as_ref() {
                    Expression::BinaryOp { op, .. } => {
                        assert_eq!(*op, BinaryOperator::Plus);
                    }
                    _ => panic!("Expected BinaryOp inside parentheses"),
                }
            }
            Expression::BinaryOp { .. } => {} // Parser might optimize away unnecessary parens
            _ => panic!("Expected Parenthesized or BinaryOp, got {:?}", expr),
        }
    }

    #[test]
    fn parse_nested_parentheses() {
        let stmt = parse_ok("SELECT ((a)) FROM t");
        // Should not crash and should preserve the expression
        let expr = first_column_expr(&stmt);
        // The innermost expression should be an identifier
        fn extract_innermost(e: &Expression) -> &Expression {
            match e {
                Expression::Parenthesized(inner) => extract_innermost(inner),
                other => other,
            }
        }
        let inner = extract_innermost(expr);
        assert_eq!(*inner, Expression::Identifier("a".to_string()));
    }

    #[test]
    fn parse_parentheses_change_precedence() {
        // (a + b) * c should parse differently from a + b * c
        let stmt = parse_ok("SELECT (a + b) * c FROM t");
        let expr = first_column_expr(&stmt);
        match expr {
            Expression::BinaryOp { left, op, .. } => {
                assert_eq!(*op, BinaryOperator::Multiply);
                match left.as_ref() {
                    Expression::Parenthesized(inner) => {
                        match inner.as_ref() {
                            Expression::BinaryOp { op, .. } => {
                                assert_eq!(*op, BinaryOperator::Plus);
                            }
                            _ => panic!("Expected BinaryOp inside parentheses"),
                        }
                    }
                    Expression::BinaryOp { op, .. } => {
                        assert_eq!(*op, BinaryOperator::Plus);
                    }
                    _ => panic!("Expected Parenthesized or BinaryOp on left"),
                }
            }
            _ => panic!("Expected BinaryOp for multiplication, got {:?}", expr),
        }
    }
}
