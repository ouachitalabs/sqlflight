//! Expression parsing
//!
//! Parses SQL expressions with proper operator precedence using Pratt parsing.

use crate::ast::*;
use crate::parser::lexer::{Span, SpannedToken, Token};
use crate::Result;

/// Parser state for token-based parsing
pub struct Parser<'a> {
    tokens: &'a [Token],
    spanned_tokens: &'a [SpannedToken],
    source: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: &'a [Token]) -> Self {
        Parser {
            tokens,
            spanned_tokens: &[],
            source: "",
            pos: 0,
        }
    }

    pub fn new_with_source(
        tokens: &'a [Token],
        spanned_tokens: &'a [SpannedToken],
        source: &'a str,
    ) -> Self {
        Parser {
            tokens,
            spanned_tokens,
            source,
            pos: 0,
        }
    }

    /// Current token
    pub fn current(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    /// Get the span of the current token
    pub fn current_span(&self) -> Span {
        self.spanned_tokens
            .get(self.pos)
            .map(|t| t.span)
            .unwrap_or_default()
    }

    /// Peek at the next token
    pub fn peek(&self) -> &Token {
        self.tokens.get(self.pos + 1).unwrap_or(&Token::Eof)
    }

    /// Peek at token at offset
    pub fn peek_at(&self, offset: usize) -> &Token {
        self.tokens.get(self.pos + offset).unwrap_or(&Token::Eof)
    }

    /// Advance to the next token
    pub fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    /// Check if current token matches
    pub fn check(&self, token: &Token) -> bool {
        std::mem::discriminant(self.current()) == std::mem::discriminant(token)
    }

    /// Consume a specific token or error
    pub fn expect(&mut self, token: &Token) -> Result<()> {
        if self.check(token) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("Expected {:?}, found {:?}", token, self.current())))
        }
    }

    /// Try to consume a token if it matches
    pub fn consume(&mut self, token: &Token) -> bool {
        if self.check(token) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Get current position
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Restore position
    pub fn restore(&mut self, pos: usize) {
        self.pos = pos;
    }

    /// Check if at end of tokens
    pub fn is_eof(&self) -> bool {
        matches!(self.current(), Token::Eof)
    }

    /// Create an error with source context
    pub fn error(&self, message: &str) -> crate::Error {
        let span = self.current_span();
        if !self.source.is_empty() && span.start < self.source.len() {
            let formatted = crate::error::format_parse_error(self.source, span.start, message);
            crate::Error::ParseError {
                message: formatted,
                span: Some((span.start, span.end)),
            }
        } else {
            crate::Error::ParseError {
                message: format!("Parse error: {}", message),
                span: None,
            }
        }
    }
}

/// Convert a token to an identifier name if possible
/// This handles both regular identifiers and keywords that can be used as identifiers in certain contexts
fn token_to_identifier_name(token: &Token) -> Option<String> {
    match token {
        Token::Identifier(name) => Some(name.clone()),
        Token::QuotedIdentifier(name) => Some(name.clone()),
        // Keywords that can be used as identifiers after a dot
        Token::Table => Some("table".to_string()),
        Token::View => Some("view".to_string()),
        Token::Date => Some("date".to_string()),
        Token::Timestamp => Some("timestamp".to_string()),
        Token::Values => Some("values".to_string()),
        Token::First => Some("first".to_string()),
        Token::Last => Some("last".to_string()),
        Token::Left => Some("left".to_string()),
        Token::Right => Some("right".to_string()),
        Token::Full => Some("full".to_string()),
        Token::Inner => Some("inner".to_string()),
        Token::Outer => Some("outer".to_string()),
        Token::Cross => Some("cross".to_string()),
        Token::Row => Some("row".to_string()),
        Token::Rows => Some("rows".to_string()),
        Token::Range => Some("range".to_string()),
        Token::Groups => Some("groups".to_string()),
        Token::Current => Some("current".to_string()),
        Token::Window => Some("window".to_string()),
        Token::Partition => Some("partition".to_string()),
        Token::Over => Some("over".to_string()),
        Token::Order => Some("order".to_string()),
        Token::By => Some("by".to_string()),
        Token::Group => Some("group".to_string()),
        Token::Having => Some("having".to_string()),
        Token::Limit => Some("limit".to_string()),
        Token::Offset => Some("offset".to_string()),
        Token::Union => Some("union".to_string()),
        Token::Except => Some("except".to_string()),
        Token::Intersect => Some("intersect".to_string()),
        Token::All => Some("all".to_string()),
        Token::Distinct => Some("distinct".to_string()),
        Token::As => Some("as".to_string()),
        Token::On => Some("on".to_string()),
        Token::Using => Some("using".to_string()),
        Token::Join => Some("join".to_string()),
        Token::Lateral => Some("lateral".to_string()),
        Token::Flatten => Some("flatten".to_string()),
        Token::Sample => Some("sample".to_string()),
        Token::Pivot => Some("pivot".to_string()),
        Token::Unpivot => Some("unpivot".to_string()),
        Token::Qualify => Some("qualify".to_string()),
        Token::Create => Some("create".to_string()),
        Token::Alter => Some("alter".to_string()),
        Token::Drop => Some("drop".to_string()),
        Token::Insert => Some("insert".to_string()),
        Token::Update => Some("update".to_string()),
        Token::Delete => Some("delete".to_string()),
        Token::Merge => Some("merge".to_string()),
        Token::Set => Some("set".to_string()),
        Token::Default => Some("default".to_string()),
        Token::True => Some("true".to_string()),
        Token::False => Some("false".to_string()),
        Token::Null => Some("null".to_string()),
        Token::And => Some("and".to_string()),
        Token::Or => Some("or".to_string()),
        Token::Not => Some("not".to_string()),
        Token::In => Some("in".to_string()),
        Token::Is => Some("is".to_string()),
        Token::Like => Some("like".to_string()),
        Token::ILike => Some("ilike".to_string()),
        Token::Between => Some("between".to_string()),
        Token::Case => Some("case".to_string()),
        Token::When => Some("when".to_string()),
        Token::Then => Some("then".to_string()),
        Token::Else => Some("else".to_string()),
        Token::End => Some("end".to_string()),
        Token::Cast => Some("cast".to_string()),
        Token::Extract => Some("extract".to_string()),
        Token::Interval => Some("interval".to_string()),
        _ => None,
    }
}

/// Parse an expression with precedence climbing
pub fn parse_expression(parser: &mut Parser) -> Result<Expression> {
    parse_or_expression(parser)
}

/// Parse OR expression (lowest precedence)
fn parse_or_expression(parser: &mut Parser) -> Result<Expression> {
    let mut left = parse_and_expression(parser)?;

    while parser.check(&Token::Or) {
        parser.advance();
        let right = parse_and_expression(parser)?;
        left = Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Or,
            right: Box::new(right),
        };
    }

    Ok(left)
}

/// Parse AND expression
fn parse_and_expression(parser: &mut Parser) -> Result<Expression> {
    let mut left = parse_not_expression(parser)?;

    while parser.check(&Token::And) {
        parser.advance();
        let right = parse_not_expression(parser)?;
        left = Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        };
    }

    Ok(left)
}

/// Parse NOT expression
fn parse_not_expression(parser: &mut Parser) -> Result<Expression> {
    if parser.check(&Token::Not) {
        parser.advance();
        let expr = parse_not_expression(parser)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        });
    }

    parse_comparison_expression(parser)
}

/// Parse comparison expression
fn parse_comparison_expression(parser: &mut Parser) -> Result<Expression> {
    let left = parse_between_in_expression(parser)?;

    // Handle IS NULL / IS NOT NULL / IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
    // / IS DISTINCT FROM / IS NOT DISTINCT FROM
    if parser.check(&Token::Is) {
        parser.advance();
        let negated = parser.consume(&Token::Not);

        if parser.consume(&Token::Null) {
            return Ok(Expression::IsNull {
                expr: Box::new(left),
                negated,
            });
        } else if parser.consume(&Token::True) {
            return Ok(Expression::IsTrue {
                expr: Box::new(left),
                negated,
            });
        } else if parser.consume(&Token::False) {
            return Ok(Expression::IsFalse {
                expr: Box::new(left),
                negated,
            });
        } else if parser.consume(&Token::Distinct) {
            parser.expect(&Token::From)?;
            let other = parse_between_in_expression(parser)?;
            return Ok(Expression::IsDistinctFrom {
                expr: Box::new(left),
                other: Box::new(other),
                negated,
            });
        } else {
            return Err(parser.error("Expected NULL, TRUE, FALSE, or DISTINCT after IS"));
        }
    }

    // Handle comparison operators
    let op = match parser.current() {
        Token::Eq => Some(BinaryOperator::Eq),
        Token::NotEq => Some(BinaryOperator::NotEq),
        Token::Lt => Some(BinaryOperator::Lt),
        Token::LtEq => Some(BinaryOperator::LtEq),
        Token::Gt => Some(BinaryOperator::Gt),
        Token::GtEq => Some(BinaryOperator::GtEq),
        Token::Like => Some(BinaryOperator::Like),
        Token::ILike => Some(BinaryOperator::ILike),
        _ => None,
    };

    if let Some(op) = op {
        parser.advance();

        // Handle LIKE/ILIKE ANY/ALL (pattern_list) - Snowflake pattern matching
        if matches!(op, BinaryOperator::Like | BinaryOperator::ILike) {
            // Check for ANY/ALL - either as Token::All keyword or as identifier "ANY"
            let quantifier = if parser.check(&Token::All) {
                parser.advance();
                Some("all".to_string())
            } else if let Token::Identifier(name) = parser.current() {
                if name.to_uppercase() == "ANY" {
                    let q = name.clone();
                    parser.advance();
                    Some(q)
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(quant) = quantifier {
                // Parse the pattern list: (pattern1, pattern2, ...)
                parser.expect(&Token::LParen)?;
                let mut patterns = vec![parse_expression(parser)?];
                while parser.consume(&Token::Comma) {
                    patterns.push(parse_expression(parser)?);
                }
                parser.expect(&Token::RParen)?;

                return Ok(Expression::LikeAny {
                    expr: Box::new(left),
                    patterns,
                    case_insensitive: matches!(op, BinaryOperator::ILike),
                    quantifier: quant.to_lowercase(),
                });
            }
        }

        let right = parse_between_in_expression(parser)?;
        return Ok(Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        });
    }

    Ok(left)
}

/// Parse BETWEEN and IN expressions
fn parse_between_in_expression(parser: &mut Parser) -> Result<Expression> {
    let expr = parse_additive_expression(parser)?;

    // Handle NOT BETWEEN / BETWEEN
    if parser.check(&Token::Not) && matches!(parser.peek(), Token::Between) {
        parser.advance(); // consume NOT
        parser.advance(); // consume BETWEEN
        let low = parse_additive_expression(parser)?;
        parser.expect(&Token::And)?;
        let high = parse_additive_expression(parser)?;
        return Ok(Expression::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
            negated: true,
        });
    }

    if parser.check(&Token::Between) {
        parser.advance();
        let low = parse_additive_expression(parser)?;
        parser.expect(&Token::And)?;
        let high = parse_additive_expression(parser)?;
        return Ok(Expression::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
            negated: false,
        });
    }

    // Handle NOT IN / IN
    if parser.check(&Token::Not) && matches!(parser.peek(), Token::In) {
        parser.advance(); // consume NOT
        parser.advance(); // consume IN
        return parse_in_list(parser, expr, true);
    }

    if parser.check(&Token::In) {
        parser.advance();
        return parse_in_list(parser, expr, false);
    }

    Ok(expr)
}

/// Parse IN list or subquery
fn parse_in_list(parser: &mut Parser, expr: Expression, negated: bool) -> Result<Expression> {
    parser.expect(&Token::LParen)?;

    // Check if it's a subquery by looking for SELECT
    if parser.check(&Token::Select) || parser.check(&Token::With) {
        let subquery = super::stmt::parse_select_statement(parser)?;
        parser.expect(&Token::RParen)?;
        return Ok(Expression::InSubquery {
            expr: Box::new(expr),
            subquery: Box::new(subquery),
            negated,
        });
    }

    // Parse a list of expressions
    let mut list = vec![parse_expression(parser)?];
    while parser.consume(&Token::Comma) {
        list.push(parse_expression(parser)?);
    }
    parser.expect(&Token::RParen)?;

    Ok(Expression::InList {
        expr: Box::new(expr),
        list,
        negated,
    })
}

/// Parse additive expression (+, -, ||)
fn parse_additive_expression(parser: &mut Parser) -> Result<Expression> {
    let mut left = parse_multiplicative_expression(parser)?;

    loop {
        let op = match parser.current() {
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Concat => Some(BinaryOperator::Concat),
            _ => None,
        };

        if let Some(op) = op {
            parser.advance();
            let right = parse_multiplicative_expression(parser)?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        } else {
            break;
        }
    }

    Ok(left)
}

/// Parse multiplicative expression (*, /, %)
fn parse_multiplicative_expression(parser: &mut Parser) -> Result<Expression> {
    let mut left = parse_unary_expression(parser)?;

    loop {
        let op = match parser.current() {
            Token::Star => Some(BinaryOperator::Multiply),
            Token::Slash => Some(BinaryOperator::Divide),
            Token::Percent => Some(BinaryOperator::Modulo),
            _ => None,
        };

        if let Some(op) = op {
            parser.advance();
            let right = parse_unary_expression(parser)?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        } else {
            break;
        }
    }

    Ok(left)
}

/// Parse unary expression (-, +)
fn parse_unary_expression(parser: &mut Parser) -> Result<Expression> {
    match parser.current() {
        Token::Minus => {
            parser.advance();
            let expr = parse_unary_expression(parser)?;
            Ok(Expression::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(expr),
            })
        }
        Token::Plus => {
            parser.advance();
            let expr = parse_unary_expression(parser)?;
            Ok(Expression::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(expr),
            })
        }
        _ => parse_postfix_expression(parser),
    }
}

/// Parse postfix expressions (array access, field access, cast, etc.)
fn parse_postfix_expression(parser: &mut Parser) -> Result<Expression> {
    let mut expr = parse_primary_expression(parser)?;

    loop {
        match parser.current() {
            // Array access: expr[index]
            Token::LBracket => {
                parser.advance();
                let index = parse_expression(parser)?;
                parser.expect(&Token::RBracket)?;
                expr = Expression::ArrayAccess {
                    expr: Box::new(expr),
                    index: Box::new(index),
                };
            }
            // Semi-structured access: expr:path or expr::type
            Token::Colon => {
                parser.advance();
                if let Token::Identifier(path) = parser.current().clone() {
                    parser.advance();
                    expr = Expression::SemiStructuredAccess {
                        expr: Box::new(expr),
                        path,
                    };
                } else {
                    return Err(crate::Error::ParseError {
                        message: "Expected identifier after ':'".to_string(),
                        span: None,
                    });
                }
            }
            // Cast: expr::type (Snowflake shorthand syntax)
            Token::DoubleColon => {
                parser.advance();
                let data_type = parse_data_type(parser)?;
                expr = Expression::Cast {
                    expr: Box::new(expr),
                    data_type,
                    shorthand: true,  // :: syntax
                    try_cast: false,  // shorthand is never TRY_CAST
                };
            }
            // Dot access: expr.field
            Token::Dot => {
                parser.advance();
                match parser.current() {
                    Token::Star => {
                        parser.advance();
                        // This is a qualified star like t.*
                        if let Expression::Identifier(name) = expr {
                            expr = Expression::QualifiedStar(name);
                        } else {
                            return Err(crate::Error::ParseError {
                                message: "Invalid qualified star expression".to_string(),
                                span: None,
                            });
                        }
                    }
                    _ => {
                        // Try to get identifier name - either an Identifier or a keyword
                        let name = token_to_identifier_name(parser.current());
                        if let Some(name) = name {
                            parser.advance();
                            // Combine into qualified identifier
                            match expr {
                                Expression::Identifier(first) => {
                                    expr = Expression::QualifiedIdentifier(vec![first, name]);
                                }
                                Expression::QualifiedIdentifier(mut parts) => {
                                    parts.push(name);
                                    expr = Expression::QualifiedIdentifier(parts);
                                }
                                _ => {
                                    return Err(crate::Error::ParseError {
                                        message: "Invalid qualified identifier".to_string(),
                                        span: None,
                                    });
                                }
                            }
                        } else {
                            return Err(crate::Error::ParseError {
                                message: "Expected identifier or * after '.'".to_string(),
                                span: None,
                            });
                        }
                    }
                }
            }
            _ => break,
        }
    }

    Ok(expr)
}

/// Check if this is a row pattern modifier (FINAL or RUNNING) for MATCH_RECOGNIZE
fn is_row_pattern_modifier(name: &str) -> bool {
    let upper = name.to_uppercase();
    upper == "FINAL" || upper == "RUNNING"
}

/// Check if this is a prefix operator like CONNECT_BY_ROOT
fn is_prefix_operator(name: &str) -> bool {
    name.eq_ignore_ascii_case("CONNECT_BY_ROOT")
}

/// Check if this is a keyword prefix operator
fn is_keyword_prefix_operator(token: &Token) -> bool {
    matches!(token, Token::Prior)
}

/// Check if current token starts a function (identifier followed by '(' or keyword function)
fn is_function_start(parser: &Parser) -> bool {
    match parser.current() {
        Token::Identifier(_) => true,
        Token::Last | Token::First | Token::Match => true,
        _ => false,
    }
}

/// Parse primary expression (literals, identifiers, function calls, etc.)
fn parse_primary_expression(parser: &mut Parser) -> Result<Expression> {
    match parser.current().clone() {
        // Literals
        Token::IntegerLiteral(n) => {
            parser.advance();
            Ok(Expression::Literal(Literal::Integer(n)))
        }
        Token::FloatLiteral(n) => {
            parser.advance();
            Ok(Expression::Literal(Literal::Float(n)))
        }
        Token::StringLiteral(s) => {
            parser.advance();
            Ok(Expression::Literal(Literal::String(s)))
        }
        Token::True => {
            parser.advance();
            Ok(Expression::Literal(Literal::Boolean(true)))
        }
        Token::False => {
            parser.advance();
            Ok(Expression::Literal(Literal::Boolean(false)))
        }
        Token::Null => {
            parser.advance();
            Ok(Expression::Literal(Literal::Null))
        }

        // Star (for SELECT *)
        Token::Star => {
            parser.advance();
            Ok(Expression::Star)
        }

        // Jinja expressions
        Token::JinjaExpression(content) => {
            parser.advance();
            Ok(Expression::JinjaExpression(content))
        }
        Token::JinjaStatement(content) => {
            parser.advance();
            Ok(Expression::JinjaBlock(content))
        }

        // CASE expression
        Token::Case => parse_case_expression(parser),

        // CAST function
        Token::Cast => parse_cast_expression(parser, false),

        // TRY_CAST function
        Token::TryCast => parse_cast_expression(parser, true),

        // EXTRACT function
        Token::Extract => parse_extract_expression(parser),

        // Parenthesized expression or subquery
        Token::LParen => {
            parser.advance();
            // Check for subquery
            if parser.check(&Token::Select) || parser.check(&Token::With) {
                let subquery = super::stmt::parse_select_statement(parser)?;
                parser.expect(&Token::RParen)?;
                return Ok(Expression::Subquery(Box::new(subquery)));
            }
            let expr = parse_expression(parser)?;
            parser.expect(&Token::RParen)?;
            Ok(Expression::Parenthesized(Box::new(expr)))
        }

        // Identifier or function call
        Token::Identifier(name) => {
            parser.advance();
            // Check if it's a function call
            if parser.check(&Token::LParen) {
                parse_function_call(parser, name)
            } else if is_row_pattern_modifier(&name) && is_function_start(parser) {
                // FINAL/RUNNING modifiers for aggregate functions in MATCH_RECOGNIZE
                let inner_expr = parse_primary_expression(parser)?;
                Ok(Expression::RowPatternModifier {
                    modifier: name.to_uppercase(),
                    expr: Box::new(inner_expr),
                })
            } else if is_prefix_operator(&name) {
                // Prefix operators like CONNECT_BY_ROOT that take an expression
                let inner_expr = parse_primary_expression(parser)?;
                Ok(Expression::FunctionCall {
                    name: name.to_uppercase(),
                    args: vec![inner_expr],
                    within_group: None,
                    over: None,
                })
            } else {
                Ok(Expression::Identifier(name))
            }
        }

        // Quoted identifier
        Token::QuotedIdentifier(name) => {
            parser.advance();
            Ok(Expression::Identifier(name))
        }

        // DATE - can be either a date literal (DATE 'yyyy-mm-dd') or an identifier
        Token::Date => {
            let pos = parser.position();
            parser.advance();
            if let Token::StringLiteral(s) = parser.current().clone() {
                // DATE followed by string literal is a date literal
                parser.advance();
                Ok(Expression::Literal(Literal::Date(s)))
            } else {
                // DATE followed by anything else - treat as identifier
                parser.restore(pos);
                parser.advance(); // consume DATE token
                Ok(Expression::Identifier("date".to_string()))
            }
        }

        // TIMESTAMP - can be either a timestamp literal (TIMESTAMP 'yyyy-mm-dd...') or an identifier
        Token::Timestamp => {
            let pos = parser.position();
            parser.advance();
            if let Token::StringLiteral(s) = parser.current().clone() {
                // TIMESTAMP followed by string literal is a timestamp literal
                parser.advance();
                Ok(Expression::Literal(Literal::Timestamp(s)))
            } else {
                // TIMESTAMP followed by anything else - treat as identifier
                parser.restore(pos);
                parser.advance(); // consume TIMESTAMP token
                Ok(Expression::Identifier("timestamp".to_string()))
            }
        }

        // EXISTS subquery expression
        Token::Exists => {
            parser.advance();
            parser.expect(&Token::LParen)?;
            let subquery = Box::new(crate::parser::stmt::parse_select_statement(parser)?);
            parser.expect(&Token::RParen)?;
            Ok(Expression::Exists { subquery })
        }

        // Positional column reference ($1, $2, etc.)
        Token::Dollar => {
            parser.advance();
            if let Token::IntegerLiteral(n) = parser.current().clone() {
                parser.advance();
                Ok(Expression::PositionalColumn(n as u32))
            } else {
                Err(parser.error("Expected column number after $"))
            }
        }

        // PRIOR operator in CONNECT BY clause (prefix unary operator)
        Token::Prior => {
            parser.advance();
            let inner_expr = parse_primary_expression(parser)?;
            Ok(Expression::FunctionCall {
                name: "PRIOR".to_string(),
                args: vec![inner_expr],
                within_group: None,
                over: None,
            })
        }

        // Keywords that can be used as function names (LAST, FIRST, MATCH_NUMBER, PREV, etc.)
        Token::Last | Token::First | Token::Match => {
            let name = match parser.current() {
                Token::Last => "last",
                Token::First => "first",
                Token::Match => "match_number",
                _ => unreachable!(),
            }.to_string();
            parser.advance();
            if parser.check(&Token::LParen) {
                parse_function_call(parser, name)
            } else {
                Ok(Expression::Identifier(name))
            }
        }

        _ => Err(crate::Error::ParseError {
            message: format!("Unexpected token: {:?}", parser.current()),
            span: None,
        }),
    }
}

/// Parse CASE expression
fn parse_case_expression(parser: &mut Parser) -> Result<Expression> {
    parser.expect(&Token::Case)?;

    // Check for searched vs simple CASE
    let operand = if !parser.check(&Token::When) {
        Some(Box::new(parse_expression(parser)?))
    } else {
        None
    };

    let mut when_clauses = Vec::new();
    while parser.consume(&Token::When) {
        let condition = parse_expression(parser)?;
        parser.expect(&Token::Then)?;
        let result = parse_expression(parser)?;
        when_clauses.push(WhenClause { condition, result });
    }

    let else_clause = if parser.consume(&Token::Else) {
        Some(Box::new(parse_expression(parser)?))
    } else {
        None
    };

    parser.expect(&Token::End)?;

    Ok(Expression::Case(CaseExpression {
        operand,
        when_clauses,
        else_clause,
    }))
}

/// Parse CAST or TRY_CAST expression
fn parse_cast_expression(parser: &mut Parser, try_cast: bool) -> Result<Expression> {
    // Consume either CAST or TRY_CAST token
    if try_cast {
        parser.expect(&Token::TryCast)?;
    } else {
        parser.expect(&Token::Cast)?;
    }
    parser.expect(&Token::LParen)?;
    let expr = parse_expression(parser)?;
    parser.expect(&Token::As)?;
    let data_type = parse_data_type(parser)?;
    parser.expect(&Token::RParen)?;

    Ok(Expression::Cast {
        expr: Box::new(expr),
        data_type,
        shorthand: false,  // explicit CAST/TRY_CAST(x AS type) syntax
        try_cast,
    })
}

/// Parse EXTRACT expression
fn parse_extract_expression(parser: &mut Parser) -> Result<Expression> {
    parser.expect(&Token::Extract)?;
    parser.expect(&Token::LParen)?;

    // Extract the date part (should be an identifier)
    let field = match parser.current().clone() {
        Token::Identifier(s) => {
            parser.advance();
            s
        }
        _ => {
            // Handle keywords that could be date parts
            let field = format!("{:?}", parser.current());
            parser.advance();
            field
        }
    };

    parser.expect(&Token::From)?;
    let expr = parse_expression(parser)?;
    parser.expect(&Token::RParen)?;

    Ok(Expression::Extract {
        field,
        expr: Box::new(expr),
    })
}

/// Parse function call
fn parse_function_call(parser: &mut Parser, name: String) -> Result<Expression> {
    parser.expect(&Token::LParen)?;

    let mut args = Vec::new();

    // Handle COUNT(*) and similar
    if parser.check(&Token::Star) && matches!(name.to_uppercase().as_str(), "COUNT") {
        parser.advance();
        args.push(Expression::Star);
    } else if !parser.check(&Token::RParen) {
        // Parse DISTINCT if present
        let _has_distinct = parser.consume(&Token::Distinct);

        // Check for subquery as argument (e.g., ANY(SELECT ...), ALL(SELECT ...))
        if parser.check(&Token::Select) || parser.check(&Token::With) {
            let subquery = super::stmt::parse_select_statement(parser)?;
            args.push(Expression::Subquery(Box::new(subquery)));
        } else {
            args.push(parse_expression(parser)?);
        }
        while parser.consume(&Token::Comma) {
            // Check for subquery in each argument position
            if parser.check(&Token::Select) || parser.check(&Token::With) {
                let subquery = super::stmt::parse_select_statement(parser)?;
                args.push(Expression::Subquery(Box::new(subquery)));
            } else {
                args.push(parse_expression(parser)?);
            }
        }
    }

    parser.expect(&Token::RParen)?;

    // Check for WITHIN GROUP (ORDER BY ...) - ordered-set aggregate functions
    let within_group = if parser.consume(&Token::Within) {
        parser.expect(&Token::Group)?;
        parser.expect(&Token::LParen)?;
        parser.expect(&Token::Order)?;
        parser.expect(&Token::By)?;
        let items = parse_order_by_items(parser)?;
        parser.expect(&Token::RParen)?;
        Some(items)
    } else {
        None
    };

    // Check for OVER clause (window function)
    let over = if parser.consume(&Token::Over) {
        if parser.check(&Token::LParen) {
            // OVER (...) - inline window specification
            Some(parse_window_spec(parser)?)
        } else if let Token::Identifier(window_name) = parser.current().clone() {
            // OVER window_name - reference to named window
            parser.advance();
            Some(WindowSpec {
                partition_by: None,
                order_by: None,
                frame: None,
                window_name: Some(window_name),
            })
        } else {
            return Err(crate::Error::ParseError {
                message: format!("Expected window specification or window name after OVER, found {:?}", parser.current()),
                span: None,
            });
        }
    } else {
        None
    };

    Ok(Expression::FunctionCall { name, args, within_group, over })
}

/// Parse window specification
fn parse_window_spec(parser: &mut Parser) -> Result<WindowSpec> {
    parser.expect(&Token::LParen)?;

    let mut partition_by = None;
    let mut order_by = None;
    let mut frame = None;

    // PARTITION BY
    if parser.consume(&Token::Partition) {
        parser.expect(&Token::By)?;
        let mut exprs = vec![parse_expression(parser)?];
        while parser.consume(&Token::Comma) {
            exprs.push(parse_expression(parser)?);
        }
        partition_by = Some(exprs);
    }

    // ORDER BY
    if parser.consume(&Token::Order) {
        parser.expect(&Token::By)?;
        order_by = Some(parse_order_by_items(parser)?);
    }

    // Window frame (ROWS, RANGE, GROUPS)
    let frame_unit = match parser.current() {
        Token::Rows => {
            parser.advance();
            Some(WindowFrameUnit::Rows)
        }
        Token::Range => {
            parser.advance();
            Some(WindowFrameUnit::Range)
        }
        Token::Groups => {
            parser.advance();
            Some(WindowFrameUnit::Groups)
        }
        _ => None,
    };

    if let Some(unit) = frame_unit {
        // Check for BETWEEN ... AND ... syntax
        let has_between = parser.consume(&Token::Between);
        let start = parse_window_frame_bound(parser)?;
        let end = if has_between {
            // BETWEEN requires AND
            parser.expect(&Token::And)?;
            Some(parse_window_frame_bound(parser)?)
        } else if parser.consume(&Token::And) {
            // Optional AND for non-BETWEEN syntax
            Some(parse_window_frame_bound(parser)?)
        } else {
            None
        };
        frame = Some(WindowFrame { unit, start, end });
    }

    parser.expect(&Token::RParen)?;

    Ok(WindowSpec {
        partition_by,
        order_by,
        frame,
        window_name: None,
    })
}

/// Parse window frame bound
fn parse_window_frame_bound(parser: &mut Parser) -> Result<WindowFrameBound> {
    if parser.consume(&Token::Current) {
        parser.expect(&Token::Row)?;
        return Ok(WindowFrameBound::CurrentRow);
    }

    if parser.consume(&Token::Unbounded) {
        if parser.consume(&Token::Preceding) {
            return Ok(WindowFrameBound::UnboundedPreceding);
        } else if parser.consume(&Token::Following) {
            return Ok(WindowFrameBound::UnboundedFollowing);
        }
    }

    // Check for INTERVAL bound (e.g., INTERVAL '1 hour' PRECEDING)
    if parser.consume(&Token::Interval) {
        if let Token::StringLiteral(value) = parser.current().clone() {
            parser.advance();
            // The unit might be inside the string (e.g., '1 hour') or as a separate identifier
            // Snowflake typically uses INTERVAL 'value unit' syntax
            if parser.consume(&Token::Preceding) {
                return Ok(WindowFrameBound::Preceding(FrameBoundValue::Interval {
                    value,
                    unit: String::new(),  // Unit is part of the value string
                }));
            } else if parser.consume(&Token::Following) {
                return Ok(WindowFrameBound::Following(FrameBoundValue::Interval {
                    value,
                    unit: String::new(),
                }));
            }
        }
    }

    // Check for numeric bound
    if let Token::IntegerLiteral(n) = parser.current().clone() {
        parser.advance();
        if parser.consume(&Token::Preceding) {
            return Ok(WindowFrameBound::Preceding(FrameBoundValue::Numeric(n as u64)));
        } else if parser.consume(&Token::Following) {
            return Ok(WindowFrameBound::Following(FrameBoundValue::Numeric(n as u64)));
        }
    }

    Err(crate::Error::ParseError {
        message: "Invalid window frame bound".to_string(),
        span: None,
    })
}

/// Parse ORDER BY items
pub fn parse_order_by_items(parser: &mut Parser) -> Result<Vec<OrderByItem>> {
    let mut items = vec![parse_order_by_item(parser)?];
    while parser.consume(&Token::Comma) {
        items.push(parse_order_by_item(parser)?);
    }
    Ok(items)
}

/// Parse a single ORDER BY item
fn parse_order_by_item(parser: &mut Parser) -> Result<OrderByItem> {
    let expr = parse_expression(parser)?;

    let direction = if parser.consume(&Token::Asc) {
        Some(SortDirection::Asc)
    } else if parser.consume(&Token::Desc) {
        Some(SortDirection::Desc)
    } else {
        None
    };

    let nulls = if parser.consume(&Token::Nulls) {
        if parser.consume(&Token::First) {
            Some(NullsOrder::First)
        } else if parser.consume(&Token::Last) {
            Some(NullsOrder::Last)
        } else {
            return Err(crate::Error::ParseError {
                message: "Expected FIRST or LAST after NULLS".to_string(),
                span: None,
            });
        }
    } else {
        None
    };

    Ok(OrderByItem {
        expr,
        direction,
        nulls,
    })
}

/// Parse data type
pub fn parse_data_type(parser: &mut Parser) -> Result<DataType> {
    match parser.current().clone() {
        Token::Identifier(name) => {
            parser.advance();
            let upper = name.to_uppercase();
            match upper.as_str() {
                "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
                "INTEGER" => Ok(DataType::Integer),
                "INT" => Ok(DataType::Int),
                "BIGINT" => Ok(DataType::BigInt),
                "FLOAT" | "REAL" => Ok(DataType::Float),
                "DOUBLE" => Ok(DataType::Double),
                "DECIMAL" | "NUMERIC" => {
                    if parser.consume(&Token::LParen) {
                        let precision = if let Token::IntegerLiteral(n) = parser.current().clone() {
                            parser.advance();
                            Some(n as u8)
                        } else {
                            None
                        };
                        let scale = if parser.consume(&Token::Comma) {
                            if let Token::IntegerLiteral(n) = parser.current().clone() {
                                parser.advance();
                                Some(n as u8)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        parser.expect(&Token::RParen)?;
                        Ok(DataType::Decimal(precision, scale))
                    } else {
                        Ok(DataType::Decimal(None, None))
                    }
                }
                "NUMBER" => {
                    if parser.consume(&Token::LParen) {
                        let precision = if let Token::IntegerLiteral(n) = parser.current().clone() {
                            parser.advance();
                            Some(n as u8)
                        } else {
                            None
                        };
                        let scale = if parser.consume(&Token::Comma) {
                            if let Token::IntegerLiteral(n) = parser.current().clone() {
                                parser.advance();
                                Some(n as u8)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        parser.expect(&Token::RParen)?;
                        Ok(DataType::Number(precision, scale))
                    } else {
                        Ok(DataType::Number(None, None))
                    }
                }
                "VARCHAR" => {
                    if parser.consume(&Token::LParen) {
                        let len = if let Token::IntegerLiteral(n) = parser.current().clone() {
                            parser.advance();
                            Some(n as u32)
                        } else {
                            None
                        };
                        parser.expect(&Token::RParen)?;
                        Ok(DataType::Varchar(len))
                    } else {
                        Ok(DataType::Varchar(None))
                    }
                }
                "STRING" => {
                    if parser.consume(&Token::LParen) {
                        let len = if let Token::IntegerLiteral(n) = parser.current().clone() {
                            parser.advance();
                            Some(n as u32)
                        } else {
                            None
                        };
                        parser.expect(&Token::RParen)?;
                        Ok(DataType::String(len))
                    } else {
                        Ok(DataType::String(None))
                    }
                }
                "TEXT" => Ok(DataType::Text),
                "CHAR" => {
                    if parser.consume(&Token::LParen) {
                        let len = if let Token::IntegerLiteral(n) = parser.current().clone() {
                            parser.advance();
                            Some(n as u32)
                        } else {
                            None
                        };
                        parser.expect(&Token::RParen)?;
                        Ok(DataType::Char(len))
                    } else {
                        Ok(DataType::Char(None))
                    }
                }
                "DATE" => Ok(DataType::Date),
                "TIME" => Ok(DataType::Time),
                "TIMESTAMP" => Ok(DataType::Timestamp),
                "TIMESTAMP_TZ" | "TIMESTAMPTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_NTZ" => {
                    Ok(DataType::TimestampTz)
                }
                "VARIANT" => Ok(DataType::Variant),
                "OBJECT" => Ok(DataType::Object),
                "ARRAY" => Ok(DataType::Array),
                _ => Ok(DataType::Varchar(None)), // Default fallback
            }
        }
        Token::Date => {
            parser.advance();
            Ok(DataType::Date)
        }
        Token::Timestamp => {
            parser.advance();
            Ok(DataType::Timestamp)
        }
        _ => Err(crate::Error::ParseError {
            message: format!("Expected data type, found {:?}", parser.current()),
            span: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::lexer::tokenize;

    fn parse_expr(input: &str) -> Result<Expression> {
        let tokens = tokenize(input)?;
        let mut parser = Parser::new(&tokens);
        parse_expression(&mut parser)
    }

    #[test]
    fn test_parse_literal_integer() {
        let expr = parse_expr("42").unwrap();
        assert!(matches!(expr, Expression::Literal(Literal::Integer(42))));
    }

    #[test]
    fn test_parse_literal_string() {
        let expr = parse_expr("'hello'").unwrap();
        assert!(matches!(expr, Expression::Literal(Literal::String(s)) if s == "hello"));
    }

    #[test]
    fn test_parse_identifier() {
        let expr = parse_expr("foo").unwrap();
        assert!(matches!(expr, Expression::Identifier(s) if s == "foo"));
    }

    #[test]
    fn test_parse_binary_add() {
        let expr = parse_expr("1 + 2").unwrap();
        assert!(matches!(expr, Expression::BinaryOp { op: BinaryOperator::Plus, .. }));
    }

    #[test]
    fn test_parse_qualified_identifier() {
        let expr = parse_expr("schema.table").unwrap();
        assert!(matches!(expr, Expression::QualifiedIdentifier(parts) if parts == vec!["schema", "table"]));
    }

    #[test]
    fn test_parse_function_call() {
        let expr = parse_expr("count(*)").unwrap();
        assert!(matches!(expr, Expression::FunctionCall { name, .. } if name == "count"));
    }

    #[test]
    fn test_parse_case_expression() {
        let expr = parse_expr("CASE WHEN x = 1 THEN 'a' ELSE 'b' END").unwrap();
        assert!(matches!(expr, Expression::Case(_)));
    }
}
