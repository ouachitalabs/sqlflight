//! SQL parser using winnow parser combinators

pub mod expr;
pub mod lexer;
pub mod snowflake;
pub mod stmt;

use crate::ast::Statement;
use crate::error::{Error, Result};

/// Parse SQL string into AST
pub fn parse(input: &str) -> Result<Statement> {
    // TODO: Implement parser
    Err(Error::ParseError {
        message: "Parser not implemented".to_string(),
        span: None,
    })
}

/// Parse multiple SQL statements
pub fn parse_statements(input: &str) -> Result<Vec<Statement>> {
    // TODO: Implement multi-statement parser
    Err(Error::ParseError {
        message: "Parser not implemented".to_string(),
        span: None,
    })
}
