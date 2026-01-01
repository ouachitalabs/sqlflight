//! Statement parsing

use crate::ast::Statement;
use crate::Result;

/// Parse a SQL statement
pub fn parse_statement(_input: &str) -> Result<Statement> {
    // TODO: Implement statement parser
    Err(crate::Error::ParseError {
        message: "Statement parser not implemented".to_string(),
        span: None,
    })
}
