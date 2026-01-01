//! Expression parsing

use crate::ast::Expression;
use crate::Result;

/// Parse an expression
pub fn parse_expression(_input: &str) -> Result<Expression> {
    // TODO: Implement expression parser
    Err(crate::Error::ParseError {
        message: "Expression parser not implemented".to_string(),
        span: None,
    })
}
