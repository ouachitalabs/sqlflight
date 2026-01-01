//! SQL parser using winnow parser combinators

pub mod expr;
pub mod lexer;
pub mod snowflake;
pub mod stmt;

use crate::ast::Statement;
use crate::error::{Error, Result};
use expr::Parser;
use lexer::{tokenize, Token};

/// Parse SQL string into AST
pub fn parse(input: &str) -> Result<Statement> {
    // Step 1: Tokenize
    let tokens = tokenize(input)?;

    // Handle empty input
    if tokens.is_empty() {
        return Err(Error::ParseError {
            message: "Empty input".to_string(),
            span: None,
        });
    }

    // Step 2: Parse tokens into AST
    let mut parser = Parser::new(&tokens);
    let stmt = stmt::parse_statement(&mut parser)?;

    // Step 3: Ensure all tokens were consumed (except optional trailing semicolon and EOF)
    while parser.check(&Token::Semicolon) {
        parser.advance();
    }
    if !parser.is_eof() {
        return Err(Error::ParseError {
            message: format!("Unexpected token after statement: {:?}", parser.current()),
            span: None,
        });
    }

    Ok(stmt)
}

/// Parse multiple SQL statements
pub fn parse_statements(input: &str) -> Result<Vec<Statement>> {
    // Step 1: Tokenize
    let tokens = tokenize(input)?;

    // Handle empty input
    if tokens.is_empty() {
        return Ok(vec![]);
    }

    // Step 2: Parse tokens into AST statements
    let mut parser = Parser::new(&tokens);
    let mut statements = Vec::new();

    while !parser.is_eof() {
        // Skip any leading semicolons
        while parser.check(&Token::Semicolon) {
            parser.advance();
        }

        if parser.is_eof() {
            break;
        }

        let stmt = stmt::parse_statement(&mut parser)?;
        statements.push(stmt);

        // Consume optional trailing semicolon
        if parser.check(&Token::Semicolon) {
            parser.advance();
        }
    }

    Ok(statements)
}
