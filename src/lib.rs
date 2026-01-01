//! sqlflight - A fast, opinionated SQL formatter for Snowflake with first-class Jinja support
//!
//! This library provides the core formatting functionality for the sqlflight CLI.

pub mod ast;
pub mod cli;
pub mod error;
pub mod formatter;
pub mod jinja;
pub mod parser;

pub use error::{Error, Result};
pub use formatter::format_sql;

/// Format SQL string and return the formatted result
pub fn format(input: &str) -> Result<String> {
    format_sql(input)
}

/// Check if SQL string is already formatted
pub fn check(input: &str) -> Result<bool> {
    let formatted = format_sql(input)?;
    Ok(formatted == input)
}
