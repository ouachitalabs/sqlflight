//! SQL formatting / pretty-printing logic

pub mod printer;
pub mod rules;

use crate::ast::Statement;
use crate::error::Result;
use crate::jinja;
use crate::parser;

/// Format SQL string
pub fn format_sql(input: &str) -> Result<String> {
    // Step 1: Extract Jinja
    let (sql_with_placeholders, placeholders) = jinja::extract_jinja(input)?;

    // Step 2: Parse SQL
    let ast = parser::parse(&sql_with_placeholders)?;

    // Step 3: Format AST
    let formatted = format_ast(&ast)?;

    // Step 4: Reintegrate Jinja
    let result = jinja::reintegrate_jinja(&formatted, &placeholders);

    Ok(result)
}

/// Format AST to string
pub fn format_ast(_ast: &Statement) -> Result<String> {
    // TODO: Implement AST formatter
    Err(crate::Error::FormatError {
        message: "Formatter not implemented".to_string(),
    })
}
