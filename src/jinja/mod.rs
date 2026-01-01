//! Jinja extraction and reintegration
//!
//! This module handles the two-pass approach for Jinja templating:
//! 1. Extract: Identify and extract Jinja expressions/blocks, replacing with magic identifiers
//! 2. Format: Parse and format the resulting SQL
//! 3. Reintegrate: Replace placeholders with original Jinja content

pub mod extract;
pub mod reintegrate;

use crate::Result;
use std::collections::HashMap;

/// Magic prefix for Jinja placeholders
pub const PLACEHOLDER_PREFIX: &str = "__SQLFLIGHT_JINJA_";

/// Jinja placeholder information
#[derive(Debug, Clone)]
pub struct JinjaPlaceholder {
    /// The placeholder identifier (e.g., __SQLFLIGHT_JINJA_001__)
    pub id: String,
    /// The original Jinja content
    pub original: String,
    /// The type of Jinja construct
    pub kind: JinjaKind,
}

/// Types of Jinja constructs
#[derive(Debug, Clone, PartialEq)]
pub enum JinjaKind {
    /// {{ expression }}
    Expression,
    /// {% statement %}
    Statement,
    /// {# comment #}
    Comment,
}

/// Extract Jinja from SQL and return SQL with placeholders + mapping
pub fn extract_jinja(input: &str) -> Result<(String, HashMap<String, JinjaPlaceholder>)> {
    extract::extract(input)
}

/// Reintegrate Jinja back into formatted SQL
pub fn reintegrate_jinja(
    formatted: &str,
    placeholders: &HashMap<String, JinjaPlaceholder>,
) -> String {
    reintegrate::reintegrate(formatted, placeholders)
}
