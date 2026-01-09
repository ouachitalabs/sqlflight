//! Jinja reintegration into formatted SQL
//!
//! Replaces placeholders with original Jinja content.

use super::JinjaPlaceholder;
use std::collections::HashMap;

/// Replace all placeholders with their Jinja content
/// Uses the formatted version if available (for SqlBlock), otherwise uses original
pub fn reintegrate(formatted: &str, placeholders: &HashMap<String, JinjaPlaceholder>) -> String {
    let mut result = formatted.to_string();

    for (id, placeholder) in placeholders {
        // Use formatted version if available, otherwise use original
        let replacement = placeholder.formatted.as_ref().unwrap_or(&placeholder.original);
        result = result.replace(id, replacement);
    }

    result
}
