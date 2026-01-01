//! Jinja reintegration into formatted SQL
//!
//! Replaces placeholders with original Jinja content.

use super::JinjaPlaceholder;
use std::collections::HashMap;

/// Replace all placeholders with their original Jinja content
pub fn reintegrate(formatted: &str, placeholders: &HashMap<String, JinjaPlaceholder>) -> String {
    let mut result = formatted.to_string();

    for (id, placeholder) in placeholders {
        result = result.replace(id, &placeholder.original);
    }

    result
}
