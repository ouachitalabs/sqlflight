//! Utility functions for the SQL formatter

use crate::jinja;

/// Check if a string is a Jinja placeholder that should be preserved as-is
pub(crate) fn is_jinja_placeholder(s: &str) -> bool {
    let upper = s.to_uppercase();
    upper.starts_with(jinja::PLACEHOLDER_PREFIX)
}

/// Check if a string contains an embedded Jinja placeholder
pub(crate) fn contains_jinja_placeholder(s: &str) -> bool {
    s.to_uppercase().contains(jinja::PLACEHOLDER_PREFIX)
}

/// Format an identifier, preserving Jinja placeholders
pub(crate) fn format_identifier(s: &str) -> String {
    if is_jinja_placeholder(s) {
        // Pure placeholder - preserve as-is
        s.to_string()
    } else if contains_jinja_placeholder(s) {
        // Contains embedded placeholder (e.g., "users__SQLFLIGHT_JINJA_001__")
        // Split on placeholder boundaries and format appropriately
        let upper = s.to_uppercase();
        if let Some(pos) = upper.find(jinja::PLACEHOLDER_PREFIX) {
            // Find the end of the placeholder
            let prefix_len = jinja::PLACEHOLDER_PREFIX.len();
            if let Some(end_offset) = upper[pos + prefix_len..].find("__") {
                let end = pos + prefix_len + end_offset + 2;
                let before = &s[..pos];
                let placeholder = &s[pos..end];
                let after = &s[end..];
                return format!("{}{}{}", before.to_lowercase(), placeholder, format_identifier(after));
            }
        }
        // Fallback to lowercase if pattern doesn't match
        s.to_lowercase()
    } else {
        s.to_lowercase()
    }
}
