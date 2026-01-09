//! Statement-level Jinja placeholder extraction and handling

use crate::jinja;

/// Information about Jinja placeholders at statement level
pub(crate) struct JinjaLineInfo {
    /// Placeholders before the SQL starts
    pub leading: Vec<String>,
    /// The SQL body to parse and format
    pub body: String,
    /// Placeholders after the SQL ends
    pub trailing: Vec<String>,
    /// Placeholders on their own lines within the SQL (with their ORIGINAL line positions)
    pub inline_statements: Vec<(usize, String)>,
    /// Maps body line numbers to original line numbers (for proper interleaving)
    pub body_to_original_line: Vec<usize>,
}

/// Extract Jinja placeholders that appear at statement level (before/after/within SQL)
pub(crate) fn extract_statement_level_placeholders(input: &str) -> JinjaLineInfo {
    let mut leading = Vec::new();
    let mut trailing = Vec::new();
    let mut inline_statements = Vec::new();
    let mut body = input.to_string();

    // First, extract inline leading placeholders (at the very start, no newline)
    loop {
        let trimmed = body.trim_start();
        if let Some(placeholder) = extract_leading_placeholder(trimmed) {
            leading.push(placeholder.clone());
            // Remove the placeholder from the body
            if let Some(pos) = body.to_uppercase().find(&placeholder.to_uppercase()) {
                body = body[pos + placeholder.len()..].to_string();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    // Now process line by line for the rest
    let lines: Vec<&str> = body.lines().collect();
    let mut body_start = 0;

    // Find additional leading placeholders on their own lines
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if is_pure_placeholder_line(trimmed) {
            leading.push(trimmed.to_string());
            body_start = i + 1;
        } else {
            break;
        }
    }

    // Find trailing placeholders on their own lines
    let mut body_end = lines.len();
    for (i, line) in lines.iter().enumerate().rev() {
        if i < body_start {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if is_pure_placeholder_line(trimmed) {
            trailing.insert(0, trimmed.to_string());
            body_end = i;
        } else {
            break;
        }
    }

    // Extract inline placeholders from the middle of the body
    // These are Jinja-only statements that appear after comments or between SQL statements
    // Also build a mapping from body line numbers to original line numbers
    //
    // IMPORTANT: Only extract placeholders that are NOT part of SQL continuations.
    // A placeholder is part of SQL if:
    // - The previous non-empty line ends with SQL continuation (comma, AND, OR, etc.)
    // - The next non-empty line starts with SQL continuation (FROM, JOIN, WHERE, etc.)
    let mut body_lines: Vec<&str> = Vec::new();
    let mut body_to_original_line: Vec<usize> = Vec::new();

    let relevant_lines = &lines[body_start..body_end];
    for (i, line) in relevant_lines.iter().enumerate() {
        let original_line = body_start + i;
        let trimmed = line.trim();
        if is_pure_placeholder_line(trimmed) && is_statement_level_placeholder(relevant_lines, i) {
            // Track the placeholder with its ORIGINAL position
            inline_statements.push((original_line, trimmed.to_string()));
            // Skip this line (don't include in body_lines)
        } else {
            body_lines.push(*line);
            body_to_original_line.push(original_line);
        }
    }

    let final_body = body_lines.join("\n");

    JinjaLineInfo {
        leading,
        body: final_body, // Don't trim - it affects line number calculations
        trailing,
        inline_statements,
        body_to_original_line,
    }
}

/// Check if a line contains only a Jinja placeholder (no other SQL)
pub(crate) fn is_pure_placeholder_line(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Check if it starts with our placeholder prefix
    let upper = trimmed.to_uppercase();
    if !upper.starts_with(jinja::PLACEHOLDER_PREFIX) {
        return false;
    }
    // Find the end of the placeholder
    let prefix_len = jinja::PLACEHOLDER_PREFIX.len();
    if let Some(end_offset) = upper[prefix_len..].find("__") {
        let end = prefix_len + end_offset + 2;
        // Line should be only the placeholder (possibly with whitespace)
        return end == trimmed.len();
    }
    false
}

/// Check if a placeholder line is at statement level (not inline within SQL)
/// A placeholder is statement-level if:
/// - It's not preceded by SQL continuation (comma, open paren, operators)
/// - It's not followed by SQL continuation (FROM, JOIN, WHERE, etc.)
pub(crate) fn is_statement_level_placeholder(lines: &[&str], placeholder_idx: usize) -> bool {
    // SQL tokens that indicate continuation (placeholder is inline, not statement-level)
    const SQL_CONTINUATION_ENDINGS: &[&str] = &[",", "(", "AND", "OR", "+", "-", "*", "/", "||", "=", "<", ">", "!=", "<>", "THEN", "ELSE", "WHEN", "AS"];
    const SQL_CONTINUATION_STARTS: &[&str] = &["FROM", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", ")", "AND", "OR", "ON", "USING", "THEN", "ELSE", "WHEN", "END"];

    // Find previous non-empty, non-comment, non-placeholder line
    let mut prev_sql = None;
    for i in (0..placeholder_idx).rev() {
        let trimmed = lines[i].trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") && !is_pure_placeholder_line(trimmed) {
            prev_sql = Some(trimmed);
            break;
        }
    }

    // Find next non-empty, non-comment, non-placeholder line
    let mut next_sql = None;
    for i in (placeholder_idx + 1)..lines.len() {
        let trimmed = lines[i].trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") && !is_pure_placeholder_line(trimmed) {
            next_sql = Some(trimmed);
            break;
        }
    }

    // Check if previous line ends with SQL continuation
    if let Some(prev) = prev_sql {
        let prev_upper = prev.to_uppercase();
        for ending in SQL_CONTINUATION_ENDINGS {
            if prev_upper.ends_with(ending) || prev.ends_with(ending.to_lowercase().as_str()) {
                return false; // Inline SQL
            }
        }
    }

    // Check if next line starts with SQL continuation
    if let Some(next) = next_sql {
        let next_upper = next.to_uppercase();
        let first_word = next_upper.split_whitespace().next().unwrap_or("");
        for start in SQL_CONTINUATION_STARTS {
            if first_word == *start || next.starts_with(*start) || next.starts_with(&start.to_lowercase()) {
                return false; // Inline SQL
            }
        }
    }

    // If we get here, the placeholder is at statement level
    true
}

/// Extract a Jinja placeholder from the start of a string
fn extract_leading_placeholder(s: &str) -> Option<String> {
    let upper = s.to_uppercase();
    if upper.starts_with(jinja::PLACEHOLDER_PREFIX) {
        // Jinja placeholders end with __ (format: __SQLFLIGHT_JINJA_NNN__)
        // Find the closing __
        let prefix_len = jinja::PLACEHOLDER_PREFIX.len();
        if let Some(end_pos) = upper[prefix_len..].find("__") {
            let full_end = prefix_len + end_pos + 2; // +2 for the __
            if full_end <= s.len() {
                return Some(s[..full_end].to_string());
            }
        }
    }
    None
}
