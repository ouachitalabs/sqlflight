//! Jinja extraction from SQL
//!
//! Identifies and extracts Jinja expressions, statements, and comments,
//! replacing them with magic identifiers.

use super::{JinjaKind, JinjaPlaceholder, PLACEHOLDER_PREFIX};
use crate::Result;
use std::collections::HashMap;

/// Block-forming Jinja tags that should be extracted as a single placeholder.
/// This includes both tags with non-SQL content AND control flow blocks that cannot
/// be parsed as valid SQL (because they contain alternative branches, not cumulative content).
/// Format: (start_keyword, end_tag, is_always_block, format_sql_content)
/// - is_always_block: true if the tag is always block-forming (not conditional like set)
/// - format_sql_content: true if the content should be formatted as SQL (vs preserved verbatim)
const BLOCK_FORMING_TAGS: &[(&str, &str, bool, bool)] = &[
    ("set", "endset", false, true),       // Only block if no '=' - format SQL inside
    ("macro", "endmacro", true, false),   // Macro content is template code, not SQL
    ("call", "endcall", true, false),     // Call blocks contain template code
    ("snapshot", "endsnapshot", false, true), // Snapshot contains SQL - format it
    ("filter", "endfilter", true, false), // Filter blocks contain template expressions
    ("block", "endblock", true, false),   // Block content is template code
    ("raw", "endraw", true, false),       // Raw blocks are never processed
    ("for", "endfor", true, false),       // For loops generate dynamic content
    ("if", "endif", true, false),         // If blocks have alternative branches - not valid SQL as-is
];

/// Check if a statement content starts with a block-forming keyword that uses raw content
/// (i.e., {% set name %} without assignment)
/// Returns Some((end_tag, format_sql_content)) if block-forming, None otherwise
fn is_raw_content_block(content: &str) -> Option<(&'static str, bool)> {
    let trimmed = content.trim();

    for (start_keyword, end_tag, is_always_block, format_sql) in BLOCK_FORMING_TAGS {
        if trimmed.starts_with(start_keyword) {
            // Check that it's followed by a space or end (not part of a longer word)
            let after_keyword = &trimmed[start_keyword.len()..];
            if !after_keyword.is_empty() && !after_keyword.starts_with(char::is_whitespace) {
                continue;
            }

            if *is_always_block {
                // These are always block-forming
                return Some((end_tag, *format_sql));
            }

            // For set/snapshot, it's a block form if there's NO '=' after the variable name
            if *start_keyword == "set" || *start_keyword == "snapshot" {
                let rest = after_keyword.trim();
                if !rest.contains('=') && !rest.is_empty() {
                    return Some((end_tag, *format_sql));
                }
            }
        }
    }
    None
}

/// Extract all Jinja constructs from input and replace with placeholders
pub fn extract(input: &str) -> Result<(String, HashMap<String, JinjaPlaceholder>)> {
    let mut result = String::with_capacity(input.len());
    let mut placeholders = HashMap::new();
    let mut counter = 0;
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '{' {
            match chars.peek() {
                Some('{') => {
                    // Jinja expression {{ ... }}
                    chars.next(); // consume second {
                    let content = extract_until(&mut chars, "}}")?;
                    let placeholder = create_placeholder(&mut counter, content, JinjaKind::Expression);
                    // Add space if previous char was alphanumeric to prevent merging
                    if result.chars().last().map(|c| c.is_alphanumeric()).unwrap_or(false) {
                        result.push(' ');
                    }
                    result.push_str(&placeholder.id);
                    placeholders.insert(placeholder.id.clone(), placeholder);
                }
                Some('%') => {
                    // Jinja statement {% ... %} or {%- ... -%}
                    chars.next(); // consume %
                    // Handle optional whitespace control: {%- ... -%}
                    let starts_with_trim = chars.peek() == Some(&'-');
                    if starts_with_trim {
                        chars.next(); // consume leading -
                    }
                    let stmt = extract_until_statement_end(&mut chars)?;

                    // Check if this is a block-forming statement with raw content
                    if let Some((end_tag, format_sql)) = is_raw_content_block(&stmt.content) {
                        // Extract until we find the matching end tag
                        let block_content = extract_block_content(&mut chars, end_tag)?;
                        // Preserve whitespace control markers
                        let start_marker = if starts_with_trim { "{%-" } else { "{%" };
                        let end_marker = if stmt.ends_with_trim { "-%}" } else { "%}" };
                        let end_start = if block_content.end_starts_trim { "{%-" } else { "{%" };
                        let end_end = if block_content.end_ends_trim { "-%}" } else { "%}" };

                        let start_tag_str = format!("{}{}{}", start_marker, stmt.content, end_marker);
                        let end_tag_str = format!("{} {} {}", end_start, end_tag, end_end);

                        counter += 1;
                        let placeholder = if format_sql {
                            // For SQL blocks, store inner content separately for formatting
                            JinjaPlaceholder {
                                id: format!("{}{:03}__", PLACEHOLDER_PREFIX, counter),
                                original: block_content.content.clone(),
                                kind: JinjaKind::SqlBlock {
                                    start_tag: start_tag_str,
                                    end_tag: end_tag_str,
                                },
                                formatted: None,
                            }
                        } else {
                            // For non-SQL blocks, store full block verbatim
                            let full_original = format!(
                                "{}{}{}",
                                start_tag_str,
                                block_content.content,
                                end_tag_str
                            );
                            JinjaPlaceholder {
                                id: format!("{}{:03}__", PLACEHOLDER_PREFIX, counter),
                                original: full_original,
                                kind: JinjaKind::Statement,
                                formatted: None,
                            }
                        };
                        // Add space if previous char was alphanumeric to prevent merging
                        if result.chars().last().map(|c| c.is_alphanumeric()).unwrap_or(false) {
                            result.push(' ');
                        }
                        result.push_str(&placeholder.id);
                        placeholders.insert(placeholder.id.clone(), placeholder);
                    } else {
                        let placeholder = create_placeholder_with_trim(
                            &mut counter,
                            stmt.content,
                            starts_with_trim,
                            stmt.ends_with_trim,
                        );
                        // Add space if previous char was alphanumeric to prevent merging
                        if result.chars().last().map(|c| c.is_alphanumeric()).unwrap_or(false) {
                            result.push(' ');
                        }
                        result.push_str(&placeholder.id);
                        placeholders.insert(placeholder.id.clone(), placeholder);
                    }
                }
                Some('#') => {
                    // Jinja comment {# ... #}
                    chars.next(); // consume #
                    let content = extract_until(&mut chars, "#}")?;
                    let placeholder = create_placeholder(&mut counter, content, JinjaKind::Comment);
                    // Add space if previous char was alphanumeric to prevent merging
                    if result.chars().last().map(|c| c.is_alphanumeric()).unwrap_or(false) {
                        result.push(' ');
                    }
                    result.push_str(&placeholder.id);
                    placeholders.insert(placeholder.id.clone(), placeholder);
                }
                _ => {
                    result.push(c);
                }
            }
        } else {
            result.push(c);
        }
    }

    Ok((result, placeholders))
}

/// Block content result
struct BlockContent {
    content: String,
    end_starts_trim: bool,  // true if end tag started with {%-
    end_ends_trim: bool,    // true if end tag ended with -%}
}

/// Extract content until we find the matching end tag {% endXXX %} or {%- endXXX -%}
fn extract_block_content(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    end_tag: &str,
) -> Result<BlockContent> {
    let mut content = String::new();
    let mut depth = 1; // We're already inside one block

    // Determine the start tag from the end tag (e.g., "endfor" -> "for")
    let start_tag = if let Some(stripped) = end_tag.strip_prefix("end") {
        Some(stripped)
    } else {
        None
    };

    while let Some(c) = chars.next() {
        content.push(c);

        // Check if we've reached an end tag
        if c == '{' {
            if let Some(&next) = chars.peek() {
                if next == '%' {
                    chars.next();
                    content.push('%');

                    // Handle optional whitespace trim marker {%-
                    let has_trim = chars.peek() == Some(&'-');
                    if has_trim {
                        chars.next();
                        content.push('-');
                    }

                    // Extract the statement content (handles both %} and -%})
                    let stmt = extract_until_statement_end(chars)?;
                    let trimmed = stmt.content.trim();

                    // Check for nested start tag (e.g., another "for" when looking for "endfor")
                    if let Some(start) = start_tag {
                        if trimmed.starts_with(start) {
                            // Check that it's followed by whitespace or end (not a longer word)
                            let after = &trimmed[start.len()..];
                            if after.is_empty() || after.starts_with(char::is_whitespace) {
                                depth += 1;
                            }
                        }
                    }

                    if trimmed == end_tag {
                        depth -= 1;
                        if depth == 0 {
                            // Found the matching end tag! Remove the partial "{%" or "{%-" we added
                            if has_trim {
                                content.pop(); // remove '-'
                            }
                            content.pop(); // remove '%'
                            content.pop(); // remove '{'
                            return Ok(BlockContent {
                                content,
                                end_starts_trim: has_trim,
                                end_ends_trim: stmt.ends_with_trim,
                            });
                        }
                    }

                    // Include the statement in content
                    content.push_str(&stmt.content);
                    // Preserve the original closing marker
                    if stmt.ends_with_trim {
                        content.push_str("-%}");
                    } else {
                        content.push_str("%}");
                    }
                }
            }
        }
    }

    Err(crate::Error::JinjaError {
        message: format!("Unclosed Jinja block, expected {{% {} %}}", end_tag),
    })
}

fn extract_until(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    end: &str,
) -> Result<String> {
    let mut content = String::new();
    let end_chars: Vec<char> = end.chars().collect();

    while let Some(&c) = chars.peek() {
        content.push(c);
        chars.next();

        if content.ends_with(end) {
            // Remove the end delimiter from content
            for _ in 0..end_chars.len() {
                content.pop();
            }
            return Ok(content);
        }
    }

    Err(crate::Error::JinjaError {
        message: format!("Unclosed Jinja construct, expected {}", end),
    })
}

/// Result of extracting statement content
struct StatementContent {
    content: String,
    ends_with_trim: bool,  // true if ended with -%}
}

/// Extract statement content, handling both %} and -%} endings
fn extract_until_statement_end(
    chars: &mut std::iter::Peekable<std::str::Chars>,
) -> Result<StatementContent> {
    let mut content = String::new();

    while let Some(&c) = chars.peek() {
        // Check for -%} or %}
        if c == '-' {
            let mut lookahead = chars.clone();
            lookahead.next(); // consume -
            if lookahead.next() == Some('%') && lookahead.next() == Some('}') {
                // Found -%}, consume and return
                chars.next(); // -
                chars.next(); // %
                chars.next(); // }
                return Ok(StatementContent { content, ends_with_trim: true });
            }
        } else if c == '%' {
            let mut lookahead = chars.clone();
            lookahead.next(); // consume %
            if lookahead.next() == Some('}') {
                // Found %}, consume and return
                chars.next(); // %
                chars.next(); // }
                return Ok(StatementContent { content, ends_with_trim: false });
            }
        }

        content.push(c);
        chars.next();
    }

    Err(crate::Error::JinjaError {
        message: "Unclosed Jinja statement, expected %} or -%}".to_string(),
    })
}

fn create_placeholder(counter: &mut usize, content: String, kind: JinjaKind) -> JinjaPlaceholder {
    *counter += 1;
    let id = format!("{}{:03}__", PLACEHOLDER_PREFIX, counter);
    let original = match &kind {
        JinjaKind::Expression => format!("{{{{{}}}}}", content),
        JinjaKind::Statement => format!("{{%{}%}}", content),
        JinjaKind::Comment => format!("{{#{}#}}", content),
        JinjaKind::SqlBlock { .. } => content.clone(), // SqlBlock stores inner content only
    };
    JinjaPlaceholder {
        id,
        original,
        kind,
        formatted: None,
    }
}

/// Create a placeholder for a Jinja statement with whitespace control markers
fn create_placeholder_with_trim(
    counter: &mut usize,
    content: String,
    starts_with_trim: bool,
    ends_with_trim: bool,
) -> JinjaPlaceholder {
    *counter += 1;
    let id = format!("{}{:03}__", PLACEHOLDER_PREFIX, counter);
    let start = if starts_with_trim { "{%-" } else { "{%" };
    let end = if ends_with_trim { "-%}" } else { "%}" };
    let original = format!("{}{}{}", start, content, end);
    JinjaPlaceholder {
        id,
        original,
        kind: JinjaKind::Statement,
        formatted: None,
    }
}

