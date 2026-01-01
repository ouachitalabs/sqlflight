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
/// Format: (start_keyword, end_tag, is_always_block)
/// is_always_block: true if the tag is always block-forming (not conditional like set)
const BLOCK_FORMING_TAGS: &[(&str, &str, bool)] = &[
    ("set", "endset", false),       // Only block if no '=' ({% set x %}...{% endset %})
    ("macro", "endmacro", true),    // Macro content is template code, not SQL
    ("call", "endcall", true),      // Call blocks contain template code
    ("snapshot", "endsnapshot", false), // Snapshot contains SQL - treat each tag separately
    ("filter", "endfilter", true),  // Filter blocks contain template expressions
    ("block", "endblock", true),    // Block content is template code
    ("raw", "endraw", true),        // Raw blocks are never processed
    ("for", "endfor", true),        // For loops generate dynamic content
    ("if", "endif", true),          // If blocks have alternative branches - not valid SQL as-is
];

/// Check if a statement content starts with a block-forming keyword that uses raw content
/// (i.e., {% set name %} without assignment)
fn is_raw_content_block(content: &str) -> Option<&'static str> {
    let trimmed = content.trim();

    for (start_keyword, end_tag, is_always_block) in BLOCK_FORMING_TAGS {
        if trimmed.starts_with(start_keyword) {
            // Check that it's followed by a space or end (not part of a longer word)
            let after_keyword = &trimmed[start_keyword.len()..];
            if !after_keyword.is_empty() && !after_keyword.starts_with(char::is_whitespace) {
                continue;
            }

            if *is_always_block {
                // These are always block-forming
                return Some(end_tag);
            }

            // For set, it's a block form if there's NO '=' after the variable name
            if *start_keyword == "set" {
                let rest = after_keyword.trim();
                if !rest.contains('=') && !rest.is_empty() {
                    return Some(end_tag);
                }
            }
            // For if, we don't treat it as a raw content block since the content is SQL
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
                    result.push_str(&placeholder.id);
                    placeholders.insert(placeholder.id.clone(), placeholder);
                }
                Some('%') => {
                    // Jinja statement {% ... %}
                    chars.next(); // consume %
                    let content = extract_until(&mut chars, "%}")?;

                    // Check if this is a block-forming statement with raw content
                    if let Some(end_tag) = is_raw_content_block(&content) {
                        // Extract until we find the matching end tag
                        let block_content = extract_block_content(&mut chars, end_tag)?;
                        let full_original = format!("{{%{}%}}{}{{% {} %}}", content, block_content.content, end_tag);

                        let placeholder = JinjaPlaceholder {
                            id: format!("{}{:03}__", PLACEHOLDER_PREFIX, {
                                counter += 1;
                                counter
                            }),
                            original: full_original,
                            kind: JinjaKind::Statement,
                        };
                        result.push_str(&placeholder.id);
                        placeholders.insert(placeholder.id.clone(), placeholder);
                    } else {
                        let placeholder = create_placeholder(&mut counter, content, JinjaKind::Statement);
                        result.push_str(&placeholder.id);
                        placeholders.insert(placeholder.id.clone(), placeholder);
                    }
                }
                Some('#') => {
                    // Jinja comment {# ... #}
                    chars.next(); // consume #
                    let content = extract_until(&mut chars, "#}")?;
                    let placeholder = create_placeholder(&mut counter, content, JinjaKind::Comment);
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
}

/// Extract content until we find the matching end tag {% endXXX %}
fn extract_block_content(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    end_tag: &str,
) -> Result<BlockContent> {
    let mut content = String::new();
    let end_pattern = format!("{{% {} %}}", end_tag);
    let end_pattern_ws = format!("{{% {} %}}", end_tag); // with extra space variants

    while let Some(c) = chars.next() {
        content.push(c);

        // Check if we've reached an end tag
        if c == '{' {
            if let Some(&next) = chars.peek() {
                if next == '%' {
                    chars.next();
                    content.push('%');

                    // Extract the statement content
                    let stmt_content = extract_until(chars, "%}")?;
                    let trimmed = stmt_content.trim();

                    if trimmed == end_tag {
                        // Found the end tag! Remove the partial "{% " we added and return
                        // We need to remove the "{%" from content
                        content.pop(); // remove '%'
                        content.pop(); // remove '{'
                        return Ok(BlockContent { content });
                    } else {
                        // Not our end tag, include it in content
                        content.push_str(&stmt_content);
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

fn create_placeholder(counter: &mut usize, content: String, kind: JinjaKind) -> JinjaPlaceholder {
    *counter += 1;
    let id = format!("{}{:03}__", PLACEHOLDER_PREFIX, counter);
    let original = match kind {
        JinjaKind::Expression => format!("{{{{{}}}}}", content),
        JinjaKind::Statement => format!("{{%{}%}}", content),
        JinjaKind::Comment => format!("{{#{}#}}", content),
    };
    JinjaPlaceholder { id, original, kind }
}
