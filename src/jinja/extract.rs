//! Jinja extraction from SQL
//!
//! Identifies and extracts Jinja expressions, statements, and comments,
//! replacing them with magic identifiers.

use super::{JinjaKind, JinjaPlaceholder, PLACEHOLDER_PREFIX};
use crate::Result;
use std::collections::HashMap;

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
                    let placeholder = create_placeholder(&mut counter, content, JinjaKind::Statement);
                    result.push_str(&placeholder.id);
                    placeholders.insert(placeholder.id.clone(), placeholder);
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
