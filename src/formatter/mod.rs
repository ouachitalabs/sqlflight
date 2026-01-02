//! SQL formatting / pretty-printing logic

pub mod printer;
pub mod rules;

use crate::ast::*;
use crate::error::Result;
use crate::jinja;
use crate::parser::lexer::{tokenize_with_comments, CommentToken, SpannedToken};
use crate::parser::expr::Parser;
use crate::parser::stmt;
use printer::{Printer, TARGET_WIDTH};
use rules::SELECT_COLUMN_THRESHOLD;

/// Tracks the byte position boundaries of each parsed statement
#[derive(Debug)]
struct StatementBoundary {
    /// Starting token index in the token stream
    start_token_idx: usize,
    /// Ending token index in the token stream (exclusive)
    end_token_idx: usize,
}

/// Format SQL string
pub fn format_sql(input: &str) -> Result<String> {
    // Step 1: Extract Jinja
    let (sql_with_placeholders, placeholders) = jinja::extract_jinja(input)?;

    // Step 2: Extract leading/trailing/inline Jinja placeholders
    let jinja_info = extract_statement_level_placeholders(&sql_with_placeholders);

    // Step 3: Tokenize with comments
    let tokenize_result = tokenize_with_comments(&jinja_info.body)?;
    let comments = tokenize_result.comments;

    // Step 4: Parse SQL
    // If there are no tokens (e.g., only Jinja), or only EOF token,
    // return the original with reintegrated Jinja
    let has_real_tokens = tokenize_result.tokens.iter().any(|t| !matches!(t, crate::parser::lexer::Token::Eof));
    if !has_real_tokens {
        return Ok(jinja::reintegrate_jinja(input, &placeholders));
    }

    // Parse ALL statements (not just the first one)
    // Track which statements had trailing semicolons and their boundaries
    let mut parser = Parser::new(&tokenize_result.tokens);
    let mut statements = Vec::new();
    let mut had_semicolons = Vec::new();
    let mut boundaries = Vec::new();

    while !parser.is_eof() {
        // Skip any leading semicolons
        while parser.check(&crate::parser::lexer::Token::Semicolon) {
            parser.advance();
        }

        if parser.is_eof() {
            break;
        }

        // Track the starting position before parsing
        let start_token_idx = parser.position();

        let ast = stmt::parse_statement(&mut parser)?;

        // Track the ending position after parsing (before semicolon)
        let end_token_idx = parser.position();

        boundaries.push(StatementBoundary {
            start_token_idx,
            end_token_idx,
        });

        statements.push(ast);

        // Consume optional trailing semicolon and track it
        let had_semi = parser.check(&crate::parser::lexer::Token::Semicolon);
        if had_semi {
            parser.advance();
        }
        had_semicolons.push(had_semi);
    }

    // Step 5: Map comments to statements using byte positions
    let comment_placements = map_comments_to_statements(
        &comments,
        &boundaries,
        &tokenize_result.spanned_tokens,
    );

    // Organize comments by type
    let mut leading_comments: Vec<&CommentToken> = Vec::new();
    let mut between_comments: Vec<Vec<&CommentToken>> = vec![Vec::new(); statements.len()];
    let mut inline_hints: Vec<Vec<&CommentToken>> = vec![Vec::new(); statements.len()];
    let mut trailing_comments: Vec<&CommentToken> = Vec::new();

    for (placement, comment) in &comment_placements {
        match placement {
            CommentPlacement::Leading => leading_comments.push(*comment),
            CommentPlacement::BetweenStatements(i) => {
                if *i < between_comments.len() {
                    between_comments[*i].push(*comment);
                }
            }
            CommentPlacement::InlineHint(i) => {
                if *i < inline_hints.len() {
                    inline_hints[*i].push(*comment);
                }
            }
            CommentPlacement::Trailing => trailing_comments.push(*comment),
        }
    }

    // Step 6: Format all ASTs with their associated comments
    let mut formatted_parts = Vec::new();
    for (i, ast) in statements.iter().enumerate() {
        let mut formatted = format_ast(ast)?;

        // Add inline hints right after SELECT/INSERT/UPDATE/DELETE/MERGE keywords
        // For now, we'll prepend them as a separate line (Phase 3 will improve this)
        let stmt_hints = &inline_hints[i];
        if !stmt_hints.is_empty() {
            // Insert hints into the formatted statement after the first keyword
            formatted = insert_hints_after_keyword(&formatted, stmt_hints);
        }

        // Add semicolon if the original had one and the statement doesn't already end with one
        if had_semicolons.get(i).copied().unwrap_or(false) {
            let trimmed = formatted.trim_end();
            if !trimmed.ends_with(';') {
                formatted = format!("{};\n", trimmed);
            }
        }

        // Add trailing comments that belong after this statement (between this and next)
        let stmt_between_comments = &between_comments[i];
        if !stmt_between_comments.is_empty() {
            if !formatted.ends_with('\n') {
                formatted.push('\n');
            }
            for comment in stmt_between_comments {
                formatted.push_str(&comment.text);
                formatted.push('\n');
            }
        }

        formatted_parts.push(formatted);
    }

    // Step 7: Reconstruct with leading/trailing/inline Jinja placeholders
    // We need to categorize inline placeholders by where they appear:
    // - Before first statement (leading)
    // - Between statement i and i+1
    // - After last statement (trailing)

    // Helper function to get original line number for a comment (tokenizer is 1-indexed)
    let get_original_line = |comment_line: usize| -> usize {
        let body_line = comment_line.saturating_sub(1);
        if body_line < jinja_info.body_to_original_line.len() {
            jinja_info.body_to_original_line[body_line]
        } else {
            let last_mapped = jinja_info.body_to_original_line.last().copied().unwrap_or(0);
            let diff = body_line.saturating_sub(jinja_info.body_to_original_line.len().saturating_sub(1));
            last_mapped + diff
        }
    };

    // Find the original line where each statement starts
    // We look at the first token of each statement and find its line in the body
    let mut statement_start_lines: Vec<usize> = Vec::new();
    for (_stmt_idx, boundary) in boundaries.iter().enumerate() {
        if boundary.start_token_idx < tokenize_result.spanned_tokens.len() {
            let span = &tokenize_result.spanned_tokens[boundary.start_token_idx].span;
            // Find which body line this byte position corresponds to
            let body_before = &jinja_info.body[..span.start.min(jinja_info.body.len())];
            let body_line = body_before.matches('\n').count(); // 0-indexed body line
            if body_line < jinja_info.body_to_original_line.len() {
                let original_line = jinja_info.body_to_original_line[body_line];
                statement_start_lines.push(original_line);
            } else {
                // Fallback: extrapolate
                let last = jinja_info.body_to_original_line.last().copied().unwrap_or(0);
                statement_start_lines.push(last + 1);
            }
        }
    }

    // Categorize inline placeholders by their position relative to statements
    let mut leading_inline: Vec<String> = Vec::new();
    let mut between_inline: Vec<Vec<String>> = vec![Vec::new(); statements.len()];
    let mut trailing_inline: Vec<String> = Vec::new();

    for (original_line, placeholder) in &jinja_info.inline_statements {
        let original_line = *original_line;
        if statement_start_lines.is_empty() {
            leading_inline.push(placeholder.clone());
        } else if original_line < statement_start_lines[0] {
            // Before first statement
            leading_inline.push(placeholder.clone());
        } else {
            // Find which gap it belongs to
            let mut placed = false;
            for i in 0..statement_start_lines.len() {
                let next_start = if i + 1 < statement_start_lines.len() {
                    statement_start_lines[i + 1]
                } else {
                    usize::MAX
                };
                if original_line < next_start {
                    // Belongs after statement i (in between_inline[i])
                    if i < between_inline.len() {
                        between_inline[i].push(placeholder.clone());
                    }
                    placed = true;
                    break;
                }
            }
            if !placed {
                trailing_inline.push(placeholder.clone());
            }
        }
    }

    // Build result with proper interleaving
    let mut result = String::new();

    // Add leading Jinja placeholders from extraction
    for placeholder in &jinja_info.leading {
        result.push_str(placeholder);
        result.push('\n');
    }

    // Collect leading comments and inline placeholders, sorted by original line
    #[derive(Debug)]
    enum LeadingItem {
        Comment(String),
        Placeholder(String),
    }
    let mut leading_items: Vec<(usize, LeadingItem)> = Vec::new();

    for comment in &leading_comments {
        let original_line = get_original_line(comment.line);
        leading_items.push((original_line, LeadingItem::Comment(comment.text.clone())));
    }
    for placeholder in &leading_inline {
        // Find original line for this placeholder
        for (orig_line, ph) in &jinja_info.inline_statements {
            if ph == placeholder {
                leading_items.push((*orig_line, LeadingItem::Placeholder(placeholder.clone())));
                break;
            }
        }
    }
    leading_items.sort_by_key(|(line, _)| *line);

    for (_, item) in leading_items {
        match item {
            LeadingItem::Comment(text) => {
                result.push_str(&text);
                result.push('\n');
            }
            LeadingItem::Placeholder(text) => {
                result.push_str(&text);
                result.push('\n');
            }
        }
    }

    // Add statements with their between-comments and between-inline-placeholders
    for (i, formatted) in formatted_parts.into_iter().enumerate() {
        if i > 0 && !result.ends_with("\n\n") {
            result.push('\n');
        }
        result.push_str(formatted.trim_end());
        result.push('\n');

        // Add inline placeholders that belong after this statement
        if i < between_inline.len() {
            for placeholder in &between_inline[i] {
                result.push_str(placeholder);
                result.push('\n');
            }
        }
    }

    // Add trailing SQL comments
    for comment in &trailing_comments {
        if !result.ends_with('\n') {
            result.push('\n');
        }
        result.push_str(&comment.text);
        result.push('\n');
    }

    // Add trailing inline placeholders
    for placeholder in &trailing_inline {
        if !result.ends_with('\n') {
            result.push('\n');
        }
        result.push_str(placeholder);
        result.push('\n');
    }

    // Add trailing placeholders
    for placeholder in &jinja_info.trailing {
        if !result.ends_with('\n') {
            result.push('\n');
        }
        result.push_str(placeholder);
    }

    // Step 8: Reintegrate Jinja
    let result = jinja::reintegrate_jinja(&result, &placeholders);

    Ok(result)
}

/// Information about Jinja placeholders at statement level
struct JinjaLineInfo {
    /// Placeholders before the SQL starts
    leading: Vec<String>,
    /// The SQL body to parse and format
    body: String,
    /// Placeholders after the SQL ends
    trailing: Vec<String>,
    /// Placeholders on their own lines within the SQL (with their ORIGINAL line positions)
    inline_statements: Vec<(usize, String)>,
    /// Maps body line numbers to original line numbers (for proper interleaving)
    body_to_original_line: Vec<usize>,
}

/// Extract Jinja placeholders that appear at statement level (before/after/within SQL)
fn extract_statement_level_placeholders(input: &str) -> JinjaLineInfo {
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
fn is_pure_placeholder_line(line: &str) -> bool {
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
fn is_statement_level_placeholder(lines: &[&str], placeholder_idx: usize) -> bool {
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

/// Check if a comment is an SQL hint (e.g., /*+ PARALLEL(8) */)
/// SQL hints must appear immediately after SELECT/INSERT/UPDATE/DELETE/MERGE keywords
fn is_sql_hint(comment_text: &str) -> bool {
    if !comment_text.starts_with("/*") {
        return false;
    }
    // Remove /* prefix and check if it starts with +
    let inner = comment_text.trim_start_matches("/*");
    inner.trim_start().starts_with('+')
}

/// Determines how a comment should be placed relative to statements
#[derive(Debug, Clone)]
enum CommentPlacement {
    /// Comment appears before all statements (leading comments)
    Leading,
    /// Comment is between statements i and i+1 (trailing comment for statement i)
    BetweenStatements(usize),
    /// Comment is an inline hint that should stay with statement i
    InlineHint(usize),
    /// Comment appears after all statements
    Trailing,
}

/// Maps comments to their placement relative to statements based on byte positions
fn map_comments_to_statements<'a>(
    comments: &'a [CommentToken],
    boundaries: &[StatementBoundary],
    spanned_tokens: &[SpannedToken],
) -> Vec<(CommentPlacement, &'a CommentToken)> {
    let mut placements = Vec::new();

    if boundaries.is_empty() {
        // No statements, all comments are leading/trailing
        for comment in comments {
            placements.push((CommentPlacement::Leading, comment));
        }
        return placements;
    }

    for comment in comments {
        let comment_byte_pos = comment.byte_start;

        // Find the first statement that starts after this comment
        let first_stmt_start_byte = if boundaries.is_empty() || spanned_tokens.is_empty() {
            usize::MAX
        } else if let Some(first_boundary) = boundaries.first() {
            if first_boundary.start_token_idx < spanned_tokens.len() {
                spanned_tokens[first_boundary.start_token_idx].span.start
            } else {
                usize::MAX
            }
        } else {
            usize::MAX
        };

        // If comment is before first statement, it's a leading comment
        if comment_byte_pos < first_stmt_start_byte {
            placements.push((CommentPlacement::Leading, comment));
            continue;
        }

        // Check if this is an inline hint within a statement
        if is_sql_hint(&comment.text) {
            // Find which statement contains this hint
            for (i, boundary) in boundaries.iter().enumerate() {
                if boundary.start_token_idx >= spanned_tokens.len() || boundary.end_token_idx == 0 {
                    continue;
                }

                let stmt_start = spanned_tokens[boundary.start_token_idx].span.start;
                // Use end_token_idx - 1 since end_token_idx is exclusive
                let stmt_end_idx = boundary.end_token_idx.saturating_sub(1).min(spanned_tokens.len() - 1);
                let stmt_end = spanned_tokens[stmt_end_idx].span.end;

                if comment_byte_pos >= stmt_start && comment_byte_pos <= stmt_end {
                    placements.push((CommentPlacement::InlineHint(i), comment));
                    break;
                }
            }
            // If no statement matched, treat as trailing
            if placements.last().map_or(true, |(_, c)| *c != comment) {
                placements.push((CommentPlacement::Trailing, comment));
            }
            continue;
        }

        // Find which statement(s) this comment is between
        let mut placed = false;
        for (i, boundary) in boundaries.iter().enumerate() {
            if boundary.end_token_idx == 0 || boundary.end_token_idx > spanned_tokens.len() {
                continue;
            }

            // Get the end position of the current statement
            let stmt_end_idx = boundary.end_token_idx.saturating_sub(1).min(spanned_tokens.len() - 1);
            let stmt_end = spanned_tokens[stmt_end_idx].span.end;

            // Get the start position of the next statement (if any)
            let next_stmt_start = if i + 1 < boundaries.len() {
                let next_boundary = &boundaries[i + 1];
                if next_boundary.start_token_idx < spanned_tokens.len() {
                    spanned_tokens[next_boundary.start_token_idx].span.start
                } else {
                    usize::MAX
                }
            } else {
                usize::MAX
            };

            // Comment is between this statement's end and next statement's start
            if comment_byte_pos >= stmt_end && comment_byte_pos < next_stmt_start {
                placements.push((CommentPlacement::BetweenStatements(i), comment));
                placed = true;
                break;
            }
        }

        if !placed {
            // Comment is after all statements
            placements.push((CommentPlacement::Trailing, comment));
        }
    }

    placements
}

/// Insert SQL hints after the first keyword (SELECT, INSERT, UPDATE, DELETE, MERGE)
/// The hint should appear immediately after the keyword on the same line
fn insert_hints_after_keyword(formatted: &str, hints: &[&CommentToken]) -> String {
    if hints.is_empty() {
        return formatted.to_string();
    }

    // Find the first keyword position
    let keywords = ["select", "insert", "update", "delete", "merge"];
    let lower = formatted.to_lowercase();

    for keyword in &keywords {
        if let Some(pos) = lower.find(keyword) {
            let keyword_end = pos + keyword.len();
            // Insert all hints after the keyword
            let mut result = String::new();
            result.push_str(&formatted[..keyword_end]);

            for hint in hints {
                result.push(' ');
                result.push_str(&hint.text);
            }

            result.push_str(&formatted[keyword_end..]);
            return result;
        }
    }

    // If no keyword found, just prepend the hints
    let mut result = String::new();
    for hint in hints {
        result.push_str(&hint.text);
        result.push('\n');
    }
    result.push_str(formatted);
    result
}

/// Format AST to string
pub fn format_ast(ast: &Statement) -> Result<String> {
    let mut formatter = Formatter::new();
    formatter.format_statement(ast);
    Ok(formatter.finish())
}

/// AST Formatter
struct Formatter {
    printer: Printer,
    in_subquery: bool,
}

impl Formatter {
    fn new() -> Self {
        Self {
            printer: Printer::new(),
            in_subquery: false,
        }
    }

    fn finish(self) -> String {
        let mut result = self.printer.finish();
        // Ensure trailing newline
        if !result.ends_with('\n') {
            result.push('\n');
        }
        result
    }

    fn format_statement(&mut self, stmt: &Statement) {
        match stmt {
            Statement::Select(s) => self.format_select(s),
            Statement::Insert(s) => self.format_insert(s),
            Statement::Update(s) => self.format_update(s),
            Statement::Delete(s) => self.format_delete(s),
            Statement::Merge(s) => self.format_merge(s),
            Statement::CreateTable(s) => self.format_create_table(s),
            Statement::CreateView(s) => self.format_create_view(s),
            Statement::AlterTable(s) => self.format_alter_table(s),
            Statement::DropTable(s) => self.format_drop_table(s),
            Statement::DropView(s) => self.format_drop_view(s),
        }
    }

    fn format_select(&mut self, stmt: &SelectStatement) {
        // WITH clause
        if let Some(with) = &stmt.with_clause {
            self.format_with_clause(with);
        }

        // SELECT [DISTINCT]
        self.printer.write("select");
        if stmt.distinct {
            self.printer.write(" distinct");
        }

        // Determine if we can keep a simple query all on one line
        // If any column is *, we break to multiline when there's a WHERE clause
        let has_star = stmt.columns.iter().any(|c| matches!(c.expr, Expression::Star | Expression::QualifiedStar(_)));
        let all_star = stmt.columns.iter().all(|c| matches!(c.expr, Expression::Star | Expression::QualifiedStar(_)));

        // Check if FROM has a subquery or sample clause
        let has_subquery_from = stmt.from.as_ref().map_or(false, |from| {
            matches!(from.table, TableReference::Subquery { .. })
        });

        // Check if FROM table has a SAMPLE clause
        let has_sample = stmt.from.as_ref().map_or(false, |from| {
            matches!(&from.table, TableReference::Table { sample: Some(_), .. })
        });

        // Check if FROM table is a TABLE(function) call
        let has_table_function = stmt.from.as_ref().map_or(false, |from| {
            matches!(&from.table, TableReference::TableFunction { table_wrapper: true, .. })
        });

        // Check if SELECT or table reference has PIVOT/UNPIVOT/MATCH_RECOGNIZE
        let has_pivot_unpivot = stmt.pivot.is_some() || stmt.unpivot.is_some()
            || stmt.from.as_ref().map_or(false, |from| {
                matches!(&from.table, TableReference::Table { pivot: Some(_), .. }
                    | TableReference::Table { unpivot: Some(_), .. }
                    | TableReference::Table { match_recognize: Some(_), .. })
            });

        // Check if FROM table has TIME_TRAVEL (AT/BEFORE clause)
        let has_time_travel = stmt.from.as_ref().map_or(false, |from| {
            matches!(&from.table, TableReference::Table { time_travel: Some(_), .. })
        });

        // Check if FROM table has CHANGES clause
        let has_changes = stmt.from.as_ref().map_or(false, |from| {
            matches!(&from.table, TableReference::Table { changes: Some(_), .. })
        });

        // Check if FROM is a VALUES clause
        let has_values = stmt.from.as_ref().map_or(false, |from| {
            matches!(&from.table, TableReference::Values(_))
        });

        // "Extra" clauses force column formatting decisions
        // Note: UNION doesn't force FROM to newline, each SELECT in UNION formats independently
        let has_extra_clauses = stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.qualify.is_some()
            || stmt.window.is_some()
            || stmt.order_by.is_some()
            || stmt.limit.is_some()
            || !stmt.joins.is_empty()
            || has_subquery_from;

        // Ordering clauses (ORDER BY, LIMIT) always force FROM to newline, even with select *
        // Filtering clauses (QUALIFY, HAVING) do NOT force FROM to newline with select *
        let has_ordering_clauses = stmt.order_by.is_some() || stmt.limit.is_some();

        // Estimate FROM clause width for column layout decision
        let from_width = stmt.from.as_ref().map_or(0, |from| {
            self.estimate_from_width(&from.table)
        });

        // Format columns and check if we should stay on same line for FROM
        // Force vertical if there are extra clauses
        // Only count non-comma joins as forcing vertical columns
        let has_regular_joins = stmt.joins.iter().any(|j| !j.is_comma_join);
        let columns_inline = self.format_select_columns(&stmt.columns, has_extra_clauses, from_width, has_regular_joins);

        let simple_query = columns_inline
            && !has_star  // No star columns when staying inline with WHERE
            && !has_extra_clauses
            && self.is_simple_where(&stmt.where_clause)
            && !self.in_subquery;  // Never simple when inside a subquery

        // FROM should be on new line if: vertical columns, has star with WHERE/JOINs,
        // has any joins, has sample/pivot/time_travel/changes/values, in subquery,
        // has ordering clauses (ORDER BY/LIMIT), FROM is subquery, or has non-filter extra clauses with non-star columns.
        // Special case: `select *` with QUALIFY keeps FROM inline (e.g., `select * from t qualify ...`)
        let force_from_newline = !columns_inline
            || (has_star && (stmt.where_clause.is_some() || !stmt.joins.is_empty()))
            || !stmt.joins.is_empty()  // Any join (including comma joins) forces FROM to newline
            || has_ordering_clauses  // ORDER BY/LIMIT always force FROM to newline
            || has_subquery_from  // Subquery FROM always forces newline
            || (has_extra_clauses && !all_star)  // Other extra clauses force newline unless select *
            || has_sample
            || has_table_function
            || has_pivot_unpivot
            || has_time_travel
            || has_changes
            || has_values
            || self.in_subquery;

        // FROM clause
        if let Some(from) = &stmt.from {
            if force_from_newline {
                self.printer.newline();
            } else {
                self.printer.write(" ");
            }
            self.format_from_clause(from);
        }

        // JOINs
        for join in &stmt.joins {
            self.printer.newline();
            self.format_join(join);
        }

        // PIVOT (SELECT-level, after JOINs)
        if let Some(pivot) = &stmt.pivot {
            self.printer.newline();
            self.format_pivot(pivot);
        }

        // UNPIVOT (SELECT-level, after JOINs)
        if let Some(unpivot) = &stmt.unpivot {
            self.printer.newline();
            self.format_unpivot(unpivot);
        }

        // WHERE clause
        if let Some(where_clause) = &stmt.where_clause {
            if simple_query {
                self.printer.write(" ");
                self.format_where_clause(where_clause);
            } else {
                self.printer.newline();
                self.format_where_clause(where_clause);
            }
        }

        // CONNECT BY (hierarchical query)
        if let Some(connect_by) = &stmt.connect_by {
            self.printer.newline();
            self.format_connect_by(connect_by);
        }

        // GROUP BY
        if let Some(group_by) = &stmt.group_by {
            self.printer.newline();
            self.format_group_by(group_by);
        }

        // HAVING
        if let Some(having) = &stmt.having {
            self.printer.newline();
            self.format_having(having);
        }

        // QUALIFY
        if let Some(qualify) = &stmt.qualify {
            self.printer.newline();
            self.format_qualify(qualify);
        }

        // WINDOW
        if let Some(window) = &stmt.window {
            self.printer.newline();
            self.format_window_clause(window);
        }

        // ORDER BY
        if let Some(order_by) = &stmt.order_by {
            self.printer.newline();
            self.format_order_by(order_by);
        }

        // LIMIT
        if let Some(limit) = &stmt.limit {
            self.printer.newline();
            self.format_limit(limit);
        }

        // UNION/INTERSECT/EXCEPT
        if let Some(set_op) = &stmt.union {
            self.printer.newline();
            self.format_set_operation(set_op);
        }

        // Trailing semicolon
        if stmt.semicolon {
            self.printer.write(";");
        }
    }

    /// Check if a WHERE clause is simple (no AND/OR, can be kept on same line)
    fn is_simple_where(&self, where_clause: &Option<WhereClause>) -> bool {
        match where_clause {
            None => true,
            Some(wc) => !self.has_and_or(&wc.condition),
        }
    }

    /// Check if expression contains AND/OR at top level
    fn has_and_or(&self, expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::BinaryOp { op: BinaryOperator::And | BinaryOperator::Or, .. }
        )
    }

    fn format_with_clause(&mut self, with: &WithClause) {
        self.printer.write("with ");
        if with.recursive {
            self.printer.write("recursive ");
        }

        for (i, cte) in with.ctes.iter().enumerate() {
            if i > 0 {
                self.printer.newline();
                self.printer.write(", ");
            }
            self.format_cte(cte);
        }
        self.printer.newline();
    }

    fn format_cte(&mut self, cte: &CommonTableExpression) {
        // Check if this is a Jinja placeholder CTE (for-loop that generates CTEs)
        // These are identified by having a Jinja placeholder name and a JinjaBlock query
        if cte.name.starts_with("__SQLFLIGHT_JINJA_") {
            if let Some(col) = cte.query.columns.first() {
                if let Expression::JinjaBlock(block) = &col.expr {
                    // Just output the Jinja block placeholder - it will be reintegrated later
                    self.printer.write(block);
                    return;
                }
            }
        }

        self.printer.write(&format_identifier(&cte.name));

        if let Some(columns) = &cte.columns {
            self.printer.write(" (");
            self.printer.write(&columns.iter()
                .map(|c| format_identifier(c))
                .collect::<Vec<_>>()
                .join(", "));
            self.printer.write(")");
        }

        self.printer.write(" as (");
        self.printer.indent();
        self.printer.newline();
        let was_in_subquery = self.in_subquery;
        self.in_subquery = true;
        self.format_select(&cte.query);
        self.in_subquery = was_in_subquery;
        self.printer.dedent();
        self.printer.newline();
        self.printer.write(")");
    }

    /// Format SELECT columns. Returns true if formatted inline, false if vertical.
    fn format_select_columns(&mut self, columns: &[SelectColumn], has_extra_clauses: bool, from_width: usize, has_joins: bool) -> bool {
        // Estimate total width for inline decision
        let total_width: usize = columns.iter()
            .map(|c| self.estimate_column_width(c))
            .sum::<usize>() + (columns.len().saturating_sub(1)) * 2; // account for ", "

        // Check if any column contains a complex expression that forces vertical
        let has_complex_expr = columns.iter().any(|c| self.is_complex_expression(&c.expr));

        // Check if all columns are star expressions (SELECT * or SELECT t.*)
        let all_star = columns.iter().all(|c| matches!(c.expr, Expression::Star | Expression::QualifiedStar(_)));

        // Calculate full line width: "select " + columns + " from table"
        let full_line_width = 7 + total_width + from_width; // 7 = "select "

        // Force vertical based primarily on full line width exceeding target,
        // but also consider extra clauses for longer queries
        // When there are extra clauses (GROUP BY, WINDOW, etc.), use appropriate thresholds:
        // - With 3+ columns: use tighter threshold (90) for readability
        // - With 2 columns: use normal threshold (120)
        let width_threshold = if has_extra_clauses && columns.len() >= 3 {
            90
        } else {
            TARGET_WIDTH
        };
        let exceeds_width = full_line_width > width_threshold;
        // Regular JOINs (not comma joins) force vertical columns when there are multiple columns
        // Comma joins don't force vertical if columns are simple
        let joins_force_vertical = has_joins && !all_star && columns.len() > 1;
        let force_vertical = joins_force_vertical
            || exceeds_width
            || (has_extra_clauses && !all_star && columns.len() > SELECT_COLUMN_THRESHOLD);

        let should_inline = !force_vertical
            && columns.len() <= SELECT_COLUMN_THRESHOLD
            && !has_complex_expr;

        if should_inline && !columns.is_empty() {
            self.printer.write(" ");
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    self.printer.write(", ");
                }
                self.format_select_column(col);
            }
            true
        } else {
            self.printer.indent();
            self.printer.newline();
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    self.printer.newline();
                    self.printer.write(", ");
                }
                self.format_select_column(col);
            }
            self.printer.dedent();
            false
        }
    }

    fn estimate_column_width(&self, col: &SelectColumn) -> usize {
        let expr_width = self.estimate_expr_width(&col.expr);
        let alias_width = col.alias.as_ref().map_or(0, |a| a.len() + 4); // " as alias"
        expr_width + alias_width
    }

    fn estimate_expr_width(&self, expr: &Expression) -> usize {
        match expr {
            Expression::Literal(lit) => self.estimate_literal_width(lit),
            Expression::Identifier(name) => name.len(),
            Expression::QualifiedIdentifier(parts) => parts.iter().map(|p| p.len()).sum::<usize>() + parts.len() - 1,
            Expression::Star => 1,
            Expression::QualifiedStar(name) => name.len() + 2,
            Expression::BinaryOp { left, right, .. } => {
                self.estimate_expr_width(left) + self.estimate_expr_width(right) + 3
            }
            Expression::FunctionCall { name, args, within_group, over } => {
                let base = name.len() + 2 + args.iter().map(|a| self.estimate_expr_width(a) + 2).sum::<usize>();
                let within_group_width = if within_group.is_some() { 30 } else { 0 };  // " within group (order by ...)"
                let over_width = if let Some(spec) = over {
                    if spec.window_name.is_some() {
                        // " over window_name" - roughly 15 chars
                        15
                    } else {
                        // " over (...)" - estimate based on content
                        self.estimate_window_spec_width(spec)
                    }
                } else {
                    0
                };
                base + within_group_width + over_width
            }
            _ => 20, // Default estimate for complex expressions
        }
    }

    fn estimate_literal_width(&self, lit: &Literal) -> usize {
        match lit {
            Literal::Null => 4,
            Literal::Boolean(b) => if *b { 4 } else { 5 },
            Literal::Integer(n) => format!("{}", n).len(),
            Literal::Float(n) => format!("{}", n).len(),
            Literal::String(s) => s.len() + 2,
            Literal::Date(s) | Literal::Timestamp(s) => s.len() + 7,
        }
    }

    fn estimate_from_width(&self, table: &TableReference) -> usize {
        // " from tablename" = 6 + table name length
        let table_width = match table {
            TableReference::Table { name, alias, .. } => {
                name.len() + alias.as_ref().map_or(0, |a| a.len() + 1)
            }
            TableReference::Subquery { .. } => 50, // Subqueries force multiline anyway
            TableReference::JinjaRef(name) => name.len(),
            TableReference::Flatten(_) => 20,
            TableReference::Lateral(_) => 30,
            TableReference::TableFunction { name, .. } => name.len() + 20,
            TableReference::Values(_) => 30,
            TableReference::Stage { name, path, alias } => {
                1 + name.len() + path.as_ref().map_or(0, |p| p.len()) + alias.as_ref().map_or(0, |a| a.len() + 1)
            }
        };
        6 + table_width // " from " = 6
    }

    fn estimate_window_spec_width(&self, spec: &WindowSpec) -> usize {
        let mut width = 8; // " over ()"

        if let Some(ref partition_by) = spec.partition_by {
            width += 13; // "partition by "
            width += partition_by.iter().map(|e| self.estimate_expr_width(e) + 2).sum::<usize>();
        }

        if let Some(ref order_by) = spec.order_by {
            width += 10; // "order by "
            width += order_by.iter().map(|item| {
                let expr_width = self.estimate_expr_width(&item.expr);
                let dir_width = item.direction.as_ref().map_or(0, |_| 5); // " asc" or " desc"
                expr_width + dir_width + 2
            }).sum::<usize>();
        }

        if spec.frame.is_some() {
            width += 40; // Rough estimate for frame clause
        }

        width
    }

    /// Check if an expression is complex enough to force vertical layout
    fn is_complex_expression(&self, expr: &Expression) -> bool {
        match expr {
            Expression::Case(_) | Expression::Subquery(_) => true,
            Expression::Parenthesized(inner) => self.is_complex_expression(inner),
            _ => false,
        }
    }

    fn format_select_column(&mut self, col: &SelectColumn) {
        self.format_expression(&col.expr);
        if let Some(alias) = &col.alias {
            if col.explicit_as {
                self.printer.write(" as ");
            } else {
                self.printer.write(" ");
            }
            self.printer.write(&format_identifier(alias));
        }
    }

    fn format_from_clause(&mut self, from: &FromClause) {
        self.printer.write("from ");
        self.format_table_reference(&from.table);
    }

    fn format_table_reference(&mut self, table: &TableReference) {
        match table {
            TableReference::Table { name, alias, time_travel, changes, sample, pivot, unpivot, match_recognize, .. } => {
                self.printer.write(&format_identifier(name));
                // Format CHANGES clause (includes its own AT/BEFORE)
                if let Some(ch) = changes {
                    self.printer.write(" ");
                    self.format_changes(ch);
                }
                // Format TIME_TRAVEL clause (AT/BEFORE) - only if no CHANGES
                if let Some(tt) = time_travel {
                    self.printer.write(" ");
                    self.format_time_travel(tt);
                }
                // Note: alias is printed after PIVOT/UNPIVOT/MATCH_RECOGNIZE if they exist
                let has_special_clause = pivot.is_some() || unpivot.is_some() || match_recognize.is_some();
                if !has_special_clause {
                    if let Some(a) = alias {
                        self.printer.write(" ");
                        self.printer.write(&format_identifier(a));
                    }
                }
                // Format SAMPLE clause
                if let Some(sample) = sample {
                    self.printer.newline();
                    self.format_sample(sample);
                }
                // Format PIVOT clause
                if let Some(pivot) = pivot {
                    self.printer.newline();
                    self.format_pivot(pivot);
                }
                // Format UNPIVOT clause
                if let Some(unpivot) = unpivot {
                    self.printer.newline();
                    self.format_unpivot(unpivot);
                }
                // Format MATCH_RECOGNIZE clause
                if let Some(mr) = match_recognize {
                    self.printer.newline();
                    self.format_match_recognize(mr);
                }
                // Alias after PIVOT/UNPIVOT/MATCH_RECOGNIZE
                if has_special_clause {
                    if let Some(a) = alias {
                        self.printer.write(" ");
                        self.printer.write(&format_identifier(a));
                    }
                }
            }
            TableReference::Subquery { query, alias, explicit_as } => {
                self.printer.write("(");
                self.printer.indent();
                self.printer.newline();
                let was_in_subquery = self.in_subquery;
                self.in_subquery = true;
                self.format_select(query);
                self.in_subquery = was_in_subquery;
                self.printer.dedent();
                self.printer.newline();
                self.printer.write(")");
                if let Some(a) = alias {
                    if *explicit_as {
                        self.printer.write(" as ");
                    } else {
                        self.printer.write(" ");
                    }
                    self.printer.write(&format_identifier(a));
                }
            }
            TableReference::JinjaRef(name) => {
                self.printer.write(name);
            }
            TableReference::Flatten(flatten) => {
                self.format_flatten(flatten);
            }
            TableReference::Lateral(inner) => {
                self.printer.write("lateral ");
                self.format_table_reference(inner);
            }
            TableReference::TableFunction { name, args, table_wrapper, alias } => {
                if *table_wrapper {
                    self.printer.write("table(");
                }
                self.printer.write(&format_identifier(name));
                self.printer.write("(");
                for (i, (param_name, expr)) in args.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    if let Some(pname) = param_name {
                        self.printer.write(&format_identifier(pname));
                        self.printer.write(" => ");
                    }
                    self.format_expression(expr);
                }
                self.printer.write(")");
                if *table_wrapper {
                    self.printer.write(")");
                }
                if let Some(a) = alias {
                    self.printer.write(" ");
                    self.printer.write(&format_identifier(a));
                }
            }
            TableReference::Values(values) => {
                self.format_values(values);
            }
            TableReference::Stage { name, path, alias } => {
                self.printer.write("@");
                self.printer.write(name);
                if let Some(p) = path {
                    self.printer.write(p);
                }
                if let Some(a) = alias {
                    self.printer.write(" ");
                    self.printer.write(&format_identifier(a));
                }
            }
        }
    }

    fn format_flatten(&mut self, flatten: &FlattenClause) {
        self.printer.write("flatten(");
        self.printer.write("input => ");
        self.format_expression(&flatten.input);
        if let Some(path) = &flatten.path {
            self.printer.write(", path => '");
            self.printer.write(path);
            self.printer.write("'");
        }
        if flatten.outer {
            self.printer.write(", outer => true");
        }
        if flatten.recursive {
            self.printer.write(", recursive => true");
        }
        if let Some(mode) = &flatten.mode {
            self.printer.write(", mode => '");
            self.printer.write(mode);
            self.printer.write("'");
        }
        self.printer.write(")");
    }

    fn format_sample(&mut self, sample: &SampleClause) {
        // Use SAMPLE or TABLESAMPLE based on original input
        if sample.tablesample {
            self.printer.write("tablesample");
        } else {
            self.printer.write("sample");
        }
        match sample.method {
            SampleMethod::Default => {}
            SampleMethod::Bernoulli => self.printer.write(" bernoulli"),
            SampleMethod::System => self.printer.write(" system"),
            SampleMethod::Block => self.printer.write(" block"),
        }
        self.printer.write(" (");
        // Format size
        match &sample.size {
            SampleSize::Percent(p) => {
                // Format without trailing zeros
                if p.fract() == 0.0 {
                    self.printer.write(&format!("{}", *p as i64));
                } else {
                    self.printer.write(&format!("{}", p));
                }
            }
            SampleSize::Rows(n) => {
                self.printer.write(&format!("{} rows", n));
            }
        }
        self.printer.write(")");
        // Format seed if present
        if let Some(seed) = sample.seed {
            self.printer.write(&format!(" seed ({})", seed));
        }
    }

    fn format_pivot(&mut self, pivot: &PivotClause) {
        self.printer.write("pivot (");
        self.printer.indent();
        self.printer.newline();

        // Format aggregate function(s) with leading comma style for multiple
        for (i, (agg_expr, alias)) in pivot.aggregate_functions.iter().enumerate() {
            if i > 0 {
                self.printer.newline();
                self.printer.write(", ");
            }
            self.format_expression(agg_expr);
            if let Some(a) = alias {
                self.printer.write(" as ");
                self.printer.write(&format_identifier(a));
            }
        }

        self.printer.newline();
        self.printer.write("for ");
        self.printer.write(&format_identifier(&pivot.for_column));
        self.printer.write(" in (");

        // Format IN values
        for (i, (value_expr, alias)) in pivot.in_values.iter().enumerate() {
            if i > 0 {
                self.printer.write(", ");
            }
            self.format_expression(value_expr);
            if let Some(a) = alias {
                self.printer.write(" as ");
                self.printer.write(&format_identifier(a));
            }
        }
        self.printer.write(")");

        self.printer.dedent();
        self.printer.newline();
        self.printer.write(")");

        // Output alias if present
        if let Some(alias) = &pivot.alias {
            if alias.explicit_as {
                self.printer.write(" as ");
            } else {
                self.printer.write(" ");
            }
            self.printer.write(&format_identifier(&alias.name));
            if let Some(cols) = &alias.column_aliases {
                self.printer.write(" (");
                for (i, col) in cols.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    self.printer.write(&format_identifier(col));
                }
                self.printer.write(")");
            }
        }
    }

    fn format_unpivot(&mut self, unpivot: &UnpivotClause) {
        self.printer.write("unpivot");
        if unpivot.include_nulls {
            self.printer.write(" include nulls");
        }
        self.printer.write(" (");
        self.printer.indent();
        self.printer.newline();

        // Format value column
        self.printer.write(&format_identifier(&unpivot.value_column));

        self.printer.newline();
        self.printer.write("for ");
        self.printer.write(&format_identifier(&unpivot.name_column));
        self.printer.write(" in (");

        // Format columns
        for (i, col) in unpivot.columns.iter().enumerate() {
            if i > 0 {
                self.printer.write(", ");
            }
            self.printer.write(&format_identifier(col));
        }
        self.printer.write(")");

        self.printer.dedent();
        self.printer.newline();
        self.printer.write(")");

        // Output alias if present
        if let Some(a) = &unpivot.alias {
            self.printer.write(" ");
            self.printer.write(&format_identifier(a));
        }
    }

    fn format_match_recognize(&mut self, mr: &MatchRecognizeClause) {
        self.printer.write("match_recognize (");
        self.printer.indent();
        self.printer.newline();

        // PARTITION BY
        if let Some(partition_by) = &mr.partition_by {
            self.printer.write("partition by ");
            for (i, expr) in partition_by.iter().enumerate() {
                if i > 0 {
                    self.printer.write(", ");
                }
                self.format_expression(expr);
            }
            self.printer.newline();
        }

        // ORDER BY
        if let Some(order_by) = &mr.order_by {
            self.printer.write("order by ");
            for (i, item) in order_by.iter().enumerate() {
                if i > 0 {
                    self.printer.write(", ");
                }
                self.format_order_by_item(item);
            }
            self.printer.newline();
        }

        // MEASURES (with leading comma style for multiple)
        if !mr.measures.is_empty() {
            self.printer.write("measures");
            self.printer.indent();
            self.printer.newline();
            for (i, (expr, name)) in mr.measures.iter().enumerate() {
                if i > 0 {
                    self.printer.newline();
                    self.printer.write(", ");
                }
                self.format_expression(expr);
                self.printer.write(" as ");
                self.printer.write(&name.to_lowercase());
            }
            self.printer.dedent();
            self.printer.newline();
        }

        // ROWS PER MATCH
        if let Some(rows_per_match) = &mr.rows_per_match {
            match rows_per_match {
                RowsPerMatch::OneRow => self.printer.write("one row per match"),
                RowsPerMatch::AllRows => self.printer.write("all rows per match"),
            }
            self.printer.newline();
        }

        // AFTER MATCH SKIP
        if let Some(skip) = &mr.after_match_skip {
            match skip {
                AfterMatchSkip::PastLastRow => self.printer.write("after match skip past last row"),
                AfterMatchSkip::ToNextRow => self.printer.write("after match skip to next row"),
                AfterMatchSkip::ToFirst(name) => {
                    self.printer.write("after match skip to first ");
                    self.printer.write(&name.to_lowercase());
                }
                AfterMatchSkip::ToLast(name) => {
                    self.printer.write("after match skip to last ");
                    self.printer.write(&name.to_lowercase());
                }
            }
            self.printer.newline();
        }

        // PATTERN
        self.printer.write("pattern (");
        self.printer.write(&mr.pattern.to_lowercase());
        self.printer.write(")");
        self.printer.newline();

        // DEFINE (with leading comma style for multiple)
        self.printer.write("define");
        self.printer.indent();
        self.printer.newline();
        for (i, (name, expr)) in mr.define.iter().enumerate() {
            if i > 0 {
                self.printer.newline();
                self.printer.write(", ");
            }
            self.printer.write(&name.to_lowercase());
            self.printer.write(" as ");
            self.format_expression(expr);
        }
        self.printer.dedent();

        self.printer.dedent();
        self.printer.newline();
        self.printer.write(")");
    }

    fn format_time_travel(&mut self, tt: &TimeTravelClause) {
        match tt {
            TimeTravelClause::At(point) => {
                self.printer.write("at (");
                self.format_time_travel_point(point);
                self.printer.write(")");
            }
            TimeTravelClause::Before(point) => {
                self.printer.write("before (");
                self.format_time_travel_point(point);
                self.printer.write(")");
            }
        }
    }

    fn format_time_travel_point(&mut self, point: &TimeTravelPoint) {
        match point {
            TimeTravelPoint::Timestamp(expr) => {
                self.printer.write("timestamp => ");
                self.format_expression(expr);
            }
            TimeTravelPoint::Offset(expr) => {
                self.printer.write("offset => ");
                self.format_expression(expr);
            }
            TimeTravelPoint::Statement(id) => {
                self.printer.write("statement => '");
                self.printer.write(id);
                self.printer.write("'");
            }
        }
    }

    fn format_changes(&mut self, changes: &ChangesClause) {
        self.printer.write("changes (information => ");
        match &changes.information {
            ChangesInformation::Default => self.printer.write("default"),
            ChangesInformation::AppendOnly => self.printer.write("append_only"),
        }
        self.printer.write(") ");
        self.format_time_travel(&changes.at_or_before);

        // Format optional END clause
        if let Some(end_point) = &changes.end_point {
            self.printer.write(" end (");
            self.format_time_travel_point(end_point);
            self.printer.write(")");
        }
    }

    fn format_values(&mut self, values: &ValuesClause) {
        self.printer.write("values");
        self.printer.indent();
        for (i, row) in values.rows.iter().enumerate() {
            self.printer.newline();
            if i > 0 {
                self.printer.write(", ");
            }
            self.printer.write("(");
            for (j, expr) in row.iter().enumerate() {
                if j > 0 {
                    self.printer.write(", ");
                }
                self.format_expression(expr);
            }
            self.printer.write(")");
        }
        self.printer.dedent();
        self.printer.newline();
        if let Some(alias) = &values.alias {
            self.printer.write("as ");
            self.printer.write(&format_identifier(&alias.table_alias));
            if !alias.column_aliases.is_empty() {
                self.printer.write("(");
                self.printer.write(&alias.column_aliases.iter()
                    .map(|c| format_identifier(c))
                    .collect::<Vec<_>>()
                    .join(", "));
                self.printer.write(")");
            }
        }
    }

    fn format_join(&mut self, join: &JoinClause) {
        // Comma joins use comma separator instead of JOIN keyword
        if join.is_comma_join {
            self.printer.write(", ");
            self.format_table_reference(&join.table);
            return;
        }

        let join_keyword = match join.join_type {
            JoinType::Inner => {
                if join.explicit_inner {
                    "inner join"
                } else {
                    "join"
                }
            }
            JoinType::Left => "left join",
            JoinType::Right => "right join",
            JoinType::Full => "full outer join",
            JoinType::Cross => "cross join",
        };
        self.printer.write(join_keyword);
        self.printer.write(" ");
        self.format_table_reference(&join.table);

        if let Some(condition) = &join.condition {
            self.printer.indent();
            self.printer.newline();
            self.printer.write("on ");
            self.format_expression_with_leading_operators(condition);
            self.printer.dedent();
        }
    }

    fn format_where_clause(&mut self, where_clause: &WhereClause) {
        self.printer.write("where ");
        // Indent the condition so AND/OR operators are at 2 spaces from left
        self.printer.indent();
        self.format_expression_with_leading_operators(&where_clause.condition);
        self.printer.dedent();
    }

    fn format_expression_with_leading_operators(&mut self, expr: &Expression) {
        // Flatten the AND/OR chain and format with consistent indentation
        let mut parts = Vec::new();
        self.collect_and_or_parts(expr, &mut parts);

        if parts.len() <= 1 {
            // Simple expression, no AND/OR
            self.format_expression(expr);
        } else {
            // First part without leading operator
            self.format_expression(&parts[0].1);
            // Remaining parts with leading operators - use current indent level
            for (op, part_expr) in parts.iter().skip(1) {
                self.printer.newline();
                self.printer.write(op);
                self.printer.write(" ");
                self.format_expression(part_expr);
            }
        }
    }

    /// Collect all parts of an AND/OR chain with their operators
    fn collect_and_or_parts<'a>(&self, expr: &'a Expression, parts: &mut Vec<(&'static str, &'a Expression)>) {
        match expr {
            Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
                self.collect_and_or_parts(left, parts);
                // Add right side with AND operator
                match right.as_ref() {
                    Expression::BinaryOp { op: BinaryOperator::And | BinaryOperator::Or, .. } => {
                        self.collect_and_or_parts(right, parts);
                    }
                    _ => parts.push(("and", right)),
                }
            }
            Expression::BinaryOp { left, op: BinaryOperator::Or, right } => {
                self.collect_and_or_parts(left, parts);
                // Add right side with OR operator
                match right.as_ref() {
                    Expression::BinaryOp { op: BinaryOperator::And | BinaryOperator::Or, .. } => {
                        self.collect_and_or_parts(right, parts);
                    }
                    _ => parts.push(("or", right)),
                }
            }
            _ => {
                if parts.is_empty() {
                    parts.push(("", expr));
                } else {
                    // This shouldn't happen if we're correctly walking the tree
                    parts.push(("", expr));
                }
            }
        }
    }

    fn format_connect_by(&mut self, connect_by: &ConnectByClause) {
        // START WITH comes first if present
        if let Some(start_with) = &connect_by.start_with {
            self.printer.write("start with ");
            self.format_expression(start_with);
            self.printer.newline();
        }

        // CONNECT BY
        self.printer.write("connect by");
        if connect_by.nocycle {
            self.printer.write(" nocycle");
        }
        self.printer.write(" ");
        self.format_expression(&connect_by.connect_by);

        // ORDER SIBLINGS BY
        if let Some(order_siblings_by) = &connect_by.order_siblings_by {
            self.printer.newline();
            self.printer.write("order siblings by ");
            for (i, item) in order_siblings_by.iter().enumerate() {
                if i > 0 {
                    self.printer.write(", ");
                }
                self.format_expression(&item.expr);
                if let Some(dir) = &item.direction {
                    self.printer.write(match dir {
                        SortDirection::Asc => " asc",
                        SortDirection::Desc => " desc",
                    });
                }
            }
        }
    }

    fn format_group_by(&mut self, group_by: &GroupByClause) {
        self.printer.write("group by");
        let exprs = &group_by.expressions;
        if exprs.len() <= 1 {
            // Single expression: inline
            self.printer.write(" ");
            if let Some(expr) = exprs.first() {
                self.format_expression(expr);
            }
        } else {
            // Multiple expressions: vertical with leading commas
            self.printer.indent();
            for (i, expr) in exprs.iter().enumerate() {
                self.printer.newline();
                if i > 0 {
                    self.printer.write(", ");
                }
                self.format_expression(expr);
            }
            self.printer.dedent();
        }
    }

    fn format_having(&mut self, having: &HavingClause) {
        self.printer.write("having ");
        self.format_expression(&having.condition);
    }

    fn format_qualify(&mut self, qualify: &QualifyClause) {
        self.printer.write("qualify ");
        self.format_expression(&qualify.condition);
    }

    fn format_window_clause(&mut self, window: &WindowClause) {
        self.printer.write("window ");
        for (i, def) in window.definitions.iter().enumerate() {
            if i > 0 {
                self.printer.write(", ");
            }
            self.printer.write(&format_identifier(&def.name));
            self.printer.write(" as (");
            self.format_window_spec(&def.spec);
            self.printer.write(")");
        }
    }

    fn format_order_by(&mut self, order_by: &OrderByClause) {
        self.printer.write("order by");
        let items = &order_by.items;
        if items.len() <= 1 {
            // Single item: inline
            self.printer.write(" ");
            if let Some(item) = items.first() {
                self.format_order_by_item(item);
            }
        } else {
            // Multiple items: vertical with leading commas
            self.printer.indent();
            for (i, item) in items.iter().enumerate() {
                self.printer.newline();
                if i > 0 {
                    self.printer.write(", ");
                }
                self.format_order_by_item(item);
            }
            self.printer.dedent();
        }
    }

    fn format_order_by_item(&mut self, item: &OrderByItem) {
        self.format_expression(&item.expr);
        if let Some(dir) = &item.direction {
            match dir {
                SortDirection::Asc => self.printer.write(" asc"),
                SortDirection::Desc => self.printer.write(" desc"),
            }
        }
        if let Some(nulls) = &item.nulls {
            match nulls {
                NullsOrder::First => self.printer.write(" nulls first"),
                NullsOrder::Last => self.printer.write(" nulls last"),
            }
        }
    }

    fn format_limit(&mut self, limit: &LimitClause) {
        self.printer.write("limit ");
        self.format_expression(&limit.count);
        if let Some(offset) = &limit.offset {
            self.printer.write(" offset ");
            self.format_expression(offset);
        }
    }

    fn format_set_operation(&mut self, set_op: &SetOperation) {
        let op_keyword = match set_op.op_type {
            SetOperationType::Union => "union",
            SetOperationType::Intersect => "intersect",
            SetOperationType::Except => "except",
            SetOperationType::Minus => "minus",
        };
        self.printer.write(op_keyword);
        if set_op.all {
            self.printer.write(" all");
        }
        self.printer.newline();
        self.format_select(&set_op.query);
    }

    fn format_expression(&mut self, expr: &Expression) {
        match expr {
            Expression::Literal(lit) => self.format_literal(lit),
            Expression::Identifier(name) => {
                // Preserve Jinja placeholders exactly as-is
                if is_jinja_placeholder(name) {
                    self.printer.write(name);
                } else {
                    self.printer.write(&name.to_lowercase());
                }
            }
            Expression::QualifiedIdentifier(parts) => {
                let formatted: Vec<String> = parts.iter()
                    .map(|p| {
                        // Preserve Jinja placeholders exactly as-is
                        if is_jinja_placeholder(p) {
                            p.clone()
                        } else {
                            p.to_lowercase()
                        }
                    })
                    .collect();
                self.printer.write(&formatted.join("."));
            }
            Expression::Star => self.printer.write("*"),
            Expression::QualifiedStar(name) => {
                self.printer.write(&format_identifier(name));
                self.printer.write(".*");
            }
            Expression::BinaryOp { left, op, right } => {
                self.format_expression(left);
                self.printer.write(" ");
                self.format_binary_operator(op);
                self.printer.write(" ");
                self.format_expression(right);
            }
            Expression::UnaryOp { op, expr } => {
                self.format_unary_operator(op);
                if matches!(op, UnaryOperator::Not) {
                    self.printer.write(" ");
                }
                self.format_expression(expr);
            }
            Expression::FunctionCall { name, args, within_group, over } => {
                self.printer.write(&format_identifier(name));
                self.printer.write("(");
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    self.format_expression(arg);
                }
                self.printer.write(")");
                // WITHIN GROUP (ORDER BY ...) for ordered-set aggregates
                if let Some(order_items) = within_group {
                    self.printer.write(" within group (order by ");
                    for (i, item) in order_items.iter().enumerate() {
                        if i > 0 {
                            self.printer.write(", ");
                        }
                        self.format_order_by_item(item);
                    }
                    self.printer.write(")");
                }
                if let Some(window_spec) = over {
                    if let Some(ref window_name) = window_spec.window_name {
                        // Reference to a named window from WINDOW clause
                        self.printer.write(" over ");
                        self.printer.write(window_name);
                    } else {
                        // Inline window specification
                        self.printer.write(" over (");
                        self.format_window_spec(window_spec);
                        self.printer.write(")");
                    }
                }
            }
            Expression::Case(case) => self.format_case(case),
            Expression::Subquery(query) => {
                self.printer.write("(");
                self.printer.indent();
                self.printer.newline();
                let was_in_subquery = self.in_subquery;
                self.in_subquery = true;
                self.format_select(query);
                self.in_subquery = was_in_subquery;
                self.printer.dedent();
                self.printer.newline();
                self.printer.write(")");
            }
            Expression::InList { expr, list, negated } => {
                self.format_expression(expr);
                if *negated {
                    self.printer.write(" not");
                }
                self.printer.write(" in (");
                for (i, item) in list.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    self.format_expression(item);
                }
                self.printer.write(")");
            }
            Expression::InSubquery { expr, subquery, negated } => {
                self.format_expression(expr);
                if *negated {
                    self.printer.write(" not");
                }
                self.printer.write(" in (");
                // Save current indent and reset to 1 level for subquery
                let saved_indent = self.printer.reset_indent();
                self.printer.indent();
                self.printer.newline();
                let was_in_subquery = self.in_subquery;
                self.in_subquery = true;
                self.format_select(subquery);
                self.in_subquery = was_in_subquery;
                self.printer.dedent();
                self.printer.newline();
                self.printer.write(")");
                // Restore original indent
                self.printer.restore_indent(saved_indent);
            }
            Expression::Between { expr, low, high, negated } => {
                self.format_expression(expr);
                if *negated {
                    self.printer.write(" not");
                }
                self.printer.write(" between ");
                self.format_expression(low);
                self.printer.write(" and ");
                self.format_expression(high);
            }
            Expression::IsNull { expr, negated } => {
                self.format_expression(expr);
                if *negated {
                    self.printer.write(" is not null");
                } else {
                    self.printer.write(" is null");
                }
            }
            Expression::IsTrue { expr, negated } => {
                self.format_expression(expr);
                if *negated {
                    self.printer.write(" is not true");
                } else {
                    self.printer.write(" is true");
                }
            }
            Expression::IsFalse { expr, negated } => {
                self.format_expression(expr);
                if *negated {
                    self.printer.write(" is not false");
                } else {
                    self.printer.write(" is false");
                }
            }
            Expression::IsDistinctFrom { expr, other, negated } => {
                self.format_expression(expr);
                if *negated {
                    self.printer.write(" is not distinct from ");
                } else {
                    self.printer.write(" is distinct from ");
                }
                self.format_expression(other);
            }
            Expression::Cast { expr, data_type, shorthand, try_cast } => {
                if *shorthand {
                    // Use Snowflake's :: cast syntax
                    self.format_expression(expr);
                    self.printer.write("::");
                    self.format_data_type(data_type);
                } else if *try_cast {
                    // Use TRY_CAST(expr AS type) syntax
                    self.printer.write("try_cast(");
                    self.format_expression(expr);
                    self.printer.write(" as ");
                    self.format_data_type(data_type);
                    self.printer.write(")");
                } else {
                    // Use standard CAST(expr AS type) syntax
                    self.printer.write("cast(");
                    self.format_expression(expr);
                    self.printer.write(" as ");
                    self.format_data_type(data_type);
                    self.printer.write(")");
                }
            }
            Expression::Extract { field, expr } => {
                self.printer.write("extract(");
                self.printer.write(&format_identifier(field));
                self.printer.write(" from ");
                self.format_expression(expr);
                self.printer.write(")");
            }
            Expression::JinjaExpression(content) => {
                // This is a placeholder identifier that will be replaced by reintegrate
                self.printer.write(content);
            }
            Expression::JinjaBlock(content) => {
                self.printer.write(content);
            }
            Expression::Parenthesized(inner) => {
                if self.is_complex_expression(inner) {
                    // Complex expressions inside parens get their own lines
                    self.printer.write("(");
                    self.printer.indent();
                    self.printer.newline();
                    self.format_expression(inner);
                    self.printer.dedent();
                    self.printer.newline();
                    self.printer.write(")");
                } else {
                    self.printer.write("(");
                    self.format_expression(inner);
                    self.printer.write(")");
                }
            }
            Expression::SemiStructuredAccess { expr, path } => {
                self.format_expression(expr);
                self.printer.write(":");
                self.printer.write(path);
            }
            Expression::ArrayAccess { expr, index } => {
                self.format_expression(expr);
                self.printer.write("[");
                self.format_expression(index);
                self.printer.write("]");
            }
            Expression::Exists { subquery } => {
                self.printer.write("exists (");
                let was_in_subquery = self.in_subquery;
                self.in_subquery = true;
                self.format_select(subquery);
                self.in_subquery = was_in_subquery;
                self.printer.write(")");
            }
            Expression::PositionalColumn(n) => {
                self.printer.write(&format!("${}", n));
            }
            Expression::LikeAny { expr, patterns, case_insensitive, quantifier } => {
                self.format_expression(expr);
                self.printer.write(if *case_insensitive { " ilike " } else { " like " });
                self.printer.write(quantifier);
                self.printer.write("(");
                for (i, pattern) in patterns.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    self.format_expression(pattern);
                }
                self.printer.write(")");
            }
            Expression::RowPatternModifier { modifier, expr } => {
                self.printer.write(&modifier.to_lowercase());
                self.printer.write(" ");
                self.format_expression(expr);
            }
        }
    }

    fn format_literal(&mut self, lit: &Literal) {
        match lit {
            Literal::Null => self.printer.write("null"),
            Literal::Boolean(true) => self.printer.write("true"),
            Literal::Boolean(false) => self.printer.write("false"),
            Literal::Integer(n) => self.printer.write(&format!("{}", n)),
            Literal::Float(n) => self.printer.write(&format!("{}", n)),
            Literal::String(s) => {
                self.printer.write("'");
                self.printer.write(&s.replace('\'', "''"));
                self.printer.write("'");
            }
            Literal::Date(s) => {
                self.printer.write("date '");
                self.printer.write(s);
                self.printer.write("'");
            }
            Literal::Timestamp(s) => {
                self.printer.write("timestamp '");
                self.printer.write(s);
                self.printer.write("'");
            }
        }
    }

    fn format_binary_operator(&mut self, op: &BinaryOperator) {
        let s = match op {
            BinaryOperator::Plus => "+",
            BinaryOperator::Minus => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
            BinaryOperator::Modulo => "%",
            BinaryOperator::Eq => "=",
            BinaryOperator::NotEq => "!=",
            BinaryOperator::Lt => "<",
            BinaryOperator::LtEq => "<=",
            BinaryOperator::Gt => ">",
            BinaryOperator::GtEq => ">=",
            BinaryOperator::And => "and",
            BinaryOperator::Or => "or",
            BinaryOperator::Like => "like",
            BinaryOperator::ILike => "ilike",
            BinaryOperator::Concat => "||",
        };
        self.printer.write(s);
    }

    fn format_unary_operator(&mut self, op: &UnaryOperator) {
        let s = match op {
            UnaryOperator::Not => "not",
            UnaryOperator::Minus => "-",
            UnaryOperator::Plus => "+",
        };
        self.printer.write(s);
    }

    fn format_case(&mut self, case: &CaseExpression) {
        self.printer.write("case");
        if let Some(operand) = &case.operand {
            self.printer.write(" ");
            self.format_expression(operand);
        }
        // Indent for WHEN clauses
        self.printer.indent();
        for when in &case.when_clauses {
            self.printer.newline();
            self.printer.write("when ");
            self.format_expression(&when.condition);
            // Indent for THEN
            self.printer.indent();
            self.printer.newline();
            self.printer.write("then ");
            self.format_expression(&when.result);
            self.printer.dedent();
        }
        if let Some(else_expr) = &case.else_clause {
            self.printer.newline();
            self.printer.write("else ");
            self.format_expression(else_expr);
        }
        self.printer.dedent();
        self.printer.newline();
        self.printer.write("end");
    }

    fn format_window_spec(&mut self, spec: &WindowSpec) {
        let mut first = true;
        if let Some(partition_by) = &spec.partition_by {
            self.printer.write("partition by ");
            for (i, expr) in partition_by.iter().enumerate() {
                if i > 0 {
                    self.printer.write(", ");
                }
                self.format_expression(expr);
            }
            first = false;
        }
        if let Some(order_by) = &spec.order_by {
            if !first {
                self.printer.write(" ");
            }
            self.printer.write("order by ");
            for (i, item) in order_by.iter().enumerate() {
                if i > 0 {
                    self.printer.write(", ");
                }
                self.format_expression(&item.expr);
                if let Some(dir) = &item.direction {
                    match dir {
                        SortDirection::Asc => self.printer.write(" asc"),
                        SortDirection::Desc => self.printer.write(" desc"),
                    }
                }
            }
            first = false;
        }
        if let Some(frame) = &spec.frame {
            if !first {
                self.printer.write(" ");
            }
            self.format_window_frame(frame);
        }
    }

    fn format_window_frame(&mut self, frame: &WindowFrame) {
        let unit = match frame.unit {
            WindowFrameUnit::Rows => "rows",
            WindowFrameUnit::Range => "range",
            WindowFrameUnit::Groups => "groups",
        };
        self.printer.write(unit);
        self.printer.write(" ");

        if let Some(end) = &frame.end {
            self.printer.write("between ");
            self.format_frame_bound(&frame.start);
            self.printer.write(" and ");
            self.format_frame_bound(end);
        } else {
            self.format_frame_bound(&frame.start);
        }
    }

    fn format_frame_bound(&mut self, bound: &WindowFrameBound) {
        match bound {
            WindowFrameBound::CurrentRow => self.printer.write("current row"),
            WindowFrameBound::UnboundedPreceding => self.printer.write("unbounded preceding"),
            WindowFrameBound::UnboundedFollowing => self.printer.write("unbounded following"),
            WindowFrameBound::Preceding(value) => {
                self.format_frame_bound_value(value);
                self.printer.write(" preceding");
            }
            WindowFrameBound::Following(value) => {
                self.format_frame_bound_value(value);
                self.printer.write(" following");
            }
        }
    }

    fn format_frame_bound_value(&mut self, value: &FrameBoundValue) {
        match value {
            FrameBoundValue::Numeric(n) => {
                self.printer.write(&n.to_string());
            }
            FrameBoundValue::Interval { value, unit } => {
                self.printer.write("interval '");
                self.printer.write(value);
                self.printer.write("'");
                if !unit.is_empty() {
                    self.printer.write(" ");
                    self.printer.write(unit);
                }
            }
        }
    }

    fn format_data_type(&mut self, dt: &DataType) {
        let s = match dt {
            DataType::Boolean => "boolean".to_string(),
            DataType::Int => "int".to_string(),
            DataType::Integer => "integer".to_string(),
            DataType::BigInt => "bigint".to_string(),
            DataType::Float => "float".to_string(),
            DataType::Double => "double".to_string(),
            DataType::Decimal(p, s) => {
                match (p, s) {
                    (Some(p), Some(s)) => format!("decimal({}, {})", p, s),
                    (Some(p), None) => format!("decimal({})", p),
                    _ => "decimal".to_string(),
                }
            }
            DataType::Number(p, s) => {
                match (p, s) {
                    (Some(p), Some(s)) => format!("number({}, {})", p, s),
                    (Some(p), None) => format!("number({})", p),
                    _ => "number".to_string(),
                }
            }
            DataType::Varchar(len) => {
                match len {
                    Some(n) => format!("varchar({})", n),
                    None => "varchar".to_string(),
                }
            }
            DataType::String(len) => {
                match len {
                    Some(n) => format!("string({})", n),
                    None => "string".to_string(),
                }
            }
            DataType::Char(len) => {
                match len {
                    Some(n) => format!("char({})", n),
                    None => "char".to_string(),
                }
            }
            DataType::Text => "text".to_string(),
            DataType::Date => "date".to_string(),
            DataType::Time => "time".to_string(),
            DataType::Timestamp => "timestamp".to_string(),
            DataType::TimestampTz => "timestamp_tz".to_string(),
            DataType::Variant => "variant".to_string(),
            DataType::Object => "object".to_string(),
            DataType::Array => "array".to_string(),
        };
        self.printer.write(&s);
    }

    fn format_insert(&mut self, stmt: &InsertStatement) {
        self.printer.write("insert into ");
        self.printer.write(&format_identifier(&stmt.table));

        // Format columns - vertical if >= 4 columns
        if let Some(columns) = &stmt.columns {
            if columns.len() >= 4 {
                // Vertical with leading commas
                self.printer.write(" (");
                self.printer.indent();
                self.printer.newline();
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        self.printer.newline();
                        self.printer.write(", ");
                    }
                    self.printer.write(&format_identifier(col));
                }
                self.printer.dedent();
                self.printer.newline();
                self.printer.write(")");
            } else {
                self.printer.write(" (");
                self.printer.write(&columns.iter()
                    .map(|c| format_identifier(c))
                    .collect::<Vec<_>>()
                    .join(", "));
                self.printer.write(")");
            }
        }

        self.printer.newline();
        match &stmt.source {
            InsertSource::Values(rows) => {
                self.printer.write("values ");
                // Use the first row's length to determine formatting
                let first_row_len = rows.first().map_or(0, |r| r.len());
                if rows.len() == 1 && first_row_len <= 3 {
                    // Single row with few values: inline
                    self.printer.write("(");
                    for (j, expr) in rows[0].iter().enumerate() {
                        if j > 0 {
                            self.printer.write(", ");
                        }
                        self.format_expression(expr);
                    }
                    self.printer.write(")");
                } else {
                    // Multiple rows or many values: vertical with leading commas
                    self.printer.write("(");
                    self.printer.indent();
                    self.printer.newline();
                    for (i, row) in rows.iter().enumerate() {
                        if i > 0 {
                            self.printer.newline();
                            self.printer.write("), (");
                            self.printer.newline();
                        }
                        for (j, expr) in row.iter().enumerate() {
                            if j > 0 {
                                self.printer.newline();
                                self.printer.write(", ");
                            }
                            self.format_expression(expr);
                        }
                    }
                    self.printer.dedent();
                    self.printer.newline();
                    self.printer.write(")");
                }
            }
            InsertSource::Query(query) => {
                self.format_select(query);
            }
        }
    }

    fn format_update(&mut self, stmt: &UpdateStatement) {
        self.printer.write("update ");
        self.printer.write(&format_identifier(&stmt.table));

        if let Some(alias) = &stmt.alias {
            self.printer.write(" ");
            self.printer.write(&format_identifier(alias));
        }

        self.printer.newline();
        self.printer.write("set");

        if stmt.assignments.len() == 1 {
            // Single assignment: inline
            self.printer.write(" ");
            let (col, expr) = &stmt.assignments[0];
            self.printer.write(&format_identifier(col));
            self.printer.write(" = ");
            self.format_expression(expr);
        } else {
            // Multiple assignments: vertical with leading commas
            self.printer.indent();
            self.printer.newline();

            for (i, (col, expr)) in stmt.assignments.iter().enumerate() {
                if i > 0 {
                    self.printer.newline();
                    self.printer.write(", ");
                }
                self.printer.write(&format_identifier(col));
                self.printer.write(" = ");
                self.format_expression(expr);
            }
            self.printer.dedent();
        }

        if let Some(from) = &stmt.from {
            self.printer.newline();
            self.format_from_clause(from);
        }

        if let Some(where_clause) = &stmt.where_clause {
            self.printer.newline();
            self.format_where_clause(where_clause);
        }
    }

    fn format_delete(&mut self, stmt: &DeleteStatement) {
        self.printer.write("delete from ");
        self.printer.write(&format_identifier(&stmt.table));

        if let Some(alias) = &stmt.alias {
            self.printer.write(" ");
            self.printer.write(&format_identifier(alias));
        }

        if let Some(using) = &stmt.using {
            self.printer.newline();
            self.printer.write("using ");
            self.format_table_reference(&using.table);
        }

        if let Some(where_clause) = &stmt.where_clause {
            self.printer.newline();
            self.format_where_clause(where_clause);
        }
    }

    fn format_merge(&mut self, stmt: &MergeStatement) {
        self.printer.write("merge into ");
        self.printer.write(&format_identifier(&stmt.target));

        if let Some(alias) = &stmt.target_alias {
            self.printer.write(" ");
            self.printer.write(&format_identifier(alias));
        }

        self.printer.newline();
        self.printer.write("using ");
        self.format_table_reference(&stmt.source);

        self.printer.indent();
        self.printer.newline();
        self.printer.write("on ");
        self.format_expression(&stmt.condition);
        self.printer.dedent();

        for clause in &stmt.clauses {
            self.printer.newline();
            self.format_merge_clause(clause);
        }
    }

    fn format_merge_clause(&mut self, clause: &MergeClause) {
        match clause {
            MergeClause::WhenMatched { condition, action } => {
                self.printer.write("when matched");
                if let Some(cond) = condition {
                    self.printer.write(" and ");
                    self.format_expression(cond);
                }
                self.printer.write(" then");
                self.printer.indent();
                self.printer.newline();
                self.format_merge_action(action);
                self.printer.dedent();
            }
            MergeClause::WhenNotMatched { condition, action } => {
                self.printer.write("when not matched");
                if let Some(cond) = condition {
                    self.printer.write(" and ");
                    self.format_expression(cond);
                }
                self.printer.write(" then");
                self.printer.indent();
                self.printer.newline();
                self.format_merge_action(action);
                self.printer.dedent();
            }
        }
    }

    fn format_merge_action(&mut self, action: &MergeAction) {
        match action {
            MergeAction::Update(assignments) => {
                self.printer.write("update set ");
                // Always inline for MERGE UPDATE since it's usually just one or two assignments
                for (i, (col, expr)) in assignments.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    self.printer.write(&format_identifier(col));
                    self.printer.write(" = ");
                    self.format_expression(expr);
                }
            }
            MergeAction::Delete => {
                self.printer.write("delete");
            }
            MergeAction::Insert { columns, values } => {
                self.printer.write("insert");
                if let Some(cols) = columns {
                    self.printer.write(" (");
                    self.printer.write(&cols.iter()
                        .map(|c| format_identifier(c))
                        .collect::<Vec<_>>()
                        .join(", "));
                    self.printer.write(")");
                }
                self.printer.newline();
                self.printer.write("values (");
                for (i, expr) in values.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    self.format_expression(expr);
                }
                self.printer.write(")");
            }
        }
    }

    fn format_create_table(&mut self, stmt: &CreateTableStatement) {
        self.printer.write("create table ");
        if stmt.if_not_exists {
            self.printer.write("if not exists ");
        }
        self.printer.write(&format_identifier(&stmt.name));

        if let Some(source) = &stmt.clone_source {
            self.printer.write(" clone ");
            self.printer.write(&format_identifier(source));
        } else if let Some(query) = &stmt.as_query {
            self.printer.write(" as");
            self.printer.newline();
            self.format_select(query);
        } else {
            self.printer.write(" (");
            self.printer.indent();
            self.printer.newline();

            for (i, col) in stmt.columns.iter().enumerate() {
                if i > 0 {
                    self.printer.newline();
                    self.printer.write(", ");
                }
                self.format_column_definition(col);
            }

            self.printer.dedent();
            self.printer.newline();
            self.printer.write(")");
        }
    }

    fn format_column_definition(&mut self, col: &ColumnDefinition) {
        self.printer.write(&format_identifier(&col.name));
        self.printer.write(" ");
        self.format_data_type(&col.data_type);

        if !col.nullable {
            self.printer.write(" not null");
        }

        if let Some(default) = &col.default {
            self.printer.write(" default ");
            self.format_expression(default);
        }
    }

    fn format_create_view(&mut self, stmt: &CreateViewStatement) {
        if stmt.or_replace {
            self.printer.write("create or replace view ");
        } else {
            self.printer.write("create view ");
        }
        self.printer.write(&format_identifier(&stmt.name));

        if let Some(columns) = &stmt.columns {
            self.printer.write(" (");
            self.printer.write(&columns.iter()
                .map(|c| format_identifier(c))
                .collect::<Vec<_>>()
                .join(", "));
            self.printer.write(")");
        }

        if stmt.copy_grants {
            self.printer.write(" copy grants");
        }

        self.printer.write(" as");
        self.printer.newline();
        // Views are formatted like subqueries (FROM on newline for star queries)
        let was_in_subquery = self.in_subquery;
        self.in_subquery = true;
        self.format_select(&stmt.query);
        self.in_subquery = was_in_subquery;
    }

    fn format_alter_table(&mut self, stmt: &AlterTableStatement) {
        self.printer.write("alter table ");
        self.printer.write(&format_identifier(&stmt.name));
        self.printer.write(" ");

        match &stmt.action {
            AlterTableAction::AddColumn(col) => {
                self.printer.write("add column ");
                self.format_column_definition(col);
            }
            AlterTableAction::DropColumn(name) => {
                self.printer.write("drop column ");
                self.printer.write(&format_identifier(name));
            }
            AlterTableAction::RenameColumn { old, new } => {
                self.printer.write("rename column ");
                self.printer.write(&format_identifier(old));
                self.printer.write(" to ");
                self.printer.write(&format_identifier(new));
            }
            AlterTableAction::AlterColumn { name, data_type } => {
                self.printer.write("alter column ");
                self.printer.write(&format_identifier(name));
                self.printer.write(" set data type ");
                self.format_data_type(data_type);
            }
        }
    }

    fn format_drop_table(&mut self, stmt: &DropTableStatement) {
        self.printer.write("drop table ");
        if stmt.if_exists {
            self.printer.write("if exists ");
        }
        self.printer.write(&format_identifier(&stmt.name));
    }

    fn format_drop_view(&mut self, stmt: &DropViewStatement) {
        self.printer.write("drop view ");
        if stmt.if_exists {
            self.printer.write("if exists ");
        }
        self.printer.write(&format_identifier(&stmt.name));
    }
}

/// Check if a string is a Jinja placeholder that should be preserved as-is
fn is_jinja_placeholder(s: &str) -> bool {
    let upper = s.to_uppercase();
    upper.starts_with(jinja::PLACEHOLDER_PREFIX)
}

/// Check if a string contains an embedded Jinja placeholder
fn contains_jinja_placeholder(s: &str) -> bool {
    s.to_uppercase().contains(jinja::PLACEHOLDER_PREFIX)
}

/// Format an identifier, preserving Jinja placeholders
fn format_identifier(s: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to simulate placeholder extraction from Jinja-extracted input
    fn extract_test(input: &str) -> JinjaLineInfo {
        extract_statement_level_placeholders(input)
    }

    // ===============================================
    // Tests for extract_statement_level_placeholders
    // ===============================================

    mod leading_placeholder_tests {
        use super::*;

        #[test]
        fn test_leading_placeholder_only() {
            let input = "__SQLFLIGHT_JINJA_001__";
            let result = extract_test(input);
            assert_eq!(result.leading, vec!["__SQLFLIGHT_JINJA_001__"]);
            assert!(result.body.trim().is_empty());
            assert!(result.trailing.is_empty());
            assert!(result.inline_statements.is_empty());
        }

        #[test]
        fn test_leading_placeholder_with_sql() {
            let input = "__SQLFLIGHT_JINJA_001__\nSELECT 1;";
            let result = extract_test(input);
            assert_eq!(result.leading, vec!["__SQLFLIGHT_JINJA_001__"]);
            assert!(result.body.contains("SELECT 1;"));
            assert!(result.trailing.is_empty());
        }

        #[test]
        fn test_multiple_leading_placeholders() {
            let input = "__SQLFLIGHT_JINJA_001__\n__SQLFLIGHT_JINJA_002__\nSELECT 1;";
            let result = extract_test(input);
            assert_eq!(result.leading.len(), 2);
            assert_eq!(result.leading[0], "__SQLFLIGHT_JINJA_001__");
            assert_eq!(result.leading[1], "__SQLFLIGHT_JINJA_002__");
            assert!(result.body.contains("SELECT 1;"));
        }

        #[test]
        fn test_leading_placeholder_with_whitespace() {
            let input = "  __SQLFLIGHT_JINJA_001__  \nSELECT 1;";
            let result = extract_test(input);
            assert_eq!(result.leading, vec!["__SQLFLIGHT_JINJA_001__"]);
        }
    }

    mod trailing_placeholder_tests {
        use super::*;

        #[test]
        fn test_trailing_placeholder_only() {
            let input = "__SQLFLIGHT_JINJA_001__";
            let result = extract_test(input);
            // A single placeholder can be leading or trailing - we treat it as leading
            assert_eq!(result.leading, vec!["__SQLFLIGHT_JINJA_001__"]);
        }

        #[test]
        fn test_trailing_placeholder_with_sql() {
            let input = "SELECT 1;\n__SQLFLIGHT_JINJA_001__";
            let result = extract_test(input);
            assert!(result.leading.is_empty());
            assert!(result.body.contains("SELECT 1;"));
            assert_eq!(result.trailing, vec!["__SQLFLIGHT_JINJA_001__"]);
        }

        #[test]
        fn test_multiple_trailing_placeholders() {
            let input = "SELECT 1;\n__SQLFLIGHT_JINJA_001__\n__SQLFLIGHT_JINJA_002__";
            let result = extract_test(input);
            assert!(result.leading.is_empty());
            assert!(result.body.contains("SELECT 1;"));
            assert_eq!(result.trailing.len(), 2);
        }

        #[test]
        fn test_trailing_placeholder_with_whitespace() {
            let input = "SELECT 1;\n  __SQLFLIGHT_JINJA_001__  ";
            let result = extract_test(input);
            assert_eq!(result.trailing, vec!["__SQLFLIGHT_JINJA_001__"]);
        }
    }

    mod inline_placeholder_tests {
        use super::*;

        #[test]
        fn test_inline_placeholder_after_comment() {
            let input = "-- comment\n__SQLFLIGHT_JINJA_001__\nSELECT 1;";
            let result = extract_test(input);
            assert!(result.leading.is_empty());
            assert_eq!(result.inline_statements.len(), 1);
            assert_eq!(result.inline_statements[0].1, "__SQLFLIGHT_JINJA_001__");
            // Body should contain the comment but not the placeholder
            assert!(result.body.contains("-- comment"));
            assert!(!result.body.contains("__SQLFLIGHT_JINJA_001__"));
        }

        #[test]
        fn test_inline_placeholder_between_statements() {
            let input = "SELECT 1;\n__SQLFLIGHT_JINJA_001__\nSELECT 2;";
            let result = extract_test(input);
            assert!(result.leading.is_empty());
            assert_eq!(result.inline_statements.len(), 1);
            assert_eq!(result.inline_statements[0].1, "__SQLFLIGHT_JINJA_001__");
        }

        #[test]
        fn test_multiple_inline_placeholders() {
            let input = "SELECT 1;\n__SQLFLIGHT_JINJA_001__\n__SQLFLIGHT_JINJA_002__\nSELECT 2;";
            let result = extract_test(input);
            assert_eq!(result.inline_statements.len(), 2);
        }

        #[test]
        fn test_inline_placeholder_tracks_line_number() {
            let input = "-- comment\n__SQLFLIGHT_JINJA_001__\nSELECT 1;";
            let result = extract_test(input);
            // The placeholder is on line 1 (0-indexed)
            assert_eq!(result.inline_statements[0].0, 1);
        }
    }

    mod mixed_placeholder_tests {
        use super::*;

        #[test]
        fn test_mixed_leading_inline_trailing() {
            let input = "__SQLFLIGHT_JINJA_001__\nSELECT 1;\n__SQLFLIGHT_JINJA_002__\nSELECT 2;\n__SQLFLIGHT_JINJA_003__";
            let result = extract_test(input);
            assert_eq!(result.leading, vec!["__SQLFLIGHT_JINJA_001__"]);
            assert_eq!(result.inline_statements.len(), 1);
            assert_eq!(result.inline_statements[0].1, "__SQLFLIGHT_JINJA_002__");
            assert_eq!(result.trailing, vec!["__SQLFLIGHT_JINJA_003__"]);
        }

        #[test]
        fn test_comments_and_placeholders() {
            let input = "-- c1\n__SQLFLIGHT_JINJA_001__\n-- c2\n__SQLFLIGHT_JINJA_002__\nSELECT 1;";
            let result = extract_test(input);
            // Inline placeholders extracted
            assert_eq!(result.inline_statements.len(), 2);
            // Body should contain comments but not placeholders
            assert!(result.body.contains("-- c1"));
            assert!(result.body.contains("-- c2"));
        }

        #[test]
        fn test_leading_placeholder_then_comment_then_inline() {
            let input = "__SQLFLIGHT_JINJA_001__\n-- comment\n__SQLFLIGHT_JINJA_002__\nSELECT 1;";
            let result = extract_test(input);
            assert_eq!(result.leading, vec!["__SQLFLIGHT_JINJA_001__"]);
            assert_eq!(result.inline_statements.len(), 1);
            assert_eq!(result.inline_statements[0].1, "__SQLFLIGHT_JINJA_002__");
        }
    }

    // ===============================================
    // Tests for is_statement_level_placeholder
    // ===============================================

    mod is_statement_level_tests {
        use super::*;

        #[test]
        fn test_placeholder_after_semicolon_is_statement_level() {
            let lines = vec!["SELECT 1;", "__SQLFLIGHT_JINJA_001__", "SELECT 2;"];
            assert!(is_statement_level_placeholder(&lines, 1));
        }

        #[test]
        fn test_placeholder_after_comma_is_not_statement_level() {
            let lines = vec!["SELECT a,", "__SQLFLIGHT_JINJA_001__", "FROM t;"];
            assert!(!is_statement_level_placeholder(&lines, 1));
        }

        #[test]
        fn test_placeholder_before_from_is_not_statement_level() {
            let lines = vec!["SELECT __SQLFLIGHT_JINJA_001__", "__SQLFLIGHT_JINJA_002__", "FROM t;"];
            assert!(!is_statement_level_placeholder(&lines, 1));
        }

        #[test]
        fn test_placeholder_after_comment_is_statement_level() {
            let lines = vec!["-- comment", "__SQLFLIGHT_JINJA_001__", "SELECT 1;"];
            assert!(is_statement_level_placeholder(&lines, 1));
        }

        #[test]
        fn test_placeholder_after_open_paren_is_not_statement_level() {
            let lines = vec!["SELECT (", "__SQLFLIGHT_JINJA_001__", ") FROM t;"];
            assert!(!is_statement_level_placeholder(&lines, 1));
        }

        #[test]
        fn test_placeholder_before_where_is_not_statement_level() {
            let lines = vec!["SELECT *", "__SQLFLIGHT_JINJA_001__", "WHERE x = 1;"];
            assert!(!is_statement_level_placeholder(&lines, 1));
        }

        #[test]
        fn test_placeholder_alone_is_statement_level() {
            let lines = vec!["__SQLFLIGHT_JINJA_001__"];
            assert!(is_statement_level_placeholder(&lines, 0));
        }

        #[test]
        fn test_placeholder_after_and_is_not_statement_level() {
            let lines = vec!["WHERE a = 1 AND", "__SQLFLIGHT_JINJA_001__", ";"];
            assert!(!is_statement_level_placeholder(&lines, 1));
        }

        #[test]
        fn test_placeholder_before_join_is_not_statement_level() {
            let lines = vec!["SELECT * FROM a", "__SQLFLIGHT_JINJA_001__", "JOIN b ON a.id = b.id;"];
            assert!(!is_statement_level_placeholder(&lines, 1));
        }
    }

    // ===============================================
    // Tests for body_to_original_line mapping
    // ===============================================

    mod line_mapping_tests {
        use super::*;

        #[test]
        fn test_body_to_original_line_basic() {
            let input = "__SQLFLIGHT_JINJA_001__\nSELECT 1;\nSELECT 2;";
            let result = extract_test(input);
            // Leading placeholder is extracted, body contains the SQL
            // The mapping tracks original line numbers
            assert!(!result.body_to_original_line.is_empty());
            // Body starts after the leading placeholder
            // First body line (SELECT 1;) maps to original line 1
            // But we map from the start of the body segment
            assert!(!result.body.is_empty());
        }

        #[test]
        fn test_body_to_original_line_with_inline() {
            let input = "-- comment\n__SQLFLIGHT_JINJA_001__\nSELECT 1;";
            let result = extract_test(input);
            // Body has "-- comment", empty line, and "SELECT 1;" (placeholder was removed)
            // Body line 0 = -- comment = original line 0
            assert_eq!(result.body_to_original_line[0], 0);
            // Check that the mapping has entries
            assert!(result.body_to_original_line.len() >= 2);
        }

        #[test]
        fn test_body_to_original_line_preserves_empty_lines() {
            let input = "SELECT 1;\n\nSELECT 2;";
            let result = extract_test(input);
            assert_eq!(result.body_to_original_line.len(), 3);
            assert_eq!(result.body_to_original_line[0], 0);
            assert_eq!(result.body_to_original_line[1], 1); // empty line
            assert_eq!(result.body_to_original_line[2], 2);
        }
    }

    // ===============================================
    // Tests for is_pure_placeholder_line
    // ===============================================

    mod is_pure_placeholder_line_tests {
        use super::*;

        #[test]
        fn test_pure_placeholder() {
            assert!(is_pure_placeholder_line("__SQLFLIGHT_JINJA_001__"));
        }

        #[test]
        fn test_placeholder_with_whitespace() {
            assert!(is_pure_placeholder_line("  __SQLFLIGHT_JINJA_001__  "));
        }

        #[test]
        fn test_placeholder_with_other_text() {
            assert!(!is_pure_placeholder_line("SELECT __SQLFLIGHT_JINJA_001__"));
        }

        #[test]
        fn test_placeholder_in_middle() {
            assert!(!is_pure_placeholder_line("a __SQLFLIGHT_JINJA_001__ b"));
        }

        #[test]
        fn test_empty_line() {
            assert!(!is_pure_placeholder_line(""));
        }

        #[test]
        fn test_comment_only() {
            assert!(!is_pure_placeholder_line("-- comment"));
        }

        #[test]
        fn test_non_placeholder() {
            assert!(!is_pure_placeholder_line("SELECT 1"));
        }
    }
}
