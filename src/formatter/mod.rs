//! SQL formatting / pretty-printing logic

pub mod printer;
pub mod rules;

use crate::ast::*;
use crate::error::Result;
use crate::jinja;
use crate::parser::lexer::{tokenize_with_comments, CommentToken};
use crate::parser::expr::Parser;
use crate::parser::stmt;
use printer::{Printer, TARGET_WIDTH};
use rules::SELECT_COLUMN_THRESHOLD;

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
    let mut parser = Parser::new(&tokenize_result.tokens);
    let ast = stmt::parse_statement(&mut parser)?;

    // Step 5: Format AST
    let formatted = format_ast(&ast)?;

    // Step 6: Interleave comments
    let with_comments = interleave_comments(&jinja_info.body, &formatted, &comments);

    // Step 7: Reconstruct with leading/trailing/inline placeholders
    let mut result = String::new();

    // Add leading placeholders
    for placeholder in &jinja_info.leading {
        result.push_str(placeholder);
        result.push('\n');
    }

    // Add formatted SQL with inline placeholders reinserted
    if jinja_info.inline_statements.is_empty() {
        result.push_str(&with_comments);
    } else {
        // Reinsert inline placeholders at approximately the right positions
        let formatted_lines: Vec<&str> = with_comments.lines().collect();
        let original_lines: Vec<&str> = jinja_info.body.lines().collect();

        // Map inline placeholders back to formatted output
        // This is approximate - we preserve them relative to their line context
        for (line_num, placeholder) in &jinja_info.inline_statements {
            // For now, insert inline placeholders at the start of the formatted output
            // This preserves them but may not maintain exact positioning
            result.push_str(placeholder);
            result.push('\n');
        }
        result.push_str(&with_comments);
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
    /// Placeholders on their own lines within the SQL (with their line positions)
    inline_statements: Vec<(usize, String)>,
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

    // Keep all lines in the body - placeholders within the SQL are treated as identifiers
    // Only leading/trailing placeholders are extracted
    let final_body = lines[body_start..body_end].join("\n");

    JinjaLineInfo {
        leading,
        body: final_body.trim().to_string(),
        trailing,
        inline_statements,
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

/// Extract a Jinja placeholder from the end of a string (only if on its own line)
fn extract_trailing_placeholder(s: &str) -> Option<String> {
    // Only extract trailing placeholders that are on their own line
    let lines: Vec<&str> = s.lines().collect();
    if let Some(last_line) = lines.last() {
        let trimmed = last_line.trim();
        let upper = trimmed.to_uppercase();
        if upper.starts_with(jinja::PLACEHOLDER_PREFIX) {
            // Check if this is a valid placeholder (ends with __)
            let prefix_len = jinja::PLACEHOLDER_PREFIX.len();
            if let Some(end_pos) = upper[prefix_len..].find("__") {
                let full_end = prefix_len + end_pos + 2;
                // Must be ONLY the placeholder on this line
                if full_end == trimmed.len() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

/// Interleave comments into formatted output based on original positions
fn interleave_comments(original: &str, formatted: &str, comments: &[CommentToken]) -> String {
    if comments.is_empty() {
        return formatted.to_string();
    }

    let original_lines: Vec<&str> = original.lines().collect();

    // Determine comment type and context for each comment
    #[derive(Debug, PartialEq, Clone)]
    enum CommentType {
        Standalone,      // On its own line (no SQL before it)
        Trailing,        // At end of line with SQL before it, no SQL after
        Inline,          // Between SQL tokens on same line (has SQL before AND after)
    }

    #[derive(Debug, Clone)]
    struct CommentInfo {
        comment: CommentToken,
        comment_type: CommentType,
        before_text: String,   // SQL text before the comment on the same line
        after_text: String,    // SQL text after the comment on the same line (for inline)
        orig_line: usize,      // Original line number (0-indexed)
    }

    let mut comment_infos: Vec<CommentInfo> = Vec::new();

    for comment in comments {
        let line_idx = comment.line.saturating_sub(1);
        if line_idx < original_lines.len() {
            let line = original_lines[line_idx];
            let comment_marker = if comment.is_block { "/*" } else { "--" };

            if let Some(pos) = line.find(comment_marker) {
                let before = &line[..pos];
                let after = if comment.is_block {
                    // Find the end of this comment
                    if let Some(end) = line[pos..].find("*/") {
                        &line[pos + end + 2..]
                    } else {
                        ""
                    }
                } else {
                    "" // Single-line comment goes to end
                };

                let has_sql_before = !before.trim().is_empty();
                let has_sql_after = !after.trim().is_empty()
                    && !after.trim().starts_with("--")
                    && !after.trim().starts_with("/*");

                let comment_type = if !has_sql_before {
                    CommentType::Standalone
                } else if has_sql_after {
                    CommentType::Inline
                } else {
                    CommentType::Trailing
                };

                comment_infos.push(CommentInfo {
                    comment: comment.clone(),
                    comment_type,
                    before_text: before.trim().to_lowercase(),
                    after_text: after.trim().to_lowercase(),
                    orig_line: line_idx,
                });
            } else {
                // Comment marker not found on this line - likely a multi-line block comment
                // continuation or something similar
                comment_infos.push(CommentInfo {
                    comment: comment.clone(),
                    comment_type: CommentType::Standalone,
                    before_text: String::new(),
                    after_text: String::new(),
                    orig_line: line_idx,
                });
            }
        } else {
            comment_infos.push(CommentInfo {
                comment: comment.clone(),
                comment_type: CommentType::Standalone,
                before_text: String::new(),
                after_text: String::new(),
                orig_line: line_idx,
            });
        }
    }

    // Count how many original lines have SQL content
    let orig_sql_lines: Vec<usize> = original_lines.iter()
        .enumerate()
        .filter(|(_, line)| {
            let trimmed = line.trim();
            !trimmed.is_empty()
                && !trimmed.starts_with("--")
                && !trimmed.starts_with("/*")
        })
        .map(|(i, _)| i)
        .collect();

    let formatted_lines: Vec<&str> = formatted.lines().collect();

    // If the formatter merged multiple SQL lines into fewer formatted lines,
    // we need to split them back up when there are trailing comments

    // Build the result by tracking original SQL lines
    let mut result = String::new();
    let mut formatted_char_idx = 0;
    let formatted_flat = formatted.replace('\n', " ");
    let mut comment_idx = 0;

    for (orig_line_idx, orig_line) in original_lines.iter().enumerate() {
        // Add standalone comments for this line
        while comment_idx < comment_infos.len()
            && comment_infos[comment_idx].orig_line == orig_line_idx
            && comment_infos[comment_idx].comment_type == CommentType::Standalone
        {
            if !result.is_empty() && !result.ends_with('\n') {
                result.push('\n');
            }
            result.push_str(&comment_infos[comment_idx].comment.text);
            comment_idx += 1;
        }

        // Check if this original line has SQL content
        let line_trimmed = orig_line.trim();
        let is_standalone_comment_line = line_trimmed.starts_with("--") || line_trimmed.starts_with("/*");
        let has_sql = !line_trimmed.is_empty() && !is_standalone_comment_line;

        if has_sql {
            // Collect comments for this line
            let line_comments: Vec<&CommentInfo> = comment_infos[comment_idx..]
                .iter()
                .take_while(|c| c.orig_line == orig_line_idx)
                .filter(|c| c.comment_type != CommentType::Standalone)
                .collect();

            let num_line_comments = line_comments.len();

            // Check if there's an inline comment - if so, the whole original line stays together
            let has_inline = line_comments.iter().any(|c| c.comment_type == CommentType::Inline);

            if has_inline {
                // For inline comments, find both before and after content in formatted output
                // and reconstruct the line with the comment inserted
                let inline_comment = line_comments.iter()
                    .find(|c| c.comment_type == CommentType::Inline)
                    .unwrap();

                let before_text = &inline_comment.before_text;
                let after_text = &inline_comment.after_text;
                let formatted_lower = formatted_flat.to_lowercase();

                // Find before text
                if let Some(before_pos) = formatted_lower[formatted_char_idx..].find(before_text) {
                    let abs_before_pos = formatted_char_idx + before_pos;
                    let before_end = abs_before_pos + before_text.len();

                    // Find after text
                    if let Some(after_pos) = formatted_lower[before_end..].find(after_text) {
                        let abs_after_pos = before_end + after_pos;
                        let after_end = abs_after_pos + after_text.len();

                        // Build the line: before + comment + after
                        let formatted_before = &formatted_flat[abs_before_pos..before_end];
                        let formatted_after = &formatted_flat[abs_after_pos..after_end];

                        if !result.is_empty() && !result.ends_with('\n') {
                            result.push('\n');
                        }
                        result.push_str(formatted_before.trim());
                        result.push(' ');
                        result.push_str(&inline_comment.comment.text);
                        result.push(' ');
                        result.push_str(formatted_after.trim());

                        formatted_char_idx = after_end;
                    }
                }
            } else {
                // No inline comment - get the SQL content before any trailing comment
                let sql_content = if let Some(pos) = line_trimmed.find("--") {
                    &line_trimmed[..pos]
                } else if let Some(pos) = line_trimmed.find("/*") {
                    &line_trimmed[..pos]
                } else {
                    line_trimmed
                };
                let sql_content = sql_content.trim().to_lowercase();

                // Find this content in the formatted output (flattened)
                let formatted_lower = formatted_flat.to_lowercase();
                if let Some(pos) = formatted_lower[formatted_char_idx..].find(&sql_content) {
                    let absolute_pos = formatted_char_idx + pos;
                    let end_pos = absolute_pos + sql_content.len();

                    // Get the formatted version of this SQL segment
                    let formatted_segment = &formatted_flat[absolute_pos..end_pos];

                    if !result.is_empty() && !result.ends_with('\n') {
                        result.push('\n');
                    }
                    result.push_str(formatted_segment.trim());

                    // Add trailing comments
                    for ci in &line_comments {
                        if ci.comment_type == CommentType::Trailing {
                            result.push(' ');
                            result.push_str(&ci.comment.text);
                        }
                    }

                    formatted_char_idx = end_pos;
                }
            }

            comment_idx += num_line_comments;
        }
    }

    // Append any remaining formatted content
    let remaining = formatted_flat[formatted_char_idx..].trim();
    if !remaining.is_empty() {
        if !result.is_empty() && !result.ends_with('\n') {
            result.push('\n');
        }
        result.push_str(remaining);
    }

    // Add any remaining comments
    while comment_idx < comment_infos.len() {
        if !result.is_empty() && !result.ends_with('\n') {
            result.push('\n');
        }
        result.push_str(&comment_infos[comment_idx].comment.text);
        comment_idx += 1;
    }

    // Add trailing newline if formatted had one
    if formatted.ends_with('\n') && !result.ends_with('\n') {
        result.push('\n');
    }

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

        // Check if FROM has a subquery
        let has_subquery_from = stmt.from.as_ref().map_or(false, |from| {
            matches!(from.table, TableReference::Subquery { .. })
        });

        // Can only inline if: no other clauses except simple WHERE with named columns
        // Note: UNION doesn't force FROM to newline, each SELECT in UNION formats independently
        let has_extra_clauses = stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.qualify.is_some()
            || stmt.window.is_some()
            || stmt.order_by.is_some()
            || stmt.limit.is_some()
            || !stmt.joins.is_empty()
            || has_subquery_from;

        // Estimate FROM clause width for column layout decision
        let from_width = stmt.from.as_ref().map_or(0, |from| {
            self.estimate_from_width(&from.table)
        });

        // Format columns and check if we should stay on same line for FROM
        // Force vertical if there are extra clauses
        let has_joins = !stmt.joins.is_empty();
        let columns_inline = self.format_select_columns(&stmt.columns, has_extra_clauses, from_width, has_joins);

        let simple_query = columns_inline
            && !has_star  // No star columns when staying inline with WHERE
            && !has_extra_clauses
            && self.is_simple_where(&stmt.where_clause)
            && !self.in_subquery;  // Never simple when inside a subquery

        // FROM should be on new line if: vertical columns, has star with WHERE/JOINs, has extra clauses,
        // or we're inside a subquery
        let force_from_newline = !columns_inline
            || (has_star && (stmt.where_clause.is_some() || !stmt.joins.is_empty()))
            || has_extra_clauses
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
        // When there are extra clauses (GROUP BY, WINDOW, etc.), use a tighter threshold
        // to keep each clause on its own line for readability
        let width_threshold = if has_extra_clauses { 80 } else { TARGET_WIDTH };
        let exceeds_width = full_line_width > width_threshold;
        // JOINs force vertical columns when there are multiple columns (but not for SELECT *)
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
            Expression::FunctionCall { name, args, over } => {
                let base = name.len() + 2 + args.iter().map(|a| self.estimate_expr_width(a) + 2).sum::<usize>();
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
                base + over_width
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
            TableReference::Table { name, alias, .. } => {
                self.printer.write(&format_identifier(name));
                if let Some(a) = alias {
                    self.printer.write(" ");
                    self.printer.write(&format_identifier(a));
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
                if *explicit_as {
                    self.printer.write(" as ");
                } else {
                    self.printer.write(" ");
                }
                self.printer.write(&format_identifier(alias));
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
            TableReference::TableFunction { name, args } => {
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
            }
            TableReference::Values(values) => {
                self.format_values(values);
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

    fn format_values(&mut self, values: &ValuesClause) {
        self.printer.write("values");
        for (i, row) in values.rows.iter().enumerate() {
            if i > 0 {
                self.printer.write(",");
            }
            self.printer.newline();
            self.printer.indent();
            self.printer.write("(");
            for (j, expr) in row.iter().enumerate() {
                if j > 0 {
                    self.printer.write(", ");
                }
                self.format_expression(expr);
            }
            self.printer.write(")");
            self.printer.dedent();
        }
        if let Some(alias) = &values.alias {
            self.printer.write(" as ");
            self.printer.write(&format_identifier(&alias.table_alias));
            if !alias.column_aliases.is_empty() {
                self.printer.write(" (");
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
            Expression::FunctionCall { name, args, over } => {
                self.printer.write(&format_identifier(name));
                self.printer.write("(");
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    self.format_expression(arg);
                }
                self.printer.write(")");
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
            Expression::Cast { expr, data_type } => {
                self.printer.write("cast(");
                self.format_expression(expr);
                self.printer.write(" as ");
                self.format_data_type(data_type);
                self.printer.write(")");
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
            WindowFrameBound::Preceding(None) => self.printer.write("unbounded preceding"),
            WindowFrameBound::Preceding(Some(n)) => {
                self.printer.write(&format!("{} preceding", n));
            }
            WindowFrameBound::Following(None) => self.printer.write("unbounded following"),
            WindowFrameBound::Following(Some(n)) => {
                self.printer.write(&format!("{} following", n));
            }
        }
    }

    fn format_data_type(&mut self, dt: &DataType) {
        let s = match dt {
            DataType::Boolean => "boolean".to_string(),
            DataType::Integer => "int".to_string(),
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
            DataType::Varchar(len) => {
                match len {
                    Some(n) => format!("varchar({})", n),
                    None => "varchar".to_string(),
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

        if let Some(query) = &stmt.as_query {
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

        self.printer.write(" as");
        self.printer.newline();
        self.format_select(&stmt.query);
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
