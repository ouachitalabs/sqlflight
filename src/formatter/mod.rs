//! SQL formatting / pretty-printing logic

pub mod printer;
pub mod rules;

use crate::ast::*;
use crate::error::Result;
use crate::jinja;
use crate::parser;
use printer::{Printer, TARGET_WIDTH};
use rules::SELECT_COLUMN_THRESHOLD;

/// Format SQL string
pub fn format_sql(input: &str) -> Result<String> {
    // Step 1: Extract Jinja
    let (sql_with_placeholders, placeholders) = jinja::extract_jinja(input)?;

    // Step 2: Parse SQL
    let ast = parser::parse(&sql_with_placeholders)?;

    // Step 3: Format AST
    let formatted = format_ast(&ast)?;

    // Step 4: Reintegrate Jinja
    let result = jinja::reintegrate_jinja(&formatted, &placeholders);

    Ok(result)
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
}

impl Formatter {
    fn new() -> Self {
        Self {
            printer: Printer::new(),
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

        // Format columns and check if we should stay on same line for FROM
        let columns_inline = self.format_select_columns(&stmt.columns);

        // Determine if we can keep a simple query all on one line
        // If any column is *, we break to multiline when there's a WHERE clause
        let has_star = stmt.columns.iter().any(|c| matches!(c.expr, Expression::Star | Expression::QualifiedStar(_)));

        // Can only inline if: no other clauses except simple WHERE with named columns
        // Note: UNION doesn't force FROM to newline, each SELECT in UNION formats independently
        let has_extra_clauses = stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.qualify.is_some()
            || stmt.window.is_some()
            || stmt.order_by.is_some()
            || stmt.limit.is_some()
            || !stmt.joins.is_empty();

        let simple_query = columns_inline
            && !has_star  // No star columns when staying inline with WHERE
            && !has_extra_clauses
            && self.is_simple_where(&stmt.where_clause);

        // FROM should be on new line if: vertical columns, has star with WHERE/JOINs, or has extra clauses
        let force_from_newline = !columns_inline
            || (has_star && (stmt.where_clause.is_some() || !stmt.joins.is_empty()))
            || has_extra_clauses;

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
        self.printer.write(&cte.name.to_lowercase());

        if let Some(columns) = &cte.columns {
            self.printer.write(" (");
            self.printer.write(&columns.iter()
                .map(|c| c.to_lowercase())
                .collect::<Vec<_>>()
                .join(", "));
            self.printer.write(")");
        }

        self.printer.write(" as (");
        self.printer.indent();
        self.printer.newline();
        self.format_select(&cte.query);
        self.printer.dedent();
        self.printer.newline();
        self.printer.write(")");
    }

    /// Format SELECT columns. Returns true if formatted inline, false if vertical.
    fn format_select_columns(&mut self, columns: &[SelectColumn]) -> bool {
        // Estimate total width for inline decision
        let total_width: usize = columns.iter()
            .map(|c| self.estimate_column_width(c))
            .sum::<usize>() + (columns.len().saturating_sub(1)) * 2; // account for ", "

        // Check if any column contains a complex expression that forces vertical
        let has_complex_expr = columns.iter().any(|c| self.is_complex_expression(&c.expr));

        let should_inline = columns.len() <= SELECT_COLUMN_THRESHOLD
            && total_width + 7 <= TARGET_WIDTH  // 7 = "select "
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
            Expression::FunctionCall { name, args, .. } => {
                name.len() + 2 + args.iter().map(|a| self.estimate_expr_width(a) + 2).sum::<usize>()
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

    /// Check if an expression is complex enough to force vertical layout
    fn is_complex_expression(&self, expr: &Expression) -> bool {
        matches!(expr,
            Expression::Case(_)
            | Expression::Subquery(_)
        )
    }

    fn format_select_column(&mut self, col: &SelectColumn) {
        self.format_expression(&col.expr);
        if let Some(alias) = &col.alias {
            self.printer.write(" as ");
            self.printer.write(&alias.to_lowercase());
        }
    }

    fn format_from_clause(&mut self, from: &FromClause) {
        self.printer.write("from ");
        self.format_table_reference(&from.table);
    }

    fn format_table_reference(&mut self, table: &TableReference) {
        match table {
            TableReference::Table { name, alias, .. } => {
                self.printer.write(&name.to_lowercase());
                if let Some(a) = alias {
                    self.printer.write(" ");
                    self.printer.write(&a.to_lowercase());
                }
            }
            TableReference::Subquery { query, alias } => {
                self.printer.write("(");
                self.printer.newline();
                self.printer.indent();
                self.format_select(query);
                self.printer.dedent();
                self.printer.newline();
                self.printer.write(") ");
                self.printer.write(&alias.to_lowercase());
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
                self.printer.write(&name.to_lowercase());
                self.printer.write("(");
                for (i, (param_name, expr)) in args.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    if let Some(pname) = param_name {
                        self.printer.write(&pname.to_lowercase());
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
            self.printer.write(&alias.table_alias.to_lowercase());
            if !alias.column_aliases.is_empty() {
                self.printer.write(" (");
                self.printer.write(&alias.column_aliases.iter()
                    .map(|c| c.to_lowercase())
                    .collect::<Vec<_>>()
                    .join(", "));
                self.printer.write(")");
            }
        }
    }

    fn format_join(&mut self, join: &JoinClause) {
        // Note: We always output just "join" for Inner joins
        // since we don't track whether INNER was explicit or implicit
        let join_keyword = match join.join_type {
            JoinType::Inner => "join",
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
            self.printer.write(&def.name.to_lowercase());
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
            Expression::Identifier(name) => self.printer.write(&name.to_lowercase()),
            Expression::QualifiedIdentifier(parts) => {
                let formatted: Vec<String> = parts.iter().map(|p| p.to_lowercase()).collect();
                self.printer.write(&formatted.join("."));
            }
            Expression::Star => self.printer.write("*"),
            Expression::QualifiedStar(name) => {
                self.printer.write(&name.to_lowercase());
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
                self.printer.write(&name.to_lowercase());
                self.printer.write("(");
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        self.printer.write(", ");
                    }
                    self.format_expression(arg);
                }
                self.printer.write(")");
                if let Some(window_spec) = over {
                    self.printer.write(" over (");
                    self.format_window_spec(window_spec);
                    self.printer.write(")");
                }
            }
            Expression::Case(case) => self.format_case(case),
            Expression::Subquery(query) => {
                self.printer.write("(");
                self.printer.newline();
                self.printer.indent();
                self.format_select(query);
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
                self.printer.newline();
                self.printer.indent();
                self.format_select(subquery);
                self.printer.dedent();
                self.printer.newline();
                self.printer.write(")");
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
                self.printer.write(&field.to_lowercase());
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
                self.printer.write("(");
                self.format_expression(inner);
                self.printer.write(")");
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
        self.printer.write(&stmt.table.to_lowercase());

        if let Some(columns) = &stmt.columns {
            self.printer.write(" (");
            self.printer.write(&columns.iter()
                .map(|c| c.to_lowercase())
                .collect::<Vec<_>>()
                .join(", "));
            self.printer.write(")");
        }

        self.printer.newline();
        match &stmt.source {
            InsertSource::Values(rows) => {
                self.printer.write("values");
                for (i, row) in rows.iter().enumerate() {
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
            }
            InsertSource::Query(query) => {
                self.format_select(query);
            }
        }
    }

    fn format_update(&mut self, stmt: &UpdateStatement) {
        self.printer.write("update ");
        self.printer.write(&stmt.table.to_lowercase());

        if let Some(alias) = &stmt.alias {
            self.printer.write(" ");
            self.printer.write(&alias.to_lowercase());
        }

        self.printer.newline();
        self.printer.write("set");
        self.printer.newline();
        self.printer.indent();

        for (i, (col, expr)) in stmt.assignments.iter().enumerate() {
            if i > 0 {
                self.printer.newline();
                self.printer.write(", ");
            }
            self.printer.write(&col.to_lowercase());
            self.printer.write(" = ");
            self.format_expression(expr);
        }
        self.printer.dedent();

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
        self.printer.write(&stmt.table.to_lowercase());

        if let Some(alias) = &stmt.alias {
            self.printer.write(" ");
            self.printer.write(&alias.to_lowercase());
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
        self.printer.write(&stmt.target.to_lowercase());

        if let Some(alias) = &stmt.target_alias {
            self.printer.write(" ");
            self.printer.write(&alias.to_lowercase());
        }

        self.printer.newline();
        self.printer.write("using ");
        self.format_table_reference(&stmt.source);

        self.printer.newline();
        self.printer.write("on ");
        self.format_expression(&stmt.condition);

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
                self.printer.newline();
                self.printer.indent();
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
                self.printer.newline();
                self.printer.indent();
                self.format_merge_action(action);
                self.printer.dedent();
            }
        }
    }

    fn format_merge_action(&mut self, action: &MergeAction) {
        match action {
            MergeAction::Update(assignments) => {
                self.printer.write("update set");
                self.printer.newline();
                self.printer.indent();
                for (i, (col, expr)) in assignments.iter().enumerate() {
                    if i > 0 {
                        self.printer.newline();
                        self.printer.write(", ");
                    }
                    self.printer.write(&col.to_lowercase());
                    self.printer.write(" = ");
                    self.format_expression(expr);
                }
                self.printer.dedent();
            }
            MergeAction::Delete => {
                self.printer.write("delete");
            }
            MergeAction::Insert { columns, values } => {
                self.printer.write("insert");
                if let Some(cols) = columns {
                    self.printer.write(" (");
                    self.printer.write(&cols.iter()
                        .map(|c| c.to_lowercase())
                        .collect::<Vec<_>>()
                        .join(", "));
                    self.printer.write(")");
                }
                self.printer.write(" values (");
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
        self.printer.write(&stmt.name.to_lowercase());

        if let Some(query) = &stmt.as_query {
            self.printer.write(" as");
            self.printer.newline();
            self.format_select(query);
        } else {
            self.printer.write(" (");
            self.printer.newline();
            self.printer.indent();

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
        self.printer.write(&col.name.to_lowercase());
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
        self.printer.write(&stmt.name.to_lowercase());

        if let Some(columns) = &stmt.columns {
            self.printer.write(" (");
            self.printer.write(&columns.iter()
                .map(|c| c.to_lowercase())
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
        self.printer.write(&stmt.name.to_lowercase());
        self.printer.write(" ");

        match &stmt.action {
            AlterTableAction::AddColumn(col) => {
                self.printer.write("add column ");
                self.format_column_definition(col);
            }
            AlterTableAction::DropColumn(name) => {
                self.printer.write("drop column ");
                self.printer.write(&name.to_lowercase());
            }
            AlterTableAction::RenameColumn { old, new } => {
                self.printer.write("rename column ");
                self.printer.write(&old.to_lowercase());
                self.printer.write(" to ");
                self.printer.write(&new.to_lowercase());
            }
            AlterTableAction::AlterColumn { name, data_type } => {
                self.printer.write("alter column ");
                self.printer.write(&name.to_lowercase());
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
        self.printer.write(&stmt.name.to_lowercase());
    }

    fn format_drop_view(&mut self, stmt: &DropViewStatement) {
        self.printer.write("drop view ");
        if stmt.if_exists {
            self.printer.write("if exists ");
        }
        self.printer.write(&stmt.name.to_lowercase());
    }
}
