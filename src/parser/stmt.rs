//! Statement parsing
//!
//! Parses SQL statements (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.)

use crate::ast::*;
use crate::jinja::PLACEHOLDER_PREFIX;
use crate::parser::expr::{parse_data_type, parse_expression, parse_order_by_items, Parser};
use crate::parser::lexer::Token;
use crate::Result;

/// Check if a token is a Jinja placeholder identifier
fn is_jinja_placeholder_token(token: &Token) -> bool {
    match token {
        Token::Identifier(name) => name.to_uppercase().starts_with(PLACEHOLDER_PREFIX),
        _ => false,
    }
}

/// Parse a SQL statement from tokens
pub fn parse_statement(parser: &mut Parser) -> Result<Statement> {
    // Skip any leading Jinja config blocks, Jinja tokens, or Jinja placeholders
    while parser.check(&Token::JinjaExpression("".to_string()))
        || parser.check(&Token::JinjaStatement("".to_string()))
        || is_jinja_placeholder_token(parser.current())
    {
        parser.advance();
    }

    match parser.current() {
        Token::Select => parse_select_statement(parser).map(Statement::Select),
        Token::With => parse_with_statement(parser),
        Token::Insert => parse_insert_statement(parser).map(Statement::Insert),
        Token::Update => parse_update_statement(parser).map(Statement::Update),
        Token::Delete => parse_delete_statement(parser).map(Statement::Delete),
        Token::Merge => parse_merge_statement(parser).map(Statement::Merge),
        Token::Create => parse_create_statement(parser),
        Token::Alter => parse_alter_statement(parser),
        Token::Drop => parse_drop_statement(parser),
        _ => Err(parser.error(&format!("Unexpected token: {:?}", parser.current()))),
    }
}

/// Parse SELECT statement (may include WITH clause)
fn parse_with_statement(parser: &mut Parser) -> Result<Statement> {
    parser.expect(&Token::With)?;

    // Optional RECURSIVE keyword
    let recursive = parser.consume(&Token::Recursive);

    let mut ctes = Vec::new();

    loop {
        // Check if current token is a Jinja placeholder that represents CTE-generating block
        // (like a for-loop that generates multiple CTEs)
        let jinja_cte_placeholder = if let Token::Identifier(name) = parser.current() {
            let next_token = parser.peek();
            let next_is_as = matches!(next_token, Token::As);
            let next_is_lparen = matches!(next_token, Token::LParen);
            if name.starts_with("__SQLFLIGHT_JINJA_") && !next_is_as && !next_is_lparen {
                Some(name.clone())
            } else {
                None
            }
        } else {
            None
        };

        if let Some(name) = jinja_cte_placeholder {
            // This is a Jinja block placeholder that will expand to CTEs at runtime
            // Create a placeholder CTE with a Jinja query
            parser.advance();
            ctes.push(CommonTableExpression {
                name: name.clone(),
                columns: None,
                query: Box::new(SelectStatement {
                    columns: vec![SelectColumn {
                        expr: Expression::JinjaBlock(name.clone()),
                        alias: None,
                        explicit_as: false,
                    }],
                    ..Default::default()
                }),
            });

            // Check if there's a comma (more CTEs follow)
            // The Jinja for-loop might not end with a comma if it's the last iteration
            // so also check if next token looks like a CTE definition
            if parser.consume(&Token::Comma) {
                continue;
            }
            // No comma - check if next token could be a CTE name
            // (identifier followed by AS or LPAREN)
            if let Token::Identifier(_) = parser.current() {
                if matches!(parser.peek(), Token::As | Token::LParen) {
                    // Looks like another CTE follows without comma (from Jinja loop)
                    continue;
                }
            }
            break;
        }

        let cte = parse_cte(parser)?;
        ctes.push(cte);

        if !parser.consume(&Token::Comma) {
            break;
        }
    }

    // Now parse the main SELECT
    let mut select = parse_select_statement(parser)?;
    select.with_clause = Some(WithClause { recursive, ctes });

    Ok(Statement::Select(select))
}

/// Parse a single CTE
fn parse_cte(parser: &mut Parser) -> Result<CommonTableExpression> {
    let name = parse_identifier(parser)?;

    // Optional column list
    let columns = if parser.consume(&Token::LParen) {
        let mut cols = vec![parse_identifier(parser)?];
        while parser.consume(&Token::Comma) {
            cols.push(parse_identifier(parser)?);
        }
        parser.expect(&Token::RParen)?;
        Some(cols)
    } else {
        None
    };

    parser.expect(&Token::As)?;
    parser.expect(&Token::LParen)?;
    let query = Box::new(parse_select_statement(parser)?);
    parser.expect(&Token::RParen)?;

    Ok(CommonTableExpression {
        name,
        columns,
        query,
    })
}

/// Parse SELECT statement
pub fn parse_select_statement(parser: &mut Parser) -> Result<SelectStatement> {
    parser.expect(&Token::Select)?;

    let distinct = parser.consume(&Token::Distinct);

    // Parse columns
    let columns = parse_select_columns(parser)?;

    // FROM clause
    let from = if parser.consume(&Token::From) {
        Some(parse_from_clause(parser)?)
    } else {
        None
    };

    // JOIN clauses
    let joins = parse_join_clauses(parser)?;

    // PIVOT/UNPIVOT at SELECT level (after JOINs)
    // When there are JOINs, PIVOT/UNPIVOT applies to the entire result
    let (pivot, unpivot) = if !joins.is_empty() {
        let p = parse_pivot_clause(parser)?;
        let u = if p.is_none() { parse_unpivot_clause(parser)? } else { None };
        (p, u)
    } else {
        // Without JOINs, PIVOT/UNPIVOT might be in the table reference already,
        // but we also try SELECT-level for consistency
        let p = parse_pivot_clause(parser)?;
        let u = if p.is_none() { parse_unpivot_clause(parser)? } else { None };
        (p, u)
    };

    // WHERE clause
    let where_clause = if parser.consume(&Token::Where) {
        // WHERE without FROM is invalid (semantically meaningless)
        if from.is_none() && joins.is_empty() {
            return Err(crate::Error::ParseError {
                message: "WHERE clause requires FROM clause".to_string(),
                span: None,
            });
        }
        Some(WhereClause {
            condition: parse_expression(parser)?,
        })
    } else {
        None
    };

    // CONNECT BY clause (hierarchical query)
    let connect_by = parse_connect_by_clause(parser)?;

    // GROUP BY clause
    let group_by = if parser.consume(&Token::Group) {
        parser.expect(&Token::By)?;
        let mut expressions = vec![parse_expression(parser)?];
        while parser.consume(&Token::Comma) {
            expressions.push(parse_expression(parser)?);
        }
        Some(GroupByClause { expressions })
    } else {
        None
    };

    // HAVING clause
    let having = if parser.consume(&Token::Having) {
        Some(HavingClause {
            condition: parse_expression(parser)?,
        })
    } else {
        None
    };

    // QUALIFY clause (Snowflake-specific)
    let qualify = if parser.consume(&Token::Qualify) {
        Some(QualifyClause {
            condition: parse_expression(parser)?,
        })
    } else {
        None
    };

    // WINDOW clause
    let window = if parser.consume(&Token::Window) {
        let mut definitions = vec![parse_window_definition(parser)?];
        while parser.consume(&Token::Comma) {
            definitions.push(parse_window_definition(parser)?);
        }
        Some(WindowClause { definitions })
    } else {
        None
    };

    // ORDER BY clause
    let order_by = if parser.consume(&Token::Order) {
        parser.expect(&Token::By)?;
        Some(OrderByClause {
            items: parse_order_by_items(parser)?,
        })
    } else {
        None
    };

    // LIMIT clause
    let limit = if parser.consume(&Token::Limit) {
        let count = parse_expression(parser)?;
        let offset = if parser.consume(&Token::Offset) {
            Some(parse_expression(parser)?)
        } else {
            None
        };
        Some(LimitClause { count, offset })
    } else {
        None
    };

    // UNION / INTERSECT / EXCEPT
    let union = parse_set_operation(parser)?;

    // Optional semicolon
    let semicolon = parser.consume(&Token::Semicolon);

    Ok(SelectStatement {
        with_clause: None, // Set by caller if there's a WITH clause
        distinct,
        columns,
        from,
        joins,
        pivot,
        unpivot,
        where_clause,
        connect_by,
        group_by,
        having,
        qualify,
        window,
        order_by,
        limit,
        union,
        semicolon,
    })
}

/// Parse select column list
fn parse_select_columns(parser: &mut Parser) -> Result<Vec<SelectColumn>> {
    let mut columns = vec![parse_select_column(parser)?];

    while parser.consume(&Token::Comma) {
        columns.push(parse_select_column(parser)?);
    }

    Ok(columns)
}

/// Parse a single select column
fn parse_select_column(parser: &mut Parser) -> Result<SelectColumn> {
    let expr = parse_expression(parser)?;

    let (alias, explicit_as) = if parser.consume(&Token::As) {
        (Some(parse_identifier(parser)?), true)
    } else if matches!(parser.current(), Token::Identifier(_) | Token::QuotedIdentifier(_)) {
        // Allow alias without AS keyword
        let pos = parser.position();
        // Look ahead to make sure it's not a keyword
        if !is_keyword_that_ends_column(parser.current()) {
            (Some(parse_identifier(parser)?), false)
        } else {
            parser.restore(pos);
            (None, false)
        }
    } else {
        (None, false)
    };

    Ok(SelectColumn { expr, alias, explicit_as })
}

/// Check if token is a keyword that could end a column expression
fn is_keyword_that_ends_column(token: &Token) -> bool {
    matches!(
        token,
        Token::From
            | Token::Where
            | Token::Group
            | Token::Having
            | Token::Order
            | Token::Limit
            | Token::Union
            | Token::Intersect
            | Token::Except
            | Token::MinusSet
            | Token::Join
            | Token::Inner
            | Token::Left
            | Token::Right
            | Token::Full
            | Token::Cross
            | Token::On
            | Token::Qualify
            | Token::Comma
            | Token::RParen
            | Token::Semicolon
            | Token::Eof
    )
}

/// Parse FROM clause
fn parse_from_clause(parser: &mut Parser) -> Result<FromClause> {
    let table = parse_table_reference(parser)?;
    Ok(FromClause { table })
}

/// Parse table reference
fn parse_table_reference(parser: &mut Parser) -> Result<TableReference> {
    // Handle Jinja ref
    if let Token::JinjaExpression(content) = parser.current().clone() {
        parser.advance();
        return Ok(TableReference::JinjaRef(content));
    }

    // Handle LATERAL
    if parser.consume(&Token::Lateral) {
        let inner = parse_table_reference(parser)?;
        return Ok(TableReference::Lateral(Box::new(inner)));
    }

    // Handle stage reference (@stage_name, @stage_name/path, @~, @%table_name)
    if parser.consume(&Token::AtSign) {
        let (name, path) = parse_stage_path(parser)?;
        let alias = parse_optional_alias(parser);
        return Ok(TableReference::Stage { name, path, alias });
    }

    // Handle TABLE(function_call) - Snowflake table function syntax
    if parser.check(&Token::Table) {
        let pos = parser.position();
        parser.advance();
        if parser.consume(&Token::LParen) {
            // This is TABLE(function_call) syntax
            // Handle FLATTEN specifically since it's a keyword
            let func_name = if parser.consume(&Token::Flatten) {
                "FLATTEN".to_string()
            } else {
                parse_qualified_table_name(parser)?
            };
            parser.expect(&Token::LParen)?;
            let args = parse_table_function_args(parser)?;
            parser.expect(&Token::RParen)?;
            parser.expect(&Token::RParen)?;  // Close the outer TABLE()
            let alias = parse_optional_alias(parser);
            return Ok(TableReference::TableFunction { name: func_name, args, table_wrapper: true, alias });
        } else {
            // Not TABLE(...), restore and continue as regular table name
            parser.restore(pos);
        }
    }

    // Handle direct FLATTEN syntax (without TABLE wrapper)
    if parser.consume(&Token::Flatten) {
        parser.expect(&Token::LParen)?;
        let args = parse_table_function_args(parser)?;
        parser.expect(&Token::RParen)?;
        let alias = parse_optional_alias(parser);
        // Store as TableFunction with name "FLATTEN"
        return Ok(TableReference::TableFunction { name: "FLATTEN".to_string(), args, table_wrapper: false, alias });
    }

    // Handle VALUES clause (inline table values)
    // Only treat as VALUES clause if followed by '(' - otherwise it's a table named "values"
    if parser.check(&Token::Values) && matches!(parser.peek_at(1), Token::LParen) {
        parser.advance();  // consume VALUES
        let mut rows = Vec::new();
        // Parse first row
        parser.expect(&Token::LParen)?;
        let mut row = vec![parse_expression(parser)?];
        while parser.consume(&Token::Comma) {
            row.push(parse_expression(parser)?);
        }
        rows.push(row);
        parser.expect(&Token::RParen)?;
        // Parse additional rows
        while parser.consume(&Token::Comma) {
            parser.expect(&Token::LParen)?;
            let mut row = vec![parse_expression(parser)?];
            while parser.consume(&Token::Comma) {
                row.push(parse_expression(parser)?);
            }
            rows.push(row);
            parser.expect(&Token::RParen)?;
        }
        // Parse optional alias: AS t(col1, col2, ...)
        let alias = if parser.consume(&Token::As) {
            if let Token::Identifier(table_alias) = parser.current().clone() {
                parser.advance();
                let mut column_aliases = Vec::new();
                if parser.consume(&Token::LParen) {
                    loop {
                        if let Token::Identifier(col_name) = parser.current().clone() {
                            parser.advance();
                            column_aliases.push(col_name);
                        } else {
                            break;
                        }
                        if !parser.consume(&Token::Comma) {
                            break;
                        }
                    }
                    parser.expect(&Token::RParen)?;
                }
                Some(ValuesAlias { table_alias, column_aliases })
            } else {
                None
            }
        } else {
            None
        };
        return Ok(TableReference::Values(ValuesClause { rows, alias }));
    }

    // Handle subquery
    if parser.check(&Token::LParen) {
        parser.advance();
        if parser.check(&Token::Select) || parser.check(&Token::With) {
            let query = Box::new(parse_select_statement(parser)?);
            parser.expect(&Token::RParen)?;
            // Alias is optional when followed by PIVOT or UNPIVOT
            let (alias, explicit_as) = if parser.check(&Token::Pivot) || parser.check(&Token::Unpivot) {
                (None, false)
            } else {
                let (a, e) = parse_required_alias_with_as(parser)?;
                (Some(a), e)
            };
            return Ok(TableReference::Subquery { query, alias, explicit_as });
        } else {
            // Not a subquery, restore
            parser.restore(parser.position() - 1);
        }
    }

    // Table name
    let name = parse_qualified_table_name(parser)?;

    // Check if this is a table function call (name followed by parenthesis)
    // e.g., SPLIT_TO_TABLE(tags.tag_list, ',')
    if parser.check(&Token::LParen) {
        parser.advance();
        let args = parse_table_function_args(parser)?;
        parser.expect(&Token::RParen)?;
        let alias = parse_optional_alias(parser);
        return Ok(TableReference::TableFunction { name, args, table_wrapper: false, alias });
    }

    // Parse optional CHANGES clause (must come before TIME_TRAVEL)
    let changes = parse_changes_clause(parser)?;

    // Parse optional AT/BEFORE time travel clause (only if no CHANGES clause, since CHANGES has its own)
    let time_travel = if changes.is_none() {
        parse_time_travel_clause(parser)?
    } else {
        None
    };

    // Optional alias (before SAMPLE/PIVOT)
    let alias = parse_optional_alias(parser);

    // Parse optional SAMPLE/TABLESAMPLE clause
    let sample = parse_sample_clause(parser)?;

    // Parse optional PIVOT clause
    let pivot = parse_pivot_clause(parser)?;

    // Parse optional UNPIVOT clause
    let unpivot = parse_unpivot_clause(parser)?;

    // Parse optional MATCH_RECOGNIZE clause
    let match_recognize = parse_match_recognize_clause(parser)?;

    // Parse optional alias after PIVOT/UNPIVOT/MATCH_RECOGNIZE
    let alias = if (pivot.is_some() || unpivot.is_some() || match_recognize.is_some()) && alias.is_none() {
        parse_optional_alias(parser)
    } else {
        alias
    };

    Ok(TableReference::Table {
        name,
        alias,
        time_travel,
        changes,
        sample,
        pivot,
        unpivot,
        match_recognize,
    })
}

/// Parse a time travel point specification (inside the parentheses)
/// TIMESTAMP => expr | OFFSET => expr | STATEMENT => 'id'
fn parse_time_travel_point(parser: &mut Parser) -> Result<TimeTravelPoint> {
    if parser.consume(&Token::Timestamp) {
        parser.expect(&Token::FatArrow)?;
        let expr = parse_expression(parser)?;
        Ok(TimeTravelPoint::Timestamp(expr))
    } else if parser.consume(&Token::Offset) {
        parser.expect(&Token::FatArrow)?;
        let expr = parse_expression(parser)?;
        Ok(TimeTravelPoint::Offset(expr))
    } else if parser.consume(&Token::Statement) {
        parser.expect(&Token::FatArrow)?;
        if let Token::StringLiteral(id) = parser.current().clone() {
            parser.advance();
            Ok(TimeTravelPoint::Statement(id))
        } else {
            Err(crate::Error::ParseError {
                message: "Expected string literal for STATEMENT".to_string(),
                span: None,
            })
        }
    } else {
        Err(crate::Error::ParseError {
            message: "Expected TIMESTAMP, OFFSET, or STATEMENT".to_string(),
            span: None,
        })
    }
}

/// Parse optional AT/BEFORE time travel clause
fn parse_time_travel_clause(parser: &mut Parser) -> Result<Option<TimeTravelClause>> {
    // AT (TIMESTAMP => expr | OFFSET => expr | STATEMENT => 'id')
    // BEFORE (TIMESTAMP => expr | OFFSET => expr | STATEMENT => 'id')
    let is_at = if parser.consume(&Token::At) {
        true
    } else if parser.consume(&Token::Before) {
        false
    } else {
        return Ok(None);
    };

    parser.expect(&Token::LParen)?;
    let point = parse_time_travel_point(parser)?;
    parser.expect(&Token::RParen)?;

    if is_at {
        Ok(Some(TimeTravelClause::At(point)))
    } else {
        Ok(Some(TimeTravelClause::Before(point)))
    }
}

/// Parse optional CHANGES clause
/// CHANGES (INFORMATION => DEFAULT | APPEND_ONLY) AT/BEFORE (TIMESTAMP => ...)
fn parse_changes_clause(parser: &mut Parser) -> Result<Option<ChangesClause>> {
    if !parser.consume(&Token::Changes) {
        return Ok(None);
    }

    parser.expect(&Token::LParen)?;

    // Parse INFORMATION => DEFAULT | APPEND_ONLY
    if parser.check(&Token::Information) {
        parser.advance();
    } else if let Token::Identifier(name) = parser.current().clone() {
        if name.eq_ignore_ascii_case("information") {
            parser.advance();
        } else {
            return Err(crate::Error::ParseError {
                message: format!("Expected INFORMATION in CHANGES clause, found {}", name),
                span: None,
            });
        }
    }
    parser.expect(&Token::FatArrow)?;

    let information = if parser.consume(&Token::Default) {
        ChangesInformation::Default
    } else if let Token::Identifier(name) = parser.current().clone() {
        if name.eq_ignore_ascii_case("append_only") {
            parser.advance();
            ChangesInformation::AppendOnly
        } else if name.eq_ignore_ascii_case("default") {
            parser.advance();
            ChangesInformation::Default
        } else {
            return Err(crate::Error::ParseError {
                message: format!("Expected DEFAULT or APPEND_ONLY in CHANGES clause, found {}", name),
                span: None,
            });
        }
    } else {
        return Err(crate::Error::ParseError {
            message: "Expected DEFAULT or APPEND_ONLY in CHANGES clause".to_string(),
            span: None,
        });
    };

    parser.expect(&Token::RParen)?;

    // Parse required AT/BEFORE clause
    let at_or_before = parse_time_travel_clause(parser)?
        .ok_or_else(|| crate::Error::ParseError {
            message: "CHANGES clause requires AT or BEFORE clause".to_string(),
            span: None,
        })?;

    // Parse optional END clause: END(TIMESTAMP => ... | OFFSET => ...)
    let end_point = if parser.check(&Token::End) {
        parser.advance();
        parser.expect(&Token::LParen)?;
        let point = parse_time_travel_point(parser)?;
        parser.expect(&Token::RParen)?;
        Some(point)
    } else {
        None
    };

    Ok(Some(ChangesClause { information, at_or_before, end_point }))
}

/// Parse optional SAMPLE/TABLESAMPLE clause
fn parse_sample_clause(parser: &mut Parser) -> Result<Option<SampleClause>> {
    // SAMPLE [method] (size) [SEED (n)]
    // TABLESAMPLE [method] (size) [SEED (n)]
    let tablesample = if parser.consume(&Token::Tablesample) {
        true
    } else if parser.consume(&Token::Sample) {
        false
    } else {
        return Ok(None);
    };

    // Parse optional method: BERNOULLI, SYSTEM, BLOCK
    let method = if parser.consume(&Token::Bernoulli) {
        SampleMethod::Bernoulli
    } else if parser.consume(&Token::System) {
        SampleMethod::System
    } else if parser.consume(&Token::Block) {
        SampleMethod::Block
    } else {
        SampleMethod::Default
    };

    parser.expect(&Token::LParen)?;

    // Parse size - either a number (percent) or "n ROWS"
    let size = if let Token::IntegerLiteral(n) = parser.current().clone() {
        parser.advance();
        // Check for ROWS keyword
        if parser.consume(&Token::Rows) {
            SampleSize::Rows(n)
        } else {
            SampleSize::Percent(n as f64)
        }
    } else if let Token::FloatLiteral(n) = parser.current().clone() {
        parser.advance();
        SampleSize::Percent(n)
    } else {
        return Err(crate::Error::ParseError {
            message: "Expected sample size".to_string(),
            span: None,
        });
    };

    parser.expect(&Token::RParen)?;

    // Parse optional SEED or REPEATABLE clause
    let seed = if parser.consume(&Token::Seed) || parser.consume(&Token::Repeatable) {
        parser.expect(&Token::LParen)?;
        let seed_val = if let Token::IntegerLiteral(n) = parser.current().clone() {
            parser.advance();
            n
        } else {
            return Err(crate::Error::ParseError {
                message: "Expected seed value".to_string(),
                span: None,
            });
        };
        parser.expect(&Token::RParen)?;
        Some(seed_val)
    } else {
        None
    };

    Ok(Some(SampleClause { method, size, seed, tablesample }))
}

/// Parse optional CONNECT BY clause (hierarchical query)
/// START WITH condition CONNECT BY [NOCYCLE] condition [ORDER SIBLINGS BY ...]
/// or: CONNECT BY [NOCYCLE] condition [START WITH condition] [ORDER SIBLINGS BY ...]
fn parse_connect_by_clause(parser: &mut Parser) -> Result<Option<ConnectByClause>> {
    use crate::ast::ConnectByClause;

    // Try START WITH first
    let mut start_with = None;
    if parser.check(&Token::Start) {
        let pos = parser.position();
        parser.advance();
        if parser.consume(&Token::With) {
            start_with = Some(parse_expression(parser)?);
        } else {
            // Not START WITH, restore and continue
            parser.restore(pos);
        }
    }

    // CONNECT BY
    if !parser.consume(&Token::Connect) {
        // If we parsed START WITH but no CONNECT BY, that's an error
        if start_with.is_some() {
            return Err(crate::Error::ParseError {
                message: "START WITH requires CONNECT BY".to_string(),
                span: None,
            });
        }
        return Ok(None);
    }
    parser.expect(&Token::By)?;

    // Optional NOCYCLE
    let nocycle = if let Token::Identifier(name) = parser.current().clone() {
        if name.eq_ignore_ascii_case("nocycle") {
            parser.advance();
            true
        } else {
            false
        }
    } else {
        false
    };

    // CONNECT BY condition
    let connect_by_expr = parse_expression(parser)?;

    // Optional START WITH after CONNECT BY (if not already parsed)
    if start_with.is_none() && parser.check(&Token::Start) {
        let pos = parser.position();
        parser.advance();
        if parser.consume(&Token::With) {
            start_with = Some(parse_expression(parser)?);
        } else {
            parser.restore(pos);
        }
    }

    // Optional ORDER SIBLINGS BY
    let order_siblings_by = if parser.consume(&Token::Order) {
        parser.expect(&Token::Siblings)?;
        parser.expect(&Token::By)?;
        Some(parse_order_by_items(parser)?)
    } else {
        None
    };

    Ok(Some(ConnectByClause {
        start_with,
        connect_by: connect_by_expr,
        nocycle,
        order_siblings_by,
    }))
}

/// Parse optional PIVOT clause
fn parse_pivot_clause(parser: &mut Parser) -> Result<Option<PivotClause>> {
    // PIVOT (aggregate_func FOR column IN (value1, value2, ...))
    if !parser.consume(&Token::Pivot) {
        return Ok(None);
    }

    parser.expect(&Token::LParen)?;

    // Parse aggregate function(s)
    let mut aggregate_functions = Vec::new();
    loop {
        let agg_expr = parse_expression(parser)?;
        let alias = parse_optional_alias(parser);
        aggregate_functions.push((agg_expr, alias));

        if !parser.consume(&Token::Comma) {
            break;
        }
    }

    // FOR column
    parser.expect(&Token::For)?;
    let for_column = parse_qualified_identifier(parser)?;

    // IN (value1, value2, ...)
    parser.expect(&Token::In)?;
    parser.expect(&Token::LParen)?;

    let mut in_values = Vec::new();
    loop {
        let value_expr = parse_expression(parser)?;
        let alias = if parser.consume(&Token::As) {
            Some(parse_identifier(parser)?)
        } else {
            None
        };
        in_values.push((value_expr, alias));

        if !parser.consume(&Token::Comma) {
            break;
        }
    }

    parser.expect(&Token::RParen)?;  // Close IN list
    parser.expect(&Token::RParen)?;  // Close PIVOT

    // Parse optional alias after PIVOT (possibly with column aliases)
    let alias = parse_pivot_alias(parser)?;

    Ok(Some(PivotClause {
        aggregate_functions,
        for_column,
        in_values,
        alias,
    }))
}

/// Parse optional PIVOT alias with optional column aliases: [AS] name [(col1, col2, ...)]
fn parse_pivot_alias(parser: &mut Parser) -> Result<Option<PivotAlias>> {
    use crate::ast::PivotAlias;

    let explicit_as = parser.consume(&Token::As);
    let name = if explicit_as {
        parse_identifier(parser).ok()
    } else if matches!(parser.current(), Token::Identifier(_) | Token::QuotedIdentifier(_)) {
        if !is_keyword_that_ends_column(parser.current()) {
            parse_identifier(parser).ok()
        } else {
            None
        }
    } else {
        None
    };

    if let Some(name) = name {
        // Check for optional column aliases: (col1, col2, ...)
        let column_aliases = if parser.consume(&Token::LParen) {
            let mut aliases = Vec::new();
            loop {
                if let Ok(alias) = parse_identifier(parser) {
                    aliases.push(alias);
                }
                if !parser.consume(&Token::Comma) {
                    break;
                }
            }
            parser.expect(&Token::RParen)?;
            Some(aliases)
        } else {
            None
        };
        Ok(Some(PivotAlias { name, explicit_as, column_aliases }))
    } else {
        Ok(None)
    }
}

/// Parse optional UNPIVOT clause
fn parse_unpivot_clause(parser: &mut Parser) -> Result<Option<UnpivotClause>> {
    // UNPIVOT [INCLUDE NULLS] (value_column FOR name_column IN (col1, col2, ...))
    if !parser.consume(&Token::Unpivot) {
        return Ok(None);
    }

    // Optional INCLUDE NULLS
    let include_nulls = if parser.consume(&Token::Include) {
        parser.expect(&Token::Nulls)?;
        true
    } else {
        false
    };

    parser.expect(&Token::LParen)?;

    // value_column
    let value_column = parse_identifier(parser)?;

    // FOR name_column
    parser.expect(&Token::For)?;
    let name_column = parse_identifier(parser)?;

    // IN (col1, col2, ...)
    parser.expect(&Token::In)?;
    parser.expect(&Token::LParen)?;

    let mut columns = Vec::new();
    loop {
        columns.push(parse_identifier(parser)?);
        if !parser.consume(&Token::Comma) {
            break;
        }
    }

    parser.expect(&Token::RParen)?;  // Close IN list
    parser.expect(&Token::RParen)?;  // Close UNPIVOT

    // Parse optional alias after UNPIVOT
    let alias = parse_optional_alias(parser);

    Ok(Some(UnpivotClause {
        value_column,
        name_column,
        columns,
        include_nulls,
        alias,
    }))
}

/// Parse optional MATCH_RECOGNIZE clause
fn parse_match_recognize_clause(parser: &mut Parser) -> Result<Option<MatchRecognizeClause>> {
    // MATCH_RECOGNIZE (
    //   [PARTITION BY expr, ...]
    //   ORDER BY expr, ...
    //   [MEASURES expr AS name, ...]
    //   [ONE ROW PER MATCH | ALL ROWS PER MATCH]
    //   [AFTER MATCH SKIP ...]
    //   PATTERN (pattern)
    //   DEFINE name AS expr, ...
    // )
    if !parser.consume(&Token::MatchRecognize) {
        return Ok(None);
    }

    parser.expect(&Token::LParen)?;

    // Optional PARTITION BY
    let partition_by = if parser.consume(&Token::Partition) {
        parser.expect(&Token::By)?;
        let mut exprs = vec![parse_expression(parser)?];
        while parser.consume(&Token::Comma) {
            exprs.push(parse_expression(parser)?);
        }
        Some(exprs)
    } else {
        None
    };

    // ORDER BY (required in MATCH_RECOGNIZE)
    let order_by = if parser.consume(&Token::Order) {
        parser.expect(&Token::By)?;
        Some(parse_order_by_items(parser)?)
    } else {
        None
    };

    // Optional MEASURES
    let measures = if parser.consume(&Token::Measures) {
        let mut items = Vec::new();
        loop {
            let expr = parse_expression(parser)?;
            parser.expect(&Token::As)?;
            let name = parse_identifier(parser)?;
            items.push((expr, name));
            if !parser.consume(&Token::Comma) {
                break;
            }
        }
        items
    } else {
        Vec::new()
    };

    // Optional ONE ROW PER MATCH or ALL ROWS PER MATCH
    let rows_per_match = if parser.consume(&Token::One) {
        parser.expect(&Token::Row)?;
        parser.expect(&Token::Per)?;
        parser.expect(&Token::Match)?;
        Some(RowsPerMatch::OneRow)
    } else if parser.consume(&Token::All) {
        parser.expect(&Token::Rows)?;
        parser.expect(&Token::Per)?;
        parser.expect(&Token::Match)?;
        Some(RowsPerMatch::AllRows)
    } else {
        None
    };

    // Optional AFTER MATCH SKIP
    let after_match_skip = if parser.consume(&Token::After) {
        parser.expect(&Token::Match)?;
        parser.expect(&Token::Skip)?;

        // PAST LAST ROW, TO NEXT ROW, TO FIRST name, TO LAST name
        if parser.consume(&Token::Past) {
            parser.expect(&Token::Last)?;
            parser.expect(&Token::Row)?;
            Some(AfterMatchSkip::PastLastRow)
        } else if parser.check(&Token::Identifier("TO".to_string())) {
            parser.advance();
            if parser.consume(&Token::Identifier("NEXT".to_string())) {
                parser.expect(&Token::Row)?;
                Some(AfterMatchSkip::ToNextRow)
            } else if parser.consume(&Token::First) {
                let name = parse_identifier(parser)?;
                Some(AfterMatchSkip::ToFirst(name))
            } else if parser.consume(&Token::Last) {
                let name = parse_identifier(parser)?;
                Some(AfterMatchSkip::ToLast(name))
            } else {
                return Err(crate::Error::ParseError {
                    message: "Expected NEXT, FIRST, or LAST after SKIP TO".to_string(),
                    span: None,
                });
            }
        } else {
            return Err(crate::Error::ParseError {
                message: "Expected PAST or TO after SKIP".to_string(),
                span: None,
            });
        }
    } else {
        None
    };

    // PATTERN (pattern_string)
    parser.expect(&Token::Pattern)?;
    parser.expect(&Token::LParen)?;
    // Collect pattern tokens until closing paren
    let mut pattern = String::new();
    let mut paren_depth = 1;
    while paren_depth > 0 {
        match parser.current() {
            Token::LParen => {
                pattern.push('(');
                paren_depth += 1;
                parser.advance();
            }
            Token::RParen => {
                paren_depth -= 1;
                if paren_depth > 0 {
                    pattern.push(')');
                }
                parser.advance();
            }
            Token::Identifier(s) => {
                if !pattern.is_empty() && !pattern.ends_with('(') && !pattern.ends_with(' ') {
                    pattern.push(' ');
                }
                pattern.push_str(s);
                parser.advance();
            }
            Token::Star => {
                pattern.push('*');
                parser.advance();
            }
            Token::Plus => {
                pattern.push('+');
                parser.advance();
            }
            Token::Eof => {
                return Err(crate::Error::ParseError {
                    message: "Unexpected end of input in PATTERN".to_string(),
                    span: None,
                });
            }
            _ => {
                // Skip unexpected tokens or add them as-is
                parser.advance();
            }
        }
    }

    // DEFINE
    parser.expect(&Token::Define)?;
    let mut define = Vec::new();
    loop {
        let name = parse_identifier(parser)?;
        parser.expect(&Token::As)?;
        let expr = parse_expression(parser)?;
        define.push((name, expr));
        if !parser.consume(&Token::Comma) {
            break;
        }
    }

    parser.expect(&Token::RParen)?;

    Ok(Some(MatchRecognizeClause {
        partition_by,
        order_by,
        measures,
        rows_per_match,
        after_match_skip,
        pattern,
        define,
    }))
}

/// Parse a qualified identifier (potentially with dots)
fn parse_qualified_identifier(parser: &mut Parser) -> Result<String> {
    let mut ident = parse_identifier(parser)?;
    while parser.consume(&Token::Dot) {
        ident.push('.');
        ident.push_str(&parse_identifier(parser)?);
    }
    Ok(ident)
}

/// Parse qualified table name (schema.table)
fn parse_qualified_table_name(parser: &mut Parser) -> Result<String> {
    let mut name = parse_identifier(parser)?;

    while parser.consume(&Token::Dot) {
        let next = parse_identifier(parser)?;
        name = format!("{}.{}", name, next);
    }

    Ok(name)
}

/// Parse table function arguments (name => value pairs or positional)
fn parse_table_function_args(parser: &mut Parser) -> Result<Vec<(Option<String>, Expression)>> {
    let mut args = Vec::new();

    if parser.check(&Token::RParen) {
        return Ok(args);
    }

    loop {
        // Check for named argument (name => value)
        // Parameter names can be identifiers or keywords like OUTER, PATH, MODE, etc.
        let maybe_name = match parser.current().clone() {
            Token::Identifier(name) => Some(name),
            ref token => keyword_to_identifier(token),
        };

        let arg = if let Some(name) = maybe_name {
            let pos = parser.position();
            parser.advance();
            if parser.consume(&Token::FatArrow) {
                // Named argument
                let value = parse_expression(parser)?;
                (Some(name), value)
            } else {
                // Not a named argument, restore and parse as expression
                parser.restore(pos);
                let value = parse_expression(parser)?;
                (None, value)
            }
        } else {
            // Positional argument
            let value = parse_expression(parser)?;
            (None, value)
        };
        args.push(arg);

        if !parser.consume(&Token::Comma) {
            break;
        }
    }

    Ok(args)
}

/// Parse optional alias (with or without AS)
fn parse_optional_alias(parser: &mut Parser) -> Option<String> {
    if parser.consume(&Token::As) {
        parse_identifier(parser).ok()
    } else if matches!(parser.current(), Token::Identifier(_) | Token::QuotedIdentifier(_)) {
        if !is_keyword_that_ends_column(parser.current()) {
            parse_identifier(parser).ok()
        } else {
            None
        }
    } else {
        None
    }
}

/// Parse required alias (for subqueries), returns (alias, explicit_as)
fn parse_required_alias_with_as(parser: &mut Parser) -> Result<(String, bool)> {
    let explicit_as = parser.consume(&Token::As);
    let alias = parse_identifier(parser)?;
    Ok((alias, explicit_as))
}

/// Parse JOIN clauses
fn parse_join_clauses(parser: &mut Parser) -> Result<Vec<JoinClause>> {
    let mut joins = Vec::new();

    loop {
        let (join_type, explicit_inner, is_comma_join) = if parser.consume(&Token::Cross) {
            parser.expect(&Token::Join)?;
            (Some(JoinType::Cross), false, false)
        } else if parser.consume(&Token::Inner) {
            parser.expect(&Token::Join)?;
            (Some(JoinType::Inner), true, false)  // Explicit INNER
        } else if parser.consume(&Token::Left) {
            parser.consume(&Token::Outer);
            parser.expect(&Token::Join)?;
            (Some(JoinType::Left), false, false)
        } else if parser.consume(&Token::Right) {
            parser.consume(&Token::Outer);
            parser.expect(&Token::Join)?;
            (Some(JoinType::Right), false, false)
        } else if parser.consume(&Token::Full) {
            parser.consume(&Token::Outer);
            parser.expect(&Token::Join)?;
            (Some(JoinType::Full), false, false)
        } else if parser.consume(&Token::Join) {
            (Some(JoinType::Inner), false, false)  // Implicit INNER
        } else if parser.consume(&Token::Comma) {
            // Comma join (implicit cross join): FROM a, b
            (Some(JoinType::Cross), false, true)
        } else {
            (None, false, false)
        };

        if let Some(join_type) = join_type {
            let table = parse_table_reference(parser)?;

            // Comma joins and CROSS JOINs don't have ON clause
            let condition = if join_type != JoinType::Cross && !is_comma_join && parser.consume(&Token::On) {
                Some(parse_expression(parser)?)
            } else {
                None
            };

            joins.push(JoinClause {
                join_type,
                table,
                condition,
                explicit_inner,
                is_comma_join,
            });
        } else {
            break;
        }
    }

    Ok(joins)
}

/// Parse set operation (UNION, INTERSECT, EXCEPT)
fn parse_set_operation(parser: &mut Parser) -> Result<Option<Box<SetOperation>>> {
    let op_type = if parser.consume(&Token::Union) {
        Some(SetOperationType::Union)
    } else if parser.consume(&Token::Intersect) {
        Some(SetOperationType::Intersect)
    } else if parser.consume(&Token::Except) {
        Some(SetOperationType::Except)
    } else if parser.consume(&Token::MinusSet) {
        Some(SetOperationType::Minus)
    } else {
        None
    };

    if let Some(op_type) = op_type {
        let all = parser.consume(&Token::All);
        // The right side can be a SELECT statement or a parenthesized query
        let query = if parser.check(&Token::LParen) {
            parser.advance();
            let mut inner = parse_select_statement(parser)?;
            parser.expect(&Token::RParen)?;
            // After a parenthesized query, check if there are more set operations
            // These should chain at the end of the inner query's set operation chain
            if let Some(next_set_op) = parse_set_operation(parser)? {
                // Find the end of the set operation chain and append there
                fn append_set_op(stmt: &mut SelectStatement, op: Box<SetOperation>) {
                    if let Some(ref mut existing) = stmt.union {
                        append_set_op(&mut existing.query, op);
                    } else {
                        stmt.union = Some(op);
                    }
                }
                append_set_op(&mut inner, next_set_op);
            }
            inner
        } else {
            parse_select_statement(parser)?
        };
        Ok(Some(Box::new(SetOperation {
            op_type,
            all,
            query,
        })))
    } else {
        Ok(None)
    }
}

/// Parse INSERT statement
fn parse_insert_statement(parser: &mut Parser) -> Result<InsertStatement> {
    parser.expect(&Token::Insert)?;
    parser.expect(&Token::Into)?;

    let table = parse_qualified_table_name(parser)?;

    // Optional column list
    let columns = if parser.consume(&Token::LParen) {
        let mut cols = vec![parse_identifier(parser)?];
        while parser.consume(&Token::Comma) {
            cols.push(parse_identifier(parser)?);
        }
        parser.expect(&Token::RParen)?;
        Some(cols)
    } else {
        None
    };

    // VALUES or SELECT
    let source = if parser.consume(&Token::Values) {
        let mut rows = vec![parse_value_row(parser)?];
        while parser.consume(&Token::Comma) {
            rows.push(parse_value_row(parser)?);
        }
        InsertSource::Values(rows)
    } else if parser.check(&Token::Select) || parser.check(&Token::With) {
        InsertSource::Query(Box::new(parse_select_statement(parser)?))
    } else {
        return Err(crate::Error::ParseError {
            message: "Expected VALUES or SELECT".to_string(),
            span: None,
        });
    };

    Ok(InsertStatement {
        table,
        columns,
        source,
    })
}

/// Parse a single value row
fn parse_value_row(parser: &mut Parser) -> Result<Vec<Expression>> {
    parser.expect(&Token::LParen)?;
    let mut values = vec![parse_expression(parser)?];
    while parser.consume(&Token::Comma) {
        values.push(parse_expression(parser)?);
    }
    parser.expect(&Token::RParen)?;
    Ok(values)
}

/// Parse UPDATE statement
fn parse_update_statement(parser: &mut Parser) -> Result<UpdateStatement> {
    parser.expect(&Token::Update)?;

    let table = parse_qualified_table_name(parser)?;
    let alias = parse_optional_alias(parser);

    parser.expect(&Token::Set)?;

    let mut assignments = vec![parse_assignment(parser)?];
    while parser.consume(&Token::Comma) {
        assignments.push(parse_assignment(parser)?);
    }

    let from = if parser.consume(&Token::From) {
        Some(parse_from_clause(parser)?)
    } else {
        None
    };

    let where_clause = if parser.consume(&Token::Where) {
        Some(WhereClause {
            condition: parse_expression(parser)?,
        })
    } else {
        None
    };

    Ok(UpdateStatement {
        table,
        alias,
        assignments,
        from,
        where_clause,
    })
}

/// Parse column = expression assignment
fn parse_assignment(parser: &mut Parser) -> Result<(String, Expression)> {
    let column = parse_qualified_table_name(parser)?;  // Allow qualified names like t.name
    parser.expect(&Token::Eq)?;
    let value = parse_expression(parser)?;
    Ok((column, value))
}

/// Parse DELETE statement
fn parse_delete_statement(parser: &mut Parser) -> Result<DeleteStatement> {
    parser.expect(&Token::Delete)?;
    parser.expect(&Token::From)?;

    let table = parse_qualified_table_name(parser)?;
    let alias = parse_optional_alias(parser);

    let using = if parser.consume(&Token::Using) {
        Some(parse_from_clause(parser)?)
    } else {
        None
    };

    let where_clause = if parser.consume(&Token::Where) {
        Some(WhereClause {
            condition: parse_expression(parser)?,
        })
    } else {
        None
    };

    Ok(DeleteStatement {
        table,
        alias,
        using,
        where_clause,
    })
}

/// Parse MERGE statement
fn parse_merge_statement(parser: &mut Parser) -> Result<MergeStatement> {
    parser.expect(&Token::Merge)?;
    parser.expect(&Token::Into)?;

    let target = parse_qualified_table_name(parser)?;
    let target_alias = parse_optional_alias(parser);

    parser.expect(&Token::Using)?;
    let source = parse_table_reference(parser)?;

    parser.expect(&Token::On)?;
    let condition = parse_expression(parser)?;

    let mut clauses = Vec::new();

    // Parse WHEN MATCHED and WHEN NOT MATCHED clauses
    while parser.consume(&Token::When) {
        let matched = if parser.consume(&Token::Matched) {
            true
        } else if parser.consume(&Token::Not) {
            parser.expect(&Token::Matched)?;
            false
        } else {
            return Err(crate::Error::ParseError {
                message: "Expected MATCHED or NOT MATCHED".to_string(),
                span: None,
            });
        };

        // Optional AND condition
        let clause_condition = if parser.consume(&Token::And) {
            Some(parse_expression(parser)?)
        } else {
            None
        };

        parser.expect(&Token::Then)?;

        let action = if matched {
            if parser.consume(&Token::Update) {
                parser.expect(&Token::Set)?;
                let mut assignments = vec![parse_assignment(parser)?];
                while parser.consume(&Token::Comma) {
                    assignments.push(parse_assignment(parser)?);
                }
                MergeAction::Update(assignments)
            } else if parser.consume(&Token::Delete) {
                MergeAction::Delete
            } else {
                return Err(crate::Error::ParseError {
                    message: "Expected UPDATE or DELETE".to_string(),
                    span: None,
                });
            }
        } else {
            parser.expect(&Token::Insert)?;

            let columns = if parser.consume(&Token::LParen) {
                let mut cols = vec![parse_identifier(parser)?];
                while parser.consume(&Token::Comma) {
                    cols.push(parse_identifier(parser)?);
                }
                parser.expect(&Token::RParen)?;
                Some(cols)
            } else {
                None
            };

            parser.expect(&Token::Values)?;
            parser.expect(&Token::LParen)?;
            let mut values = vec![parse_expression(parser)?];
            while parser.consume(&Token::Comma) {
                values.push(parse_expression(parser)?);
            }
            parser.expect(&Token::RParen)?;

            MergeAction::Insert { columns, values }
        };

        if matched {
            clauses.push(MergeClause::WhenMatched {
                condition: clause_condition,
                action,
            });
        } else {
            clauses.push(MergeClause::WhenNotMatched {
                condition: clause_condition,
                action,
            });
        }
    }

    Ok(MergeStatement {
        target,
        target_alias,
        source,
        condition,
        clauses,
    })
}

/// Parse CREATE statement
fn parse_create_statement(parser: &mut Parser) -> Result<Statement> {
    parser.expect(&Token::Create)?;

    let or_replace = if parser.consume(&Token::Or) {
        parser.expect(&Token::Replace)?;
        true
    } else {
        false
    };

    if parser.consume(&Token::Table) {
        let if_not_exists = if parser.consume(&Token::If) {
            parser.expect(&Token::Not)?;
            parser.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        let name = parse_qualified_table_name(parser)?;

        // Check for CREATE TABLE ... CLONE source_table
        if parser.consume(&Token::Clone) {
            let source = parse_qualified_table_name(parser)?;
            return Ok(Statement::CreateTable(CreateTableStatement {
                if_not_exists,
                name,
                columns: Vec::new(),
                as_query: None,
                clone_source: Some(source),
            }));
        }

        // Check for CREATE TABLE AS SELECT
        if parser.consume(&Token::As) {
            let query = parse_select_statement(parser)?;
            return Ok(Statement::CreateTable(CreateTableStatement {
                if_not_exists,
                name,
                columns: Vec::new(),
                as_query: Some(Box::new(query)),
                clone_source: None,
            }));
        }

        // Column definitions
        parser.expect(&Token::LParen)?;
        let mut columns = vec![parse_column_definition(parser)?];
        while parser.consume(&Token::Comma) {
            columns.push(parse_column_definition(parser)?);
        }
        parser.expect(&Token::RParen)?;

        Ok(Statement::CreateTable(CreateTableStatement {
            if_not_exists,
            name,
            columns,
            as_query: None,
            clone_source: None,
        }))
    } else if parser.consume(&Token::View) {
        let name = parse_qualified_table_name(parser)?;

        // Optional column list
        let columns = if parser.consume(&Token::LParen) {
            let mut cols = vec![parse_identifier(parser)?];
            while parser.consume(&Token::Comma) {
                cols.push(parse_identifier(parser)?);
            }
            parser.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        // Optional COPY GRANTS
        let copy_grants = if parser.consume(&Token::Copy) {
            parser.expect(&Token::Grants)?;
            true
        } else {
            false
        };

        parser.expect(&Token::As)?;
        let query = parse_select_statement(parser)?;

        Ok(Statement::CreateView(CreateViewStatement {
            or_replace,
            name,
            columns,
            copy_grants,
            query: Box::new(query),
        }))
    } else {
        Err(crate::Error::ParseError {
            message: "Expected TABLE or VIEW after CREATE".to_string(),
            span: None,
        })
    }
}

/// Parse column definition
fn parse_column_definition(parser: &mut Parser) -> Result<ColumnDefinition> {
    let name = parse_identifier(parser)?;
    let data_type = parse_data_type(parser)?;

    let nullable = if parser.consume(&Token::Not) {
        parser.expect(&Token::Null)?;
        false
    } else {
        parser.consume(&Token::Null);
        true
    };

    let default = if parser.consume(&Token::Default) {
        Some(parse_expression(parser)?)
    } else {
        None
    };

    Ok(ColumnDefinition {
        name,
        data_type,
        nullable,
        default,
    })
}

/// Parse ALTER statement
fn parse_alter_statement(parser: &mut Parser) -> Result<Statement> {
    parser.expect(&Token::Alter)?;
    parser.expect(&Token::Table)?;

    let name = parse_qualified_table_name(parser)?;

    let action = if parser.consume(&Token::Identifier("ADD".to_string())) {
        // This is a workaround - "ADD" isn't a keyword but we need to handle it
        parser.restore(parser.position() - 1);
        // Check if current token is an identifier "ADD"
        if let Token::Identifier(s) = parser.current().clone() {
            if s.to_uppercase() == "ADD" {
                parser.advance();
                let col = parse_column_definition(parser)?;
                AlterTableAction::AddColumn(col)
            } else {
                return Err(crate::Error::ParseError {
                    message: "Expected ADD, DROP, or RENAME".to_string(),
                    span: None,
                });
            }
        } else {
            return Err(crate::Error::ParseError {
                message: "Expected ADD, DROP, or RENAME".to_string(),
                span: None,
            });
        }
    } else if parser.consume(&Token::Drop) {
        let column_name = parse_identifier(parser)?;
        AlterTableAction::DropColumn(column_name)
    } else {
        return Err(crate::Error::ParseError {
            message: "Expected ADD, DROP, or RENAME".to_string(),
            span: None,
        });
    };

    Ok(Statement::AlterTable(AlterTableStatement { name, action }))
}

/// Parse DROP statement
fn parse_drop_statement(parser: &mut Parser) -> Result<Statement> {
    parser.expect(&Token::Drop)?;

    if parser.consume(&Token::Table) {
        let if_exists = if parser.consume(&Token::If) {
            parser.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = parse_qualified_table_name(parser)?;
        Ok(Statement::DropTable(DropTableStatement { if_exists, name }))
    } else if parser.consume(&Token::View) {
        let if_exists = if parser.consume(&Token::If) {
            parser.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = parse_qualified_table_name(parser)?;
        Ok(Statement::DropView(DropViewStatement { if_exists, name }))
    } else {
        Err(crate::Error::ParseError {
            message: "Expected TABLE or VIEW after DROP".to_string(),
            span: None,
        })
    }
}

/// Parse identifier from current token
fn parse_identifier(parser: &mut Parser) -> Result<String> {
    match parser.current().clone() {
        Token::Identifier(name) => {
            parser.advance();
            Ok(name)
        }
        Token::QuotedIdentifier(name) => {
            parser.advance();
            Ok(name)
        }
        // Allow certain keywords as identifiers
        _ => {
            if let Some(name) = keyword_to_identifier(parser.current()) {
                parser.advance();
                Ok(name)
            } else {
                Err(crate::Error::ParseError {
                    message: format!("Expected identifier, found {:?}", parser.current()),
                    span: None,
                })
            }
        }
    }
}

/// Parse a window definition: name AS (window_spec)
fn parse_window_definition(parser: &mut Parser) -> Result<WindowDefinition> {
    let name = parse_identifier(parser)?;
    parser.expect(&Token::As)?;
    parser.expect(&Token::LParen)?;

    let mut partition_by = None;
    let mut order_by = None;
    let mut frame = None;

    // PARTITION BY
    if parser.consume(&Token::Partition) {
        parser.expect(&Token::By)?;
        let mut exprs = vec![parse_expression(parser)?];
        while parser.consume(&Token::Comma) {
            exprs.push(parse_expression(parser)?);
        }
        partition_by = Some(exprs);
    }

    // ORDER BY
    if parser.consume(&Token::Order) {
        parser.expect(&Token::By)?;
        order_by = Some(parse_order_by_items(parser)?);
    }

    // Window frame (ROWS, RANGE, GROUPS)
    let frame_unit = match parser.current() {
        Token::Rows => {
            parser.advance();
            Some(WindowFrameUnit::Rows)
        }
        Token::Range => {
            parser.advance();
            Some(WindowFrameUnit::Range)
        }
        Token::Groups => {
            parser.advance();
            Some(WindowFrameUnit::Groups)
        }
        _ => None,
    };

    if let Some(unit) = frame_unit {
        let has_between = parser.consume(&Token::Between);
        let start = parse_window_frame_bound(parser)?;
        let end = if has_between {
            parser.expect(&Token::And)?;
            Some(parse_window_frame_bound(parser)?)
        } else if parser.consume(&Token::And) {
            Some(parse_window_frame_bound(parser)?)
        } else {
            None
        };
        frame = Some(WindowFrame { unit, start, end });
    }

    parser.expect(&Token::RParen)?;

    Ok(WindowDefinition {
        name,
        spec: WindowSpec {
            partition_by,
            order_by,
            frame,
            window_name: None,
        },
    })
}

/// Parse window frame bound
fn parse_window_frame_bound(parser: &mut Parser) -> Result<WindowFrameBound> {
    if parser.consume(&Token::Current) {
        parser.expect(&Token::Row)?;
        return Ok(WindowFrameBound::CurrentRow);
    }

    if parser.consume(&Token::Unbounded) {
        if parser.consume(&Token::Preceding) {
            return Ok(WindowFrameBound::UnboundedPreceding);
        } else if parser.consume(&Token::Following) {
            return Ok(WindowFrameBound::UnboundedFollowing);
        }
    }

    // Check for INTERVAL bound (e.g., INTERVAL '1 hour' PRECEDING)
    if parser.consume(&Token::Interval) {
        if let Token::StringLiteral(value) = parser.current().clone() {
            parser.advance();
            if parser.consume(&Token::Preceding) {
                return Ok(WindowFrameBound::Preceding(FrameBoundValue::Interval {
                    value,
                    unit: String::new(),
                }));
            } else if parser.consume(&Token::Following) {
                return Ok(WindowFrameBound::Following(FrameBoundValue::Interval {
                    value,
                    unit: String::new(),
                }));
            }
        }
    }

    // Check for numeric bound
    if let Token::IntegerLiteral(n) = parser.current().clone() {
        parser.advance();
        if parser.consume(&Token::Preceding) {
            return Ok(WindowFrameBound::Preceding(FrameBoundValue::Numeric(n as u64)));
        } else if parser.consume(&Token::Following) {
            return Ok(WindowFrameBound::Following(FrameBoundValue::Numeric(n as u64)));
        }
    }

    Err(crate::Error::ParseError {
        message: format!("Expected window frame bound, found {:?}", parser.current()),
        span: None,
    })
}

/// Convert keyword token to identifier if allowed
fn keyword_to_identifier(token: &Token) -> Option<String> {
    match token {
        Token::Table => Some("table".to_string()),
        Token::View => Some("view".to_string()),
        Token::Date => Some("date".to_string()),
        Token::Timestamp => Some("timestamp".to_string()),
        Token::Values => Some("values".to_string()),
        Token::First => Some("first".to_string()),
        Token::Last => Some("last".to_string()),
        Token::Row => Some("row".to_string()),
        Token::Rows => Some("rows".to_string()),
        Token::Range => Some("range".to_string()),
        Token::Groups => Some("groups".to_string()),
        // FLATTEN-related parameter names
        Token::Outer => Some("outer".to_string()),
        Token::Inner => Some("inner".to_string()),
        Token::Left => Some("left".to_string()),
        Token::Right => Some("right".to_string()),
        Token::Full => Some("full".to_string()),
        Token::Cross => Some("cross".to_string()),
        Token::Recursive => Some("recursive".to_string()),
        Token::Partition => Some("partition".to_string()),
        Token::Order => Some("order".to_string()),
        Token::By => Some("by".to_string()),
        Token::Having => Some("having".to_string()),
        Token::Group => Some("group".to_string()),
        Token::Limit => Some("limit".to_string()),
        Token::Offset => Some("offset".to_string()),
        Token::Window => Some("window".to_string()),
        Token::Over => Some("over".to_string()),
        Token::Current => Some("current".to_string()),
        _ => None,
    }
}

/// Parse stage path after @
/// Returns (stage_name, optional_path)
/// Examples:
///   @stage_name -> ("stage_name", None)
///   @stage_name/path/to/file -> ("stage_name", Some("/path/to/file"))
///   @~ -> ("~", None)
///   @~/path -> ("~", Some("/path"))
///   @%table_name -> ("%table_name", None)
fn parse_stage_path(parser: &mut Parser) -> Result<(String, Option<String>)> {
    // Handle special prefixes: ~ for user stage, % for table stage
    let name = if let Token::Identifier(s) = parser.current().clone() {
        parser.advance();
        s
    } else if parser.consume(&Token::Percent) {
        // Table stage: @%table_name
        if let Token::Identifier(table_name) = parser.current().clone() {
            parser.advance();
            format!("%{}", table_name)
        } else {
            return Err(parser.error("Expected table name after @%"));
        }
    } else {
        // Could be ~ for user stage or some other identifier
        return Err(parser.error(&format!("Expected stage name after @, found {:?}", parser.current())));
    };

    // Check for path: /path/to/file
    if parser.check(&Token::Slash) {
        let mut path = String::new();
        while parser.consume(&Token::Slash) {
            path.push('/');
            // First part of segment: identifier
            if let Token::Identifier(s) = parser.current().clone() {
                path.push_str(&s);
                parser.advance();
            } else if matches!(parser.current(), Token::Star) {
                path.push('*');
                parser.advance();
            }
            // Continue with dots and extensions (e.g., .csv, .parquet)
            while parser.check(&Token::Dot) {
                parser.advance();
                path.push('.');
                match parser.current().clone() {
                    Token::Identifier(s) => {
                        path.push_str(&s);
                        parser.advance();
                    }
                    Token::Star => {
                        path.push('*');
                        parser.advance();
                    }
                    Token::IntegerLiteral(n) => {
                        path.push_str(&n.to_string());
                        parser.advance();
                    }
                    _ => {}
                }
            }
        }
        return Ok((name, Some(path)));
    }

    Ok((name, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::lexer::tokenize;

    fn parse_sql(input: &str) -> Result<Statement> {
        let tokens = tokenize(input)?;
        let mut parser = Parser::new(&tokens);
        parse_statement(&mut parser)
    }

    #[test]
    fn test_parse_simple_select() {
        let stmt = parse_sql("SELECT id, name FROM users").unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_select_with_where() {
        let stmt = parse_sql("SELECT * FROM users WHERE id = 1").unwrap();
        if let Statement::Select(s) = stmt {
            assert!(s.where_clause.is_some());
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_select_with_join() {
        let stmt = parse_sql("SELECT * FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        if let Statement::Select(s) = stmt {
            assert_eq!(s.joins.len(), 1);
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_with_clause() {
        let stmt = parse_sql("WITH cte AS (SELECT 1) SELECT * FROM cte").unwrap();
        if let Statement::Select(s) = stmt {
            assert!(s.with_clause.is_some());
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql("INSERT INTO users (id, name) VALUES (1, 'test')").unwrap();
        assert!(matches!(stmt, Statement::Insert(_)));
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse_sql("UPDATE users SET name = 'new' WHERE id = 1").unwrap();
        assert!(matches!(stmt, Statement::Update(_)));
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
        assert!(matches!(stmt, Statement::Delete(_)));
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap();
        assert!(matches!(stmt, Statement::CreateTable(_)));
    }
}
