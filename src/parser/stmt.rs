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
        _ => Err(crate::Error::ParseError {
            message: format!("Unexpected token: {:?}", parser.current()),
            span: None,
        }),
    }
}

/// Parse SELECT statement (may include WITH clause)
fn parse_with_statement(parser: &mut Parser) -> Result<Statement> {
    parser.expect(&Token::With)?;

    let mut ctes = Vec::new();

    loop {
        let cte = parse_cte(parser)?;
        ctes.push(cte);

        if !parser.consume(&Token::Comma) {
            break;
        }
    }

    // Now parse the main SELECT
    let mut select = parse_select_statement(parser)?;
    select.with_clause = Some(WithClause { ctes });

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
        where_clause,
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
            return Ok(TableReference::TableFunction { name: func_name, args });
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
        // Store as TableFunction with name "FLATTEN"
        return Ok(TableReference::TableFunction { name: "FLATTEN".to_string(), args });
    }

    // Handle subquery
    if parser.check(&Token::LParen) {
        parser.advance();
        if parser.check(&Token::Select) || parser.check(&Token::With) {
            let query = Box::new(parse_select_statement(parser)?);
            parser.expect(&Token::RParen)?;
            let (alias, explicit_as) = parse_required_alias_with_as(parser)?;
            return Ok(TableReference::Subquery { query, alias, explicit_as });
        } else {
            // Not a subquery, restore
            parser.restore(parser.position() - 1);
        }
    }

    // Table name
    let name = parse_qualified_table_name(parser)?;

    // Optional alias
    let alias = parse_optional_alias(parser);

    Ok(TableReference::Table {
        name,
        alias,
        time_travel: None,
        changes: None,
        sample: None,
        pivot: None,
        unpivot: None,
        match_recognize: None,
    })
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
        let arg = if let Token::Identifier(name) = parser.current().clone() {
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
    } else {
        None
    };

    if let Some(op_type) = op_type {
        let all = parser.consume(&Token::All);
        let query = parse_select_statement(parser)?;
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

        // Check for CREATE TABLE AS SELECT
        if parser.consume(&Token::As) {
            let query = parse_select_statement(parser)?;
            return Ok(Statement::CreateTable(CreateTableStatement {
                if_not_exists,
                name,
                columns: Vec::new(),
                as_query: Some(Box::new(query)),
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

        parser.expect(&Token::As)?;
        let query = parse_select_statement(parser)?;

        Ok(Statement::CreateView(CreateViewStatement {
            or_replace,
            name,
            columns,
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
        _ => None,
    }
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
