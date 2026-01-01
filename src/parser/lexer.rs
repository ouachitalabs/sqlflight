//! SQL tokenization
//!
//! This module handles breaking SQL input into tokens.

use winnow::ascii::multispace0;
use winnow::combinator::{alt, delimited};
use winnow::error::{ContextError, ErrMode, ModalResult};
use winnow::stream::AsChar;
use winnow::token::{literal, one_of, take_till, take_until, take_while};
use winnow::prelude::*;

/// Token types for SQL lexer
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    In,
    Is,
    Null,
    Like,
    ILike,
    Between,
    Case,
    When,
    Then,
    Else,
    End,
    As,
    On,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Outer,
    With,
    Union,
    Intersect,
    Except,
    All,
    Distinct,
    Recursive,
    Group,
    By,
    Having,
    Order,
    Asc,
    Desc,
    Nulls,
    First,
    Last,
    Limit,
    Offset,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Merge,
    Using,
    Matched,
    Create,
    Table,
    View,
    Alter,
    Drop,
    If,
    Exists,
    Replace,
    True,
    False,
    Cast,
    TryCast,
    Over,
    Partition,
    Rows,
    Range,
    Unbounded,
    Preceding,
    Following,
    Current,
    Row,
    Qualify,
    Flatten,
    Lateral,
    Pivot,
    Unpivot,
    Sample,
    Tablesample,
    Window,
    Within,
    Groups,
    Extract,
    Date,
    Timestamp,
    Interval,
    For,
    Include,
    Copy,
    Grants,
    Clone,
    At,
    Before,
    Statement,
    Changes,
    Information,
    Bernoulli,
    System,
    Block,
    Seed,
    MatchRecognize,
    Measures,
    Define,
    Pattern,
    After,
    Match,
    Skip,
    Past,
    One,
    Per,

    // Additional keywords
    Default,

    // Identifiers and literals
    Identifier(String),
    QuotedIdentifier(String),
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),

    // Operators
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Concat, // ||

    // Punctuation
    Comma,
    Dot,
    Semicolon,
    Colon,
    DoubleColon, // ::
    LParen,
    RParen,
    LBracket,
    RBracket,
    FatArrow, // =>
    AtSign,   // @ for stage references
    Dollar,   // $ for positional column references

    // Jinja
    JinjaExpression(String), // {{ ... }}
    JinjaStatement(String),  // {% ... %}
    JinjaComment(String),    // {# ... #}

    // Comments
    SingleLineComment(String), // -- ...
    MultiLineComment(String),  // /* ... */

    // Special
    Eof,
}

/// A span in the source code
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Span { start, end }
    }
}

/// Token with position information
#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Span,
}

/// Tokenize SQL input
/// Comment with position information
#[derive(Debug, Clone, PartialEq)]
pub struct CommentToken {
    pub text: String,
    pub line: usize,
    pub is_block: bool,  // true for /* */, false for --
}

/// Tokenize result with comments extracted
pub struct TokenizeResult {
    pub tokens: Vec<Token>,
    pub spanned_tokens: Vec<SpannedToken>,
    pub comments: Vec<CommentToken>,
}

pub fn tokenize(input: &str) -> crate::Result<Vec<Token>> {
    let result = tokenize_with_comments(input)?;
    Ok(result.tokens)
}

pub fn tokenize_with_comments(input: &str) -> crate::Result<TokenizeResult> {
    let mut remaining = input;
    let mut tokens = Vec::new();
    let mut spanned_tokens = Vec::new();
    let mut comments = Vec::new();
    let mut current_line = 1;

    while !remaining.is_empty() {
        // Track line numbers through whitespace
        let trimmed = remaining.trim_start();
        let whitespace = &remaining[..remaining.len() - trimmed.len()];
        current_line += whitespace.chars().filter(|c| *c == '\n').count();

        if trimmed.is_empty() {
            break;
        }
        remaining = trimmed;

        // Record position before parsing
        let start_pos = input.len() - remaining.len();

        // Try to parse a token
        match parse_token(&mut remaining) {
            Ok(token) => {
                let end_pos = input.len() - remaining.len();
                let span = Span::new(start_pos, end_pos);

                // Extract comments and store with line info
                match &token {
                    Token::SingleLineComment(text) => {
                        comments.push(CommentToken {
                            text: format!("--{}", text),
                            line: current_line,
                            is_block: false,
                        });
                        // Count newlines in the comment text
                        current_line += text.chars().filter(|c| *c == '\n').count();
                    }
                    Token::MultiLineComment(text) => {
                        comments.push(CommentToken {
                            text: format!("/*{}*/", text),
                            line: current_line,
                            is_block: true,
                        });
                        // Count newlines in the comment text
                        current_line += text.chars().filter(|c| *c == '\n').count();
                    }
                    Token::JinjaComment(text) => {
                        comments.push(CommentToken {
                            text: format!("{{# {} #}}", text.trim()),
                            line: current_line,
                            is_block: true,
                        });
                        current_line += text.chars().filter(|c| *c == '\n').count();
                    }
                    _ => {
                        // Don't add comment tokens to the token stream
                        tokens.push(token.clone());
                        spanned_tokens.push(SpannedToken { token, span });
                    }
                }
            }
            Err(e) => {
                // Check if it's a Cut error - these are always fatal
                let is_cut = matches!(e, ErrMode::Cut(_));

                // For backtrack errors where we've consumed all input (e.g., from EOF)
                // we can break. But for Cut errors, we always fail.
                if remaining.is_empty() && !is_cut {
                    break;
                }

                let offset = input.len() - remaining.len();
                let context_char = remaining.chars().next().unwrap_or(' ');
                let context_preview: String = remaining.chars().take(20).collect();
                let message = crate::error::format_parse_error(
                    input,
                    offset,
                    &format!("Unexpected character '{}' near: {}...", context_char, context_preview),
                );
                return Err(crate::Error::ParseError {
                    message,
                    span: Some((offset, offset + 1)),
                });
            }
        }
    }

    let eof_pos = input.len();
    tokens.push(Token::Eof);
    spanned_tokens.push(SpannedToken {
        token: Token::Eof,
        span: Span::new(eof_pos, eof_pos),
    });
    Ok(TokenizeResult { tokens, spanned_tokens, comments })
}

fn parse_token<'s>(input: &mut &'s str) -> ModalResult<Token> {
    // Skip leading whitespace
    multispace0.parse_next(input)?;

    if input.is_empty() {
        return Ok(Token::Eof);
    }

    alt((
        parse_jinja_expression,
        parse_jinja_statement,
        parse_jinja_comment,
        parse_single_line_comment,
        parse_multi_line_comment,
        parse_string_literal,
        parse_quoted_identifier,
        parse_number,
        parse_multi_char_operator,
        parse_single_char_operator,
        parse_keyword_or_identifier,
    ))
    .parse_next(input)
}

fn parse_jinja_expression<'s>(input: &mut &'s str) -> ModalResult<Token> {
    let content = delimited("{{", take_until(0.., "}}"), "}}").parse_next(input)?;
    Ok(Token::JinjaExpression(content.to_string()))
}

fn parse_jinja_statement<'s>(input: &mut &'s str) -> ModalResult<Token> {
    // Check for whitespace control variants first
    if input.starts_with("{%-") {
        *input = &input[3..];
        let content = parse_until_jinja_statement_end(input)?;
        return Ok(Token::JinjaStatement(format!("-{}", content)));
    }

    if !input.starts_with("{%") {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    *input = &input[2..];
    let content = parse_until_jinja_statement_end(input)?;
    Ok(Token::JinjaStatement(content.to_string()))
}

fn parse_until_jinja_statement_end(input: &mut &str) -> ModalResult<String> {
    let mut content = String::new();

    loop {
        if input.is_empty() {
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }

        if input.starts_with("-%}") {
            *input = &input[3..];
            content.push('-'); // Preserve the whitespace control marker
            break;
        }

        if input.starts_with("%}") {
            *input = &input[2..];
            break;
        }

        let c = input.chars().next().unwrap();
        content.push(c);
        *input = &input[c.len_utf8()..];
    }

    Ok(content.trim_end_matches('-').to_string())
}

fn parse_jinja_comment<'s>(input: &mut &'s str) -> ModalResult<Token> {
    let content = delimited("{#", take_until(0.., "#}"), "#}").parse_next(input)?;
    Ok(Token::JinjaComment(content.to_string()))
}

fn parse_single_line_comment<'s>(input: &mut &'s str) -> ModalResult<Token> {
    let _ = literal("--").parse_next(input)?;
    let content = take_till(0.., |c| c == '\n' || c == '\r').parse_next(input)?;
    Ok(Token::SingleLineComment(content.to_string()))
}

fn parse_multi_line_comment<'s>(input: &mut &'s str) -> ModalResult<Token> {
    let _ = literal("/*").parse_next(input)?;

    // Handle nested comments - count depth
    let mut depth = 1;
    let mut content = String::new();

    while depth > 0 && !input.is_empty() {
        if input.starts_with("/*") {
            depth += 1;
            content.push_str("/*");
            *input = &input[2..];
        } else if input.starts_with("*/") {
            depth -= 1;
            if depth > 0 {
                content.push_str("*/");
            }
            *input = &input[2..];
        } else {
            let c = input.chars().next().unwrap();
            content.push(c);
            *input = &input[c.len_utf8()..];
        }
    }

    if depth > 0 {
        // Use Cut to indicate this is an unrecoverable error - comment started but not closed
        return Err(ErrMode::Cut(ContextError::new()));
    }

    Ok(Token::MultiLineComment(content))
}

fn parse_string_literal<'s>(input: &mut &'s str) -> ModalResult<Token> {
    let _ = literal("'").parse_next(input)?;
    let mut content = String::new();

    loop {
        if input.is_empty() {
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }

        if input.starts_with("''") {
            // Escaped quote
            content.push('\'');
            *input = &input[2..];
        } else if input.starts_with('\'') {
            *input = &input[1..];
            break;
        } else {
            let c = input.chars().next().unwrap();
            content.push(c);
            *input = &input[c.len_utf8()..];
        }
    }

    Ok(Token::StringLiteral(content))
}

fn parse_quoted_identifier<'s>(input: &mut &'s str) -> ModalResult<Token> {
    let _ = literal("\"").parse_next(input)?;
    let content = take_until(0.., "\"").parse_next(input)?;
    let _ = literal("\"").parse_next(input)?;
    Ok(Token::QuotedIdentifier(content.to_string()))
}

fn parse_number<'s>(input: &mut &'s str) -> ModalResult<Token> {
    // Check for a digit or a dot followed by digit
    let first = input.chars().next();
    if !matches!(first, Some(c) if c.is_ascii_digit() || c == '.') {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    // Also don't match if it's just a dot not followed by a digit
    if first == Some('.') {
        let second = input.chars().nth(1);
        if !matches!(second, Some(c) if c.is_ascii_digit()) {
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }
    }

    let start = *input;

    // Parse integer part
    let int_part = take_while(0.., |c: char| c.is_ascii_digit()).parse_next(input)?;

    // Check for decimal point
    let has_decimal = if !input.is_empty() && input.starts_with('.') {
        // Make sure it's not just a dot (which could be a separator)
        if input.len() > 1 && input.chars().nth(1).map(|c| c.is_ascii_digit()).unwrap_or(false) {
            *input = &input[1..];
            let _frac_part = take_while(1.., |c: char| c.is_ascii_digit()).parse_next(input)?;
            true
        } else if int_part.is_empty() {
            // It's a lone dot, backtrack
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        } else {
            false
        }
    } else {
        false
    };

    // Check for exponent
    let has_exponent = if !input.is_empty() && (input.starts_with('e') || input.starts_with('E')) {
        *input = &input[1..];
        // Optional sign
        if !input.is_empty() && (input.starts_with('+') || input.starts_with('-')) {
            *input = &input[1..];
        }
        let exp_digits = take_while(1.., |c: char| c.is_ascii_digit()).parse_next(input)?;
        !exp_digits.is_empty()
    } else {
        false
    };

    let num_str = &start[..(start.len() - input.len())];

    if has_decimal || has_exponent {
        let f: f64 = num_str
            .parse()
            .map_err(|_| winnow::error::ErrMode::Backtrack(ContextError::new()))?;
        Ok(Token::FloatLiteral(f))
    } else {
        let i: i64 = num_str
            .parse()
            .map_err(|_| winnow::error::ErrMode::Backtrack(ContextError::new()))?;
        Ok(Token::IntegerLiteral(i))
    }
}

fn parse_multi_char_operator<'s>(input: &mut &'s str) -> ModalResult<Token> {
    alt((
        literal("=>").map(|_| Token::FatArrow),
        literal("::").map(|_| Token::DoubleColon),
        literal("||").map(|_| Token::Concat),
        literal("<>").map(|_| Token::NotEq),
        literal("!=").map(|_| Token::NotEq),
        literal("<=").map(|_| Token::LtEq),
        literal(">=").map(|_| Token::GtEq),
    ))
    .parse_next(input)
}

fn parse_single_char_operator<'s>(input: &mut &'s str) -> ModalResult<Token> {
    alt((
        literal("+").map(|_| Token::Plus),
        literal("-").map(|_| Token::Minus),
        literal("*").map(|_| Token::Star),
        literal("/").map(|_| Token::Slash),
        literal("%").map(|_| Token::Percent),
        literal("=").map(|_| Token::Eq),
        literal("<").map(|_| Token::Lt),
        literal(">").map(|_| Token::Gt),
        literal(",").map(|_| Token::Comma),
        literal(".").map(|_| Token::Dot),
        literal(";").map(|_| Token::Semicolon),
        literal(":").map(|_| Token::Colon),
        literal("(").map(|_| Token::LParen),
        literal(")").map(|_| Token::RParen),
        literal("[").map(|_| Token::LBracket),
        literal("]").map(|_| Token::RBracket),
        literal("@").map(|_| Token::AtSign),
        literal("$").map(|_| Token::Dollar),
    ))
    .parse_next(input)
}

fn parse_keyword_or_identifier<'s>(input: &mut &'s str) -> ModalResult<Token> {
    // Parse identifier-like token (starts with letter or underscore)
    let start_char = one_of(|c: char| c.is_alpha() || c == '_').parse_next(input)?;
    let rest = take_while(0.., |c: char| c.is_alphanum() || c == '_').parse_next(input)?;

    let ident = format!("{}{}", start_char, rest);
    let upper = ident.to_uppercase();

    // Check if it's a keyword
    let token = match upper.as_str() {
        "SELECT" => Token::Select,
        "FROM" => Token::From,
        "WHERE" => Token::Where,
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "IN" => Token::In,
        "IS" => Token::Is,
        "NULL" => Token::Null,
        "LIKE" => Token::Like,
        "ILIKE" => Token::ILike,
        "BETWEEN" => Token::Between,
        "CASE" => Token::Case,
        "WHEN" => Token::When,
        "THEN" => Token::Then,
        "ELSE" => Token::Else,
        "END" => Token::End,
        "AS" => Token::As,
        "ON" => Token::On,
        "JOIN" => Token::Join,
        "INNER" => Token::Inner,
        "LEFT" => Token::Left,
        "RIGHT" => Token::Right,
        "FULL" => Token::Full,
        "CROSS" => Token::Cross,
        "OUTER" => Token::Outer,
        "WITH" => Token::With,
        "UNION" => Token::Union,
        "INTERSECT" => Token::Intersect,
        "EXCEPT" => Token::Except,
        "ALL" => Token::All,
        "DISTINCT" => Token::Distinct,
        "RECURSIVE" => Token::Recursive,
        "GROUP" => Token::Group,
        "BY" => Token::By,
        "HAVING" => Token::Having,
        "ORDER" => Token::Order,
        "ASC" => Token::Asc,
        "DESC" => Token::Desc,
        "NULLS" => Token::Nulls,
        "FIRST" => Token::First,
        "LAST" => Token::Last,
        "LIMIT" => Token::Limit,
        "OFFSET" => Token::Offset,
        "INSERT" => Token::Insert,
        "INTO" => Token::Into,
        "VALUES" => Token::Values,
        "UPDATE" => Token::Update,
        "SET" => Token::Set,
        "DELETE" => Token::Delete,
        "MERGE" => Token::Merge,
        "USING" => Token::Using,
        "MATCHED" => Token::Matched,
        "CREATE" => Token::Create,
        "TABLE" => Token::Table,
        "VIEW" => Token::View,
        "ALTER" => Token::Alter,
        "DROP" => Token::Drop,
        "IF" => Token::If,
        "EXISTS" => Token::Exists,
        "REPLACE" => Token::Replace,
        "TRUE" => Token::True,
        "FALSE" => Token::False,
        "CAST" => Token::Cast,
        "TRY_CAST" => Token::TryCast,
        "OVER" => Token::Over,
        "PARTITION" => Token::Partition,
        "ROWS" => Token::Rows,
        "RANGE" => Token::Range,
        "UNBOUNDED" => Token::Unbounded,
        "PRECEDING" => Token::Preceding,
        "FOLLOWING" => Token::Following,
        "CURRENT" => Token::Current,
        "ROW" => Token::Row,
        "QUALIFY" => Token::Qualify,
        "FLATTEN" => Token::Flatten,
        "LATERAL" => Token::Lateral,
        "PIVOT" => Token::Pivot,
        "UNPIVOT" => Token::Unpivot,
        "SAMPLE" => Token::Sample,
        "TABLESAMPLE" => Token::Tablesample,
        "WINDOW" => Token::Window,
        "WITHIN" => Token::Within,
        "GROUPS" => Token::Groups,
        "EXTRACT" => Token::Extract,
        "DATE" => Token::Date,
        "TIMESTAMP" => Token::Timestamp,
        "INTERVAL" => Token::Interval,
        "DEFAULT" => Token::Default,
        "FOR" => Token::For,
        "INCLUDE" => Token::Include,
        "COPY" => Token::Copy,
        "GRANTS" => Token::Grants,
        "CLONE" => Token::Clone,
        "AT" => Token::At,
        "BEFORE" => Token::Before,
        "STATEMENT" => Token::Statement,
        "CHANGES" => Token::Changes,
        "INFORMATION" => Token::Information,
        "BERNOULLI" => Token::Bernoulli,
        "SYSTEM" => Token::System,
        "BLOCK" => Token::Block,
        "SEED" => Token::Seed,
        "MATCH_RECOGNIZE" => Token::MatchRecognize,
        "MEASURES" => Token::Measures,
        "DEFINE" => Token::Define,
        "PATTERN" => Token::Pattern,
        "AFTER" => Token::After,
        "MATCH" => Token::Match,
        "SKIP" => Token::Skip,
        "PAST" => Token::Past,
        "ONE" => Token::One,
        "PER" => Token::Per,
        _ => Token::Identifier(ident),
    };

    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_select() {
        let tokens = tokenize("SELECT").unwrap();
        assert!(matches!(&tokens[0], Token::Select));
    }

    #[test]
    fn test_tokenize_identifier() {
        let tokens = tokenize("users").unwrap();
        assert!(matches!(&tokens[0], Token::Identifier(s) if s == "users"));
    }

    #[test]
    fn test_tokenize_string() {
        let tokens = tokenize("'hello'").unwrap();
        assert!(matches!(&tokens[0], Token::StringLiteral(s) if s == "hello"));
    }

    #[test]
    fn test_tokenize_escaped_quote() {
        let tokens = tokenize("'it''s'").unwrap();
        assert!(matches!(&tokens[0], Token::StringLiteral(s) if s == "it's"));
    }

    #[test]
    fn test_tokenize_integer() {
        let tokens = tokenize("42").unwrap();
        assert!(matches!(&tokens[0], Token::IntegerLiteral(42)));
    }

    #[test]
    fn test_tokenize_float() {
        let tokens = tokenize("3.14").unwrap();
        assert!(matches!(&tokens[0], Token::FloatLiteral(f) if (*f - 3.14).abs() < 0.001));
    }

    #[test]
    fn test_tokenize_jinja_expression() {
        let tokens = tokenize("{{ variable }}").unwrap();
        assert!(matches!(&tokens[0], Token::JinjaExpression(s) if s.contains("variable")));
    }
}
