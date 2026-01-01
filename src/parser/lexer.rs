//! SQL tokenization
//!
//! This module handles breaking SQL input into tokens.

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
    Concat,  // ||

    // Punctuation
    Comma,
    Dot,
    Semicolon,
    Colon,
    DoubleColon,  // ::
    LParen,
    RParen,
    LBracket,
    RBracket,

    // Jinja
    JinjaExpression(String),  // {{ ... }}
    JinjaStatement(String),   // {% ... %}
    JinjaComment(String),     // {# ... #}

    // Comments
    SingleLineComment(String),  // -- ...
    MultiLineComment(String),   // /* ... */

    // Special
    Eof,
}

/// Tokenize SQL input
pub fn tokenize(_input: &str) -> crate::Result<Vec<Token>> {
    // TODO: Implement tokenizer
    Err(crate::Error::ParseError {
        message: "Tokenizer not implemented".to_string(),
        span: None,
    })
}
