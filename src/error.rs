//! Error types for sqlflight

use miette::Diagnostic;
use thiserror::Error;

/// Result type alias for sqlflight operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for sqlflight
#[derive(Error, Diagnostic, Debug)]
pub enum Error {
    #[error("{message}")]
    #[diagnostic(code(sqlflight::parse_error))]
    ParseError {
        message: String,
        #[label("here")]
        span: Option<(usize, usize)>,
    },

    #[error("IO error: {0}")]
    #[diagnostic(code(sqlflight::io_error))]
    IoError(#[from] std::io::Error),

    #[error("Jinja error: {message}")]
    #[diagnostic(code(sqlflight::jinja_error))]
    JinjaError { message: String },

    #[error("Format error: {message}")]
    #[diagnostic(code(sqlflight::format_error))]
    FormatError { message: String },
}

/// Calculate line and column number from byte offset
pub fn offset_to_line_col(input: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut col = 1;
    for (i, c) in input.chars().enumerate() {
        if i >= offset {
            break;
        }
        if c == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

/// Get the line content at a given line number (1-indexed)
pub fn get_line_content(input: &str, line_num: usize) -> Option<&str> {
    input.lines().nth(line_num.saturating_sub(1))
}

/// Format a parse error with context
pub fn format_parse_error(input: &str, offset: usize, message: &str) -> String {
    let (line, col) = offset_to_line_col(input, offset);
    let line_content = get_line_content(input, line).unwrap_or("");

    // Create a pointer to the error position
    let pointer = " ".repeat(col.saturating_sub(1)) + "^";

    format!(
        "Parse error at line {}, column {}:\n  |\n{:>3} | {}\n  | {}\n  = {}",
        line, col, line, line_content, pointer, message
    )
}
