//! Error types for sqlflight

use miette::Diagnostic;
use thiserror::Error;

/// Result type alias for sqlflight operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for sqlflight
#[derive(Error, Diagnostic, Debug)]
pub enum Error {
    #[error("Parse error: {message}")]
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
