//! Formatting rules
//!
//! This module defines the opinionated formatting rules for sqlflight:
//! - Keywords: lowercase
//! - Indentation: 2 spaces
//! - Line length: 120 characters target
//! - Commas: leading
//! - SELECT columns: threshold-based (3 or fewer inline, otherwise vertical)

/// Number of columns threshold for inline vs vertical SELECT formatting
pub const SELECT_COLUMN_THRESHOLD: usize = 3;

/// Convert keyword to lowercase
pub fn format_keyword(keyword: &str) -> String {
    keyword.to_lowercase()
}

/// Check if SELECT columns should be formatted inline
pub fn should_inline_columns(column_count: usize, total_width: usize) -> bool {
    column_count <= SELECT_COLUMN_THRESHOLD && total_width <= super::printer::TARGET_WIDTH
}
