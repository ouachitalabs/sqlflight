//! Width-aware pretty printer
//!
//! This module implements a pretty-printing algorithm that respects
//! the 120-character target line width.

/// Target line width
pub const TARGET_WIDTH: usize = 120;

/// Indentation size (2 spaces)
pub const INDENT_SIZE: usize = 2;

/// Pretty printer state
pub struct Printer {
    output: String,
    current_line_width: usize,
    indent_level: usize,
}

impl Printer {
    pub fn new() -> Self {
        Self {
            output: String::new(),
            current_line_width: 0,
            indent_level: 0,
        }
    }

    /// Write text to output
    pub fn write(&mut self, text: &str) {
        self.output.push_str(text);
        self.current_line_width += text.len();
    }

    /// Write a newline and indent
    pub fn newline(&mut self) {
        self.output.push('\n');
        let indent = " ".repeat(self.indent_level * INDENT_SIZE);
        self.output.push_str(&indent);
        self.current_line_width = indent.len();
    }

    /// Increase indentation level
    pub fn indent(&mut self) {
        self.indent_level += 1;
    }

    /// Decrease indentation level
    pub fn dedent(&mut self) {
        if self.indent_level > 0 {
            self.indent_level -= 1;
        }
    }

    /// Reset indent to 0 and return the previous level
    pub fn reset_indent(&mut self) -> usize {
        let saved = self.indent_level;
        self.indent_level = 0;
        saved
    }

    /// Restore indent to a saved level
    pub fn restore_indent(&mut self, level: usize) {
        self.indent_level = level;
    }

    /// Check if adding text would exceed target width
    pub fn would_exceed_width(&self, text: &str) -> bool {
        self.current_line_width + text.len() > TARGET_WIDTH
    }

    /// Get the output string
    pub fn finish(self) -> String {
        self.output
    }
}

impl Default for Printer {
    fn default() -> Self {
        Self::new()
    }
}
