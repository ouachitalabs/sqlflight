//! Printer tests for sqlflight
//!
//! Tests for the width-aware pretty printer.

use sqlflight::formatter::printer::{Printer, TARGET_WIDTH, INDENT_SIZE};

mod printer_basics {
    use super::*;

    #[test]
    fn new_printer_empty() {
        let printer = Printer::new();
        let output = printer.finish();
        assert!(output.is_empty());
    }

    #[test]
    fn write_text() {
        let mut printer = Printer::new();
        printer.write("hello");
        let output = printer.finish();
        assert_eq!(output, "hello");
    }

    #[test]
    fn write_multiple_texts() {
        let mut printer = Printer::new();
        printer.write("hello");
        printer.write(" ");
        printer.write("world");
        let output = printer.finish();
        assert_eq!(output, "hello world");
    }

    #[test]
    fn newline_adds_line_break() {
        let mut printer = Printer::new();
        printer.write("line1");
        printer.newline();
        printer.write("line2");
        let output = printer.finish();
        assert_eq!(output, "line1\nline2");
    }
}

mod printer_indentation {
    use super::*;

    #[test]
    fn indent_size_is_2() {
        assert_eq!(INDENT_SIZE, 2);
    }

    #[test]
    fn indent_increases_level() {
        let mut printer = Printer::new();
        printer.write("line1");
        printer.indent();
        printer.newline();
        printer.write("line2");
        let output = printer.finish();
        assert_eq!(output, "line1\n  line2");
    }

    #[test]
    fn dedent_decreases_level() {
        let mut printer = Printer::new();
        printer.indent();
        printer.indent();
        printer.newline();
        printer.write("deep");
        printer.dedent();
        printer.newline();
        printer.write("less deep");
        let output = printer.finish();
        assert!(output.contains("    deep"));
        assert!(output.contains("  less deep"));
    }

    #[test]
    fn multiple_indents() {
        let mut printer = Printer::new();
        printer.indent();
        printer.indent();
        printer.indent();
        printer.newline();
        printer.write("text");
        let output = printer.finish();
        // 3 levels * 2 spaces = 6 spaces
        assert!(output.contains("      text"));
    }

    #[test]
    fn dedent_at_zero_is_noop() {
        let mut printer = Printer::new();
        printer.dedent(); // Should not panic
        printer.newline();
        printer.write("text");
        let output = printer.finish();
        assert_eq!(output, "\ntext");
    }
}

mod printer_line_width {
    use super::*;

    #[test]
    fn target_width_is_120() {
        assert_eq!(TARGET_WIDTH, 120);
    }

    #[test]
    fn would_exceed_width_short() {
        let mut printer = Printer::new();
        printer.write("hello");
        assert!(!printer.would_exceed_width(" world"));
    }

    #[test]
    fn would_exceed_width_at_limit() {
        let mut printer = Printer::new();
        let text = "x".repeat(100);
        printer.write(&text);
        assert!(printer.would_exceed_width(&"y".repeat(25)));
    }

    #[test]
    fn would_exceed_width_after_newline() {
        let mut printer = Printer::new();
        let long_text = "x".repeat(100);
        printer.write(&long_text);
        printer.newline();
        // After newline, width resets
        assert!(!printer.would_exceed_width("short"));
    }

    #[test]
    fn width_includes_indent() {
        let mut printer = Printer::new();
        printer.indent();
        printer.indent();
        printer.newline();
        // After newline with 2 levels of indent, we're at column 4
        // A text of 117 chars should exceed 120
        assert!(printer.would_exceed_width(&"x".repeat(117)));
    }
}

mod printer_sql_formatting {
    use super::*;

    #[test]
    fn format_simple_select() {
        let mut printer = Printer::new();
        printer.write("select");
        printer.write(" ");
        printer.write("id");
        printer.write(" ");
        printer.write("from");
        printer.write(" ");
        printer.write("users");
        let output = printer.finish();
        assert_eq!(output, "select id from users");
    }

    #[test]
    fn format_select_with_columns() {
        let mut printer = Printer::new();
        printer.write("select");
        printer.indent();
        printer.newline();
        printer.write("id");
        printer.newline();
        printer.write(", name");
        printer.newline();
        printer.write(", email");
        printer.dedent();
        printer.newline();
        printer.write("from users");
        let output = printer.finish();

        assert!(output.contains("select\n"));
        assert!(output.contains("  id\n"));
        assert!(output.contains("  , name\n"));
        assert!(output.contains("  , email\n"));
        assert!(output.contains("from users"));
    }

    #[test]
    fn format_nested_subquery() {
        let mut printer = Printer::new();
        printer.write("select *");
        printer.newline();
        printer.write("from (");
        printer.indent();
        printer.newline();
        printer.write("select id");
        printer.newline();
        printer.write("from users");
        printer.dedent();
        printer.newline();
        printer.write(") sub");
        let output = printer.finish();

        assert!(output.contains("from (\n"));
        assert!(output.contains("  select id\n"));
        assert!(output.contains("  from users\n"));
        assert!(output.contains(") sub"));
    }
}

mod printer_edge_cases {
    use super::*;

    #[test]
    fn empty_string_write() {
        let mut printer = Printer::new();
        printer.write("");
        printer.write("text");
        printer.write("");
        let output = printer.finish();
        assert_eq!(output, "text");
    }

    #[test]
    fn consecutive_newlines() {
        let mut printer = Printer::new();
        printer.write("line1");
        printer.newline();
        printer.newline();
        printer.write("line3");
        let output = printer.finish();
        // Two newlines = blank line between
        assert!(output.contains("\n\n"));
    }

    #[test]
    fn newline_at_start() {
        let mut printer = Printer::new();
        printer.newline();
        printer.write("text");
        let output = printer.finish();
        assert!(output.starts_with('\n'));
    }

    #[test]
    fn special_characters() {
        let mut printer = Printer::new();
        printer.write("'string with spaces'");
        printer.write(" ");
        printer.write("-- comment");
        let output = printer.finish();
        assert_eq!(output, "'string with spaces' -- comment");
    }

    #[test]
    fn unicode_text() {
        let mut printer = Printer::new();
        printer.write("select café, naïve");
        let output = printer.finish();
        assert!(output.contains("café"));
        assert!(output.contains("naïve"));
    }
}
