//! Lexer/Tokenizer tests for sqlflight
//!
//! Tests for the SQL tokenization layer.

use sqlflight::parser::lexer::{tokenize, Token};

mod tokenize_keywords {
    use super::*;

    #[test]
    fn tokenize_select() {
        let tokens = tokenize("SELECT").expect("should tokenize");
        assert!(tokens.iter().any(|t| matches!(t, Token::Select)));
    }

    #[test]
    fn tokenize_from() {
        let tokens = tokenize("FROM").expect("should tokenize");
        assert!(tokens.iter().any(|t| matches!(t, Token::From)));
    }

    #[test]
    fn tokenize_where() {
        let tokens = tokenize("WHERE").expect("should tokenize");
        assert!(tokens.iter().any(|t| matches!(t, Token::Where)));
    }

    #[test]
    fn tokenize_keywords_case_insensitive() {
        let tokens1 = tokenize("SELECT").expect("uppercase");
        let tokens2 = tokenize("select").expect("lowercase");
        let tokens3 = tokenize("SeLeCt").expect("mixed case");

        // All should produce Select token
        assert!(tokens1.iter().any(|t| matches!(t, Token::Select)));
        assert!(tokens2.iter().any(|t| matches!(t, Token::Select)));
        assert!(tokens3.iter().any(|t| matches!(t, Token::Select)));
    }

    #[test]
    fn tokenize_all_sql_keywords() {
        let keywords = vec![
            ("AND", Token::And),
            ("OR", Token::Or),
            ("NOT", Token::Not),
            ("IN", Token::In),
            ("IS", Token::Is),
            ("NULL", Token::Null),
            ("LIKE", Token::Like),
            ("ILIKE", Token::ILike),
            ("BETWEEN", Token::Between),
            ("CASE", Token::Case),
            ("WHEN", Token::When),
            ("THEN", Token::Then),
            ("ELSE", Token::Else),
            ("END", Token::End),
            ("AS", Token::As),
            ("ON", Token::On),
            ("JOIN", Token::Join),
            ("INNER", Token::Inner),
            ("LEFT", Token::Left),
            ("RIGHT", Token::Right),
            ("FULL", Token::Full),
            ("CROSS", Token::Cross),
            ("OUTER", Token::Outer),
            ("WITH", Token::With),
            ("UNION", Token::Union),
            ("INTERSECT", Token::Intersect),
            ("EXCEPT", Token::Except),
            ("ALL", Token::All),
            ("DISTINCT", Token::Distinct),
            ("GROUP", Token::Group),
            ("BY", Token::By),
            ("HAVING", Token::Having),
            ("ORDER", Token::Order),
            ("ASC", Token::Asc),
            ("DESC", Token::Desc),
            ("NULLS", Token::Nulls),
            ("FIRST", Token::First),
            ("LAST", Token::Last),
            ("LIMIT", Token::Limit),
            ("OFFSET", Token::Offset),
            ("INSERT", Token::Insert),
            ("INTO", Token::Into),
            ("VALUES", Token::Values),
            ("UPDATE", Token::Update),
            ("SET", Token::Set),
            ("DELETE", Token::Delete),
            ("CREATE", Token::Create),
            ("TABLE", Token::Table),
            ("VIEW", Token::View),
            ("ALTER", Token::Alter),
            ("DROP", Token::Drop),
            ("IF", Token::If),
            ("EXISTS", Token::Exists),
            ("REPLACE", Token::Replace),
            ("TRUE", Token::True),
            ("FALSE", Token::False),
            ("CAST", Token::Cast),
            ("OVER", Token::Over),
            ("PARTITION", Token::Partition),
            ("ROWS", Token::Rows),
            ("RANGE", Token::Range),
            ("QUALIFY", Token::Qualify),
            ("FLATTEN", Token::Flatten),
            ("LATERAL", Token::Lateral),
            ("PIVOT", Token::Pivot),
            ("UNPIVOT", Token::Unpivot),
            ("SAMPLE", Token::Sample),
        ];

        for (keyword_str, expected_token) in keywords {
            let tokens = tokenize(keyword_str).expect(&format!("should tokenize {}", keyword_str));
            assert!(
                tokens.iter().any(|t| *t == expected_token),
                "Failed to tokenize {} correctly",
                keyword_str
            );
        }
    }
}

mod tokenize_identifiers {
    use super::*;

    #[test]
    fn tokenize_simple_identifier() {
        let tokens = tokenize("users").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Identifier(s) if s == "users"));
    }

    #[test]
    fn tokenize_identifier_with_underscore() {
        let tokens = tokenize("user_name").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Identifier(s) if s == "user_name"));
    }

    #[test]
    fn tokenize_identifier_with_numbers() {
        let tokens = tokenize("user123").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Identifier(s) if s == "user123"));
    }

    #[test]
    fn tokenize_quoted_identifier() {
        let tokens = tokenize(r#""User Name""#).expect("should tokenize");
        assert!(matches!(&tokens[0], Token::QuotedIdentifier(s) if s == "User Name"));
    }

    #[test]
    fn tokenize_quoted_identifier_with_spaces() {
        let tokens = tokenize(r#""Column With Spaces""#).expect("should tokenize");
        assert!(matches!(&tokens[0], Token::QuotedIdentifier(s) if s == "Column With Spaces"));
    }
}

mod tokenize_literals {
    use super::*;

    #[test]
    fn tokenize_integer() {
        let tokens = tokenize("42").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::IntegerLiteral(42)));
    }

    #[test]
    fn tokenize_negative_integer() {
        let _tokens = tokenize("-42").expect("should tokenize");
        // Could be Minus + IntegerLiteral or just IntegerLiteral(-42)
        // depending on lexer design
    }

    #[test]
    fn tokenize_float() {
        let tokens = tokenize("3.14").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::FloatLiteral(f) if (*f - 3.14).abs() < 0.001));
    }

    #[test]
    fn tokenize_scientific_notation() {
        let tokens = tokenize("1.5e10").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::FloatLiteral(_)));
    }

    #[test]
    fn tokenize_string_literal() {
        let tokens = tokenize("'hello world'").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::StringLiteral(s) if s == "hello world"));
    }

    #[test]
    fn tokenize_string_with_escaped_quote() {
        let tokens = tokenize("'it''s a test'").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::StringLiteral(s) if s == "it's a test"));
    }

    #[test]
    fn tokenize_empty_string() {
        let tokens = tokenize("''").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::StringLiteral(s) if s.is_empty()));
    }
}

mod tokenize_operators {
    use super::*;

    #[test]
    fn tokenize_plus() {
        let tokens = tokenize("+").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Plus));
    }

    #[test]
    fn tokenize_minus() {
        let tokens = tokenize("-").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Minus));
    }

    #[test]
    fn tokenize_star() {
        let tokens = tokenize("*").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Star));
    }

    #[test]
    fn tokenize_slash() {
        let tokens = tokenize("/").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Slash));
    }

    #[test]
    fn tokenize_percent() {
        let tokens = tokenize("%").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Percent));
    }

    #[test]
    fn tokenize_equals() {
        let tokens = tokenize("=").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Eq));
    }

    #[test]
    fn tokenize_not_equals() {
        let tokens = tokenize("<>").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::NotEq));
    }

    #[test]
    fn tokenize_not_equals_bang() {
        let tokens = tokenize("!=").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::NotEq));
    }

    #[test]
    fn tokenize_less_than() {
        let tokens = tokenize("<").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Lt));
    }

    #[test]
    fn tokenize_less_than_or_equal() {
        let tokens = tokenize("<=").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::LtEq));
    }

    #[test]
    fn tokenize_greater_than() {
        let tokens = tokenize(">").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Gt));
    }

    #[test]
    fn tokenize_greater_than_or_equal() {
        let tokens = tokenize(">=").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::GtEq));
    }

    #[test]
    fn tokenize_concat() {
        let tokens = tokenize("||").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Concat));
    }

    #[test]
    fn tokenize_double_colon() {
        let tokens = tokenize("::").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::DoubleColon));
    }
}

mod tokenize_punctuation {
    use super::*;

    #[test]
    fn tokenize_comma() {
        let tokens = tokenize(",").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Comma));
    }

    #[test]
    fn tokenize_dot() {
        let tokens = tokenize(".").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Dot));
    }

    #[test]
    fn tokenize_semicolon() {
        let tokens = tokenize(";").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Semicolon));
    }

    #[test]
    fn tokenize_colon() {
        let tokens = tokenize(":").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::Colon));
    }

    #[test]
    fn tokenize_left_paren() {
        let tokens = tokenize("(").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::LParen));
    }

    #[test]
    fn tokenize_right_paren() {
        let tokens = tokenize(")").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::RParen));
    }

    #[test]
    fn tokenize_left_bracket() {
        let tokens = tokenize("[").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::LBracket));
    }

    #[test]
    fn tokenize_right_bracket() {
        let tokens = tokenize("]").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::RBracket));
    }
}

mod tokenize_comments {
    use sqlflight::parser::lexer::tokenize_with_comments;

    #[test]
    fn tokenize_single_line_comment() {
        // Comments are extracted to a separate comments vector, not returned in tokens
        let result = tokenize_with_comments("-- this is a comment").expect("should tokenize");
        assert_eq!(result.comments.len(), 1);
        assert!(result.comments[0].text.contains("this is a comment"));
        assert!(!result.comments[0].is_block);
    }

    #[test]
    fn tokenize_multi_line_comment() {
        let result = tokenize_with_comments("/* this is\na comment */").expect("should tokenize");
        assert_eq!(result.comments.len(), 1);
        assert!(result.comments[0].is_block);
    }

    #[test]
    fn tokenize_nested_comment() {
        // Snowflake supports nested comments
        let result = tokenize_with_comments("/* outer /* inner */ outer */").expect("should tokenize");
        assert_eq!(result.comments.len(), 1);
        assert!(result.comments[0].is_block);
    }
}

mod tokenize_jinja {
    use super::*;

    #[test]
    fn tokenize_jinja_expression() {
        let tokens = tokenize("{{ variable }}").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::JinjaExpression(s) if s.contains("variable")));
    }

    #[test]
    fn tokenize_jinja_statement() {
        let tokens = tokenize("{% if condition %}").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::JinjaStatement(s) if s.contains("if condition")));
    }

    #[test]
    fn tokenize_jinja_comment() {
        // Jinja comments are extracted to the comments vector, not returned in tokens
        use sqlflight::parser::lexer::tokenize_with_comments;
        let result = tokenize_with_comments("{# a comment #}").expect("should tokenize");
        assert_eq!(result.comments.len(), 1);
        assert!(result.comments[0].text.contains("a comment"));
    }

    #[test]
    fn tokenize_jinja_ref() {
        let tokens = tokenize("{{ ref('users') }}").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::JinjaExpression(s) if s.contains("ref")));
    }

    #[test]
    fn tokenize_jinja_source() {
        let tokens = tokenize("{{ source('raw', 'users') }}").expect("should tokenize");
        assert!(matches!(&tokens[0], Token::JinjaExpression(s) if s.contains("source")));
    }
}

mod tokenize_full_statements {
    use super::*;

    #[test]
    fn tokenize_simple_select() {
        let tokens = tokenize("SELECT id, name FROM users").expect("should tokenize");

        assert!(matches!(&tokens[0], Token::Select));
        assert!(matches!(&tokens[1], Token::Identifier(s) if s == "id"));
        assert!(matches!(&tokens[2], Token::Comma));
        assert!(matches!(&tokens[3], Token::Identifier(s) if s == "name"));
        assert!(matches!(&tokens[4], Token::From));
        assert!(matches!(&tokens[5], Token::Identifier(s) if s == "users"));
    }

    #[test]
    fn tokenize_select_with_where() {
        let tokens = tokenize("SELECT * FROM users WHERE id = 1").expect("should tokenize");

        assert!(tokens.iter().any(|t| matches!(t, Token::Select)));
        assert!(tokens.iter().any(|t| matches!(t, Token::Star)));
        assert!(tokens.iter().any(|t| matches!(t, Token::From)));
        assert!(tokens.iter().any(|t| matches!(t, Token::Where)));
        assert!(tokens.iter().any(|t| matches!(t, Token::Eq)));
        assert!(tokens.iter().any(|t| matches!(t, Token::IntegerLiteral(1))));
    }

    #[test]
    fn tokenize_select_with_join() {
        let tokens = tokenize("SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
            .expect("should tokenize");

        assert!(tokens.iter().any(|t| matches!(t, Token::Join)));
        assert!(tokens.iter().any(|t| matches!(t, Token::On)));
    }

    #[test]
    fn tokenize_mixed_jinja_and_sql() {
        let tokens = tokenize("SELECT * FROM {{ ref('users') }} WHERE active = true")
            .expect("should tokenize");

        assert!(tokens.iter().any(|t| matches!(t, Token::Select)));
        assert!(tokens.iter().any(|t| matches!(t, Token::JinjaExpression(_))));
        assert!(tokens.iter().any(|t| matches!(t, Token::Where)));
    }
}

mod tokenize_whitespace {
    use super::*;

    #[test]
    fn tokenize_ignores_spaces() {
        let tokens = tokenize("SELECT   id   FROM   users").expect("should tokenize");
        // Should have same tokens regardless of spacing
        assert_eq!(tokens.iter().filter(|t| !matches!(t, Token::Eof)).count(), 4);
    }

    #[test]
    fn tokenize_ignores_newlines() {
        let tokens = tokenize("SELECT\nid\nFROM\nusers").expect("should tokenize");
        assert_eq!(tokens.iter().filter(|t| !matches!(t, Token::Eof)).count(), 4);
    }

    #[test]
    fn tokenize_ignores_tabs() {
        let tokens = tokenize("SELECT\tid\tFROM\tusers").expect("should tokenize");
        assert_eq!(tokens.iter().filter(|t| !matches!(t, Token::Eof)).count(), 4);
    }
}

mod tokenize_errors {
    use super::*;

    #[test]
    fn tokenize_unclosed_string() {
        let result = tokenize("'unclosed string");
        assert!(result.is_err());
    }

    #[test]
    fn tokenize_unclosed_comment() {
        let result = tokenize("/* unclosed comment");
        assert!(result.is_err());
    }

    #[test]
    fn tokenize_unclosed_jinja_expression() {
        let result = tokenize("{{ unclosed");
        assert!(result.is_err());
    }

    #[test]
    fn tokenize_unclosed_jinja_statement() {
        let result = tokenize("{% unclosed");
        assert!(result.is_err());
    }

    #[test]
    fn tokenize_invalid_character() {
        // Some characters might be invalid in SQL
        // Depends on lexer implementation
    }
}
