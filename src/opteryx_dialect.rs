// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// This is a custom dialect for Opteryx using the DataFusion SQL parser as the engine.
// Opteryx originally used the MySQL dialect, but it has been modified to support 
// features available in other syntaxes.
//
// Extends:
//  https://github.com/apache/datafusion-sqlparser-rs/blob/main/src/dialect/mod.rs

use std::boxed::Box;

use sqlparser::ast::{BinaryOperator, Expr};
use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;
use sqlparser::parser::{ParserError};

/// A [`Dialect`] for [Opteryx](https://www.opteryx.dev/)
#[derive(Debug)]
pub struct OpteryxDialect {}

impl Dialect for OpteryxDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // Identifiers which begin with a digit are recognized while tokenizing numbers,
        // so they can be distinguished from exponent numeric literals.
        ch.is_alphabetic()
            || ch == '_'
            || ch == '$'
            || ch == '@'
            || ('\u{0080}'..='\u{ffff}').contains(&ch)
    }

    // [#2376] Opteryx identifiers can contain `-`
    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit() || ch == '-'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
    fn supports_string_literal_backslash_escape(&self) -> bool {
        false
    }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    // SELECT COUNT(*) FILTER (WHERE ID < 4)
    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn parse_infix(
        &self,
        parser: &mut sqlparser::parser::Parser,
        expr: &sqlparser::ast::Expr,
        _precedence: u8,
    ) -> Option<Result<sqlparser::ast::Expr, ParserError>> {
        // Parse DIV as an operator
        if parser.parse_keyword(Keyword::DIV) {
            Some(Ok(Expr::BinaryOp {
                left: Box::new(expr.clone()),
                op: BinaryOperator::MyIntegerDivide,
                right: Box::new(parser.parse_expr().unwrap()),
            }))
        // Parse `@>>` as "ArrayContainsAll"
        } else if parser.consume_token(&Token::AtArrow) {
            // we just consumed @>
            if parser.consume_token(&Token::Gt) {
                // Actually saw @>>
                return Some(Ok(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::Custom("ArrayContainsAll".to_string()), // your ALL operator
                    right: Box::new(parser.parse_expr().unwrap()),
                }));
            } else {
                // Just plain @>
                return Some(Ok(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::AtArrow,
                    right: Box::new(parser.parse_expr().unwrap()),
                }));
            }
        } else {
            None
        }
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    /// Returns true if the dialect supports an `EXCEPT` clause following a
    /// wildcard in a select list.
    ///
    /// For example
    /// ```sql
    /// SELECT * EXCEPT order_id FROM orders;
    /// ```
    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    /// Returns true if the dialect supports subscripting arrays (field['key'])
    fn supports_partiql(&self) -> bool {
        true
    }

    // Returns true if the dialect supports numbers containing underscores, e.g. `10_000_000`
    fn supports_numeric_literal_underscores(&self) -> bool {
        true
    }

    // Does the dialect support the `MATCH() AGAINST()` syntax?
    fn supports_match_against(&self) -> bool {
        true
    }

    /// Returns true if the dialect supports timestamp versioning for time-travel queries.
    /// This enables syntax like:
    /// - `SELECT * FROM table AT(TIMESTAMP => '2024-12-15 00:00:00')`
    /// - `SELECT * FROM table BEFORE(TIMESTAMP => '2024-12-15 00:00:00')`
    /// - `SELECT * FROM table FOR SYSTEM_TIME AS OF '2024-12-15 00:00:00'`
    fn supports_timestamp_versioning(&self) -> bool {
        true
    }

}
