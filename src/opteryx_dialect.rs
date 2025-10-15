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
//
// ==================================================================================
// RECOMMENDED SQL LANGUAGE FEATURE ADDITIONS (Prioritized)
// ==================================================================================
//
// After reviewing the sqlparser-rs Dialect trait (v0.59.0) and analyzing Opteryx's
// current implementation, the following features are recommended for addition, 
// listed in priority order based on:
// 1. User value and common SQL use cases
// 2. Alignment with Opteryx's analytical query focus
// 3. Implementation complexity vs. benefit
// 4. Compatibility with existing Python execution engine capabilities
//
// CURRENT STATE:
// Opteryx already supports:
// - Basic SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT
// - JOIN operations (INNER, LEFT, RIGHT, CROSS)
// - Aggregation functions with FILTER clause
// - Array operations (@>, @>>)
// - SELECT * EXCEPT (column)
// - PartiQL-style subscripting (field['key'])
// - Numeric literals with underscores (10_000_000)
// - MATCH() AGAINST() for text search
// - Custom operators (DIV)
// - Set operations (UNION, INTERSECT, EXCEPT)
// - Subqueries in FROM clause
// - Common table expressions (WITH)
//
// PRIORITY 1: Window Functions with Named Window References
// ----------------------------------------------------------
// Feature: supports_window_clause_named_window_reference
// SQL Example:
//   SELECT *, ROW_NUMBER() OVER w1 
//   FROM table
//   WINDOW w1 AS (PARTITION BY category ORDER BY price)
//
// Rationale:
// - Critical for analytical queries (ranking, running totals, lag/lead)
// - Commonly used in business intelligence and reporting
// - Named windows improve query readability and reduce duplication
// - Opteryx's current code shows window function infrastructure exists but
//   named window references are not dialect-enabled
// - High user demand for analytical features
//
// Implementation Impact: MEDIUM
// - Parser already supports window functions
// - Need to enable dialect flag and test
// - May require minor planner updates
//
// PRIORITY 2: Lambda Functions (Higher-Order Functions)
// ------------------------------------------------------
// Feature: supports_lambda_functions
// SQL Example:
//   SELECT TRANSFORM(array_col, x -> x * 2) FROM table
//   SELECT FILTER(scores, s -> s > 70) FROM students
//
// Rationale:
// - Modern SQL feature available in BigQuery, Snowflake, DuckDB
// - Powerful for array/list transformations without UDFs
// - Aligns with Opteryx's support for arrays and complex types
// - Reduces need for complex procedural code
// - Enhances expressiveness for data transformations
//
// Implementation Impact: HIGH
// - Requires parser support (available in sqlparser-rs)
// - Needs lambda expression evaluation in Python execution engine
// - Would unlock powerful array manipulation capabilities
// - Consider starting with simple lambda functions on arrays
//
// PRIORITY 3: Dictionary/Map Literal Syntax
// ------------------------------------------
// Feature: supports_dictionary_syntax OR support_map_literal_syntax
// SQL Examples:
//   SELECT {'key': 'value', 'num': 123} AS config
//   SELECT Map {1: 'one', 2: 'two'} AS lookup
//
// Rationale:
// - Opteryx supports STRUCT types and complex data
// - Dictionary/map literals complement existing JSON/struct support
// - Common in modern analytical databases (BigQuery, Snowflake)
// - Useful for ad-hoc data structure creation
// - Aligns with PartiQL support already enabled
//
// Implementation Impact: MEDIUM
// - Parser support available in sqlparser-rs
// - Need to map to Python dict/map structures
// - Integrates with existing complex type handling
//
// PRIORITY 4: GROUP BY Expression Enhancements
// ---------------------------------------------
// Features: 
//   - supports_group_by_expr (ROLLUP, CUBE, GROUPING SETS)
//   - supports_order_by_all (ORDER BY ALL)
//
// SQL Examples:
//   SELECT region, product, SUM(sales)
//   FROM sales
//   GROUP BY ROLLUP(region, product)
//
//   SELECT * FROM table ORDER BY ALL
//
// Rationale:
// - ROLLUP/CUBE are standard OLAP operations
// - Useful for generating subtotals and cross-tabulations
// - ORDER BY ALL simplifies sorting entire result sets
// - Opteryx focuses on analytical queries - these are core features
// - Reduces complexity of multi-level aggregation queries
//
// Implementation Impact: MEDIUM-HIGH
// - Parser support exists
// - ROLLUP/CUBE require expansion of GROUP BY execution logic
// - ORDER BY ALL is simpler - just orders all columns
// - Both align well with Opteryx's aggregation capabilities
//
// PRIORITY 5: IN () Empty List Support
// -------------------------------------
// Feature: supports_in_empty_list
// SQL Example:
//   SELECT * FROM table WHERE column IN ()  -- Returns empty set
//
// Rationale:
// - Handles edge cases in dynamic query generation
// - Prevents query errors when parameter lists are empty
// - Common issue in programmatically generated SQL
// - Simple to implement with high practical value
// - Low risk, high convenience feature
//
// Implementation Impact: LOW
// - Minimal parser changes needed
// - Execution engine just returns empty result
// - Good candidate for quick win
//
// ==================================================================================
// FEATURES NOT RECOMMENDED (Opteryx has solid base without these):
// ==================================================================================
// - supports_connect_by: Hierarchical queries (niche use case)
// - supports_match_recognize: Pattern matching (very complex, niche)
// - supports_outer_join_operator: Oracle (+) syntax (legacy)
// - supports_execute_immediate: Dynamic SQL execution (security concerns)
// - supports_dollar_placeholder: $1, $2 style parameters (prefer named params)
// - Most dialect-specific syntaxes (Opteryx aims for portable SQL)
//
// CONCLUSION:
// Opteryx already has a strong SQL foundation covering core DML operations,
// joins, aggregations, and modern features like array operators and PartiQL.
// The five recommended additions above would significantly enhance analytical
// query capabilities while maintaining reasonable implementation complexity.
// Focus should be on Priority 1 (window functions) and Priority 2 (lambdas)
// as these provide the highest value for analytical workloads.

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

}
