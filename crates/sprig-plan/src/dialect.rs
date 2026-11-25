//! The MySQL dialect with the date and time forms both servers accept and
//! `sqlparser`'s MySQL dialect doesn't:
//!
//! - compound interval units: `INTERVAL '1:30' HOUR_MINUTE`,
//!   `EXTRACT(DAY_SECOND FROM d)`, `YEAR_MONTH`, `SECOND_MICROSECOND`, ...
//!   (an [`ast::DateTimeField::Custom`] holding the unit's name);
//! - the `INTERVAL(n, n1, n2, ...)` function, which is a call, not an
//!   interval.
//!
//! Everything else is [`MySqlDialect`]'s: the dialect reports itself as
//! that dialect, so the parser's MySQL rules all still apply.

use sqlparser::ast::{self, Expr};
use sqlparser::dialect::{Dialect, MySqlDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

/// [`MySqlDialect`] with MySQL's compound interval units and `INTERVAL()`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SprigMySql;

const M: MySqlDialect = MySqlDialect {};

/// The units an `INTERVAL` or `EXTRACT` may name that `sqlparser` has no
/// keyword for.
const COMPOUND: &[&str] = &[
    "SECOND_MICROSECOND",
    "MINUTE_MICROSECOND",
    "MINUTE_SECOND",
    "HOUR_MICROSECOND",
    "HOUR_SECOND",
    "HOUR_MINUTE",
    "DAY_MICROSECOND",
    "DAY_SECOND",
    "DAY_MINUTE",
    "DAY_HOUR",
    "YEAR_MONTH",
];

/// Whether `w` is one of the compound units.
#[must_use]
pub fn compound_unit(w: &str) -> bool {
    COMPOUND.iter().any(|c| c.eq_ignore_ascii_case(w))
}

fn interval(parser: &mut Parser<'_>) -> Result<Expr, ParserError> {
    let value = parser.parse_expr()?;
    let field = match &parser.peek_token_ref().token {
        Token::Word(w) if w.quote_style.is_none() && compound_unit(&w.value) => {
            let name = w.value.to_ascii_uppercase();
            parser.advance_token();
            ast::DateTimeField::Custom(ast::Ident::new(name))
        }
        // `INTERVAL(n, n1, ...)`: a call.
        _ if matches!(value, Expr::Tuple(_)) => {
            let Expr::Tuple(items) = value else {
                unreachable!("matched above")
            };
            return Ok(call("INTERVAL", items));
        }
        _ => parser.parse_date_time_field()?,
    };
    Ok(Expr::Interval(ast::Interval {
        value: Box::new(value),
        leading_field: Some(field),
        leading_precision: None,
        last_field: None,
        fractional_seconds_precision: None,
    }))
}

/// `EXPLAIN EXTENDED <statement>` (MariaDB's, and what the MySQL port
/// sends for a plain `EXPLAIN`): the plain `EXPLAIN` with `verbose` set,
/// which asks for the `filtered` column. `None` (nothing consumed)
/// otherwise.
fn explain_extended(parser: &mut Parser<'_>) -> Option<Result<ast::Statement, ParserError>> {
    let word = |t: &Token, w: &str| matches!(t, Token::Word(x) if x.quote_style.is_none() && x.value.eq_ignore_ascii_case(w));
    if !word(&parser.peek_nth_token(0).token, "EXPLAIN")
        || !word(&parser.peek_nth_token(1).token, "EXTENDED")
    {
        return None;
    }
    parser.advance_token();
    parser.advance_token();
    Some(
        parser
            .parse_statement()
            .map(|statement| ast::Statement::Explain {
                describe_alias: ast::DescribeAlias::Explain,
                analyze: false,
                verbose: true,
                query_plan: false,
                estimate: false,
                statement: Box::new(statement),
                format: None,
                options: None,
            }),
    )
}

/// A plain call of `name` with `args`.
fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(ast::Function {
        name: ast::ObjectName::from(vec![ast::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

/// `NEXT VALUE FOR name` / `PREVIOUS VALUE FOR name` at the parser's
/// place, as a call of `NEXTVAL` / `LASTVAL`; `None` (nothing consumed)
/// otherwise.
fn value_for(parser: &mut Parser<'_>) -> Option<Result<Expr, ParserError>> {
    let word = |t: &sqlparser::tokenizer::Token, w: &str| matches!(t, sqlparser::tokenizer::Token::Word(x) if x.quote_style.is_none() && x.value.eq_ignore_ascii_case(w));
    let first_tok = parser.peek_nth_token(0);
    let start = first_tok.span.start;
    let first = first_tok.token;
    let func = if word(&first, "NEXT") {
        "NEXTVAL"
    } else if word(&first, "PREVIOUS") {
        "LASTVAL"
    } else {
        return None;
    };
    if !word(&parser.peek_nth_token(1).token, "VALUE")
        || !word(&parser.peek_nth_token(2).token, "FOR")
    {
        return None;
    }
    for _ in 0..3 {
        parser.advance_token();
    }
    Some(parser.parse_object_name(false).map(|name| {
        let parts: Vec<ast::Ident> = name
            .0
            .into_iter()
            .filter_map(|p| match p {
                ast::ObjectNamePart::Identifier(i) => Some(i),
                ast::ObjectNamePart::Function(_) => None,
            })
            .collect();
        let end = parts.last().map_or(start, |i| i.span.end);
        let arg = if parts.len() == 1 {
            Expr::Identifier(
                parts
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| ast::Ident::new("")),
            )
        } else {
            Expr::CompoundIdentifier(parts)
        };
        let mut f = call(func, vec![arg]);
        // The call covers the words as written (its column's name).
        if let Expr::Function(x) = &mut f
            && let Some(ast::ObjectNamePart::Identifier(i)) = x.name.0.first_mut()
        {
            i.span = sqlparser::tokenizer::Span::new(start, end);
        }
        f
    }))
}

impl Dialect for SprigMySql {
    fn dialect(&self) -> std::any::TypeId {
        std::any::TypeId::of::<MySqlDialect>()
    }

    fn parse_prefix(&self, parser: &mut Parser<'_>) -> Option<Result<Expr, ParserError>> {
        if parser.peek_keyword(Keyword::INTERVAL) {
            parser.advance_token();
            return Some(interval(parser));
        }
        // MariaDB's `NEXT VALUE FOR s` and `PREVIOUS VALUE FOR s`: calls of
        // `NEXTVAL(s)` and `LASTVAL(s)`.
        if let Some(f) = value_for(parser) {
            return Some(f);
        }
        None
    }

    fn allow_extract_custom(&self) -> bool {
        true
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        M.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        M.is_identifier_part(ch)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        M.is_delimited_identifier_start(ch)
    }
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        M.identifier_quote_style(identifier)
    }
    fn supports_string_literal_backslash_escape(&self) -> bool {
        M.supports_string_literal_backslash_escape()
    }
    fn supports_string_literal_concatenation(&self) -> bool {
        M.supports_string_literal_concatenation()
    }
    fn ignores_wildcard_escapes(&self) -> bool {
        M.ignores_wildcard_escapes()
    }
    fn supports_numeric_prefix(&self) -> bool {
        M.supports_numeric_prefix()
    }
    fn supports_bitwise_shift_operators(&self) -> bool {
        M.supports_bitwise_shift_operators()
    }
    fn supports_multiline_comment_hints(&self) -> bool {
        M.supports_multiline_comment_hints()
    }
    fn parse_infix(
        &self,
        parser: &mut Parser<'_>,
        expr: &Expr,
        precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        M.parse_infix(parser, expr, precedence)
    }
    fn parse_statement(
        &self,
        parser: &mut Parser<'_>,
    ) -> Option<Result<ast::Statement, ParserError>> {
        if let Some(r) = explain_extended(parser) {
            return Some(r);
        }
        M.parse_statement(parser)
    }
    fn require_interval_qualifier(&self) -> bool {
        M.require_interval_qualifier()
    }
    fn supports_limit_comma(&self) -> bool {
        M.supports_limit_comma()
    }
    fn supports_create_table_select(&self) -> bool {
        M.supports_create_table_select()
    }
    fn supports_insert_set(&self) -> bool {
        M.supports_insert_set()
    }
    fn supports_user_host_grantee(&self) -> bool {
        M.supports_user_host_grantee()
    }
    fn is_table_factor_alias(&self, explicit: bool, kw: &Keyword, parser: &mut Parser<'_>) -> bool {
        M.is_table_factor_alias(explicit, kw, parser)
    }
    fn supports_table_hints(&self) -> bool {
        M.supports_table_hints()
    }
    fn requires_single_line_comment_whitespace(&self) -> bool {
        M.requires_single_line_comment_whitespace()
    }
    fn supports_match_against(&self) -> bool {
        M.supports_match_against()
    }
    fn supports_select_modifiers(&self) -> bool {
        M.supports_select_modifiers()
    }
    fn supports_set_names(&self) -> bool {
        M.supports_set_names()
    }
    fn supports_comma_separated_set_assignments(&self) -> bool {
        M.supports_comma_separated_set_assignments()
    }
    fn supports_update_order_by(&self) -> bool {
        M.supports_update_order_by()
    }
    fn supports_data_type_signed_suffix(&self) -> bool {
        M.supports_data_type_signed_suffix()
    }
    fn supports_cross_join_constraint(&self) -> bool {
        M.supports_cross_join_constraint()
    }
    fn supports_double_ampersand_operator(&self) -> bool {
        M.supports_double_ampersand_operator()
    }
    fn supports_binary_kw_as_cast(&self) -> bool {
        M.supports_binary_kw_as_cast()
    }
    fn supports_comment_optimizer_hint(&self) -> bool {
        M.supports_comment_optimizer_hint()
    }
    fn supports_constraint_keyword_without_name(&self) -> bool {
        M.supports_constraint_keyword_without_name()
    }
    fn supports_key_column_option(&self) -> bool {
        M.supports_key_column_option()
    }
    fn supports_group_by_with_modifier(&self) -> bool {
        M.supports_group_by_with_modifier()
    }
    fn supports_left_associative_joins_without_parens(&self) -> bool {
        M.supports_left_associative_joins_without_parens()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(sql: &str) -> String {
        Parser::parse_sql(&SprigMySql, sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .pop()
            .map(|s| s.to_string())
            .unwrap_or_default()
    }

    #[test]
    fn compound_units_and_the_interval_call_parse() {
        assert!(one("SELECT d + INTERVAL '1:30' HOUR_MINUTE FROM t").contains("HOUR_MINUTE"));
        assert!(one("SELECT EXTRACT(YEAR_MONTH FROM d) FROM t").contains("YEAR_MONTH"));
        assert!(one("SELECT DATE_ADD(d, INTERVAL 1 + 1 DAY) FROM t").contains("INTERVAL"));
        assert!(one("SELECT INTERVAL(23, 1, 15, 17, 30)").contains("INTERVAL(23"));
        assert!(one("SELECT INTERVAL (1) DAY + d FROM t").contains("DAY"));
        // MySQL's own rules still apply.
        assert!(one("SELECT 7 DIV 2").contains("DIV"));
        assert!(one("SELECT a FROM t LIMIT 1, 2").contains("LIMIT"));
    }

    #[test]
    fn explain_extended_is_a_verbose_explain() {
        let v =
            Parser::parse_sql(&SprigMySql, "EXPLAIN EXTENDED SELECT a FROM t WHERE b = 1").unwrap();
        assert!(matches!(
            &v[..],
            [ast::Statement::Explain { verbose: true, analyze: false, statement, .. }]
                if matches!(**statement, ast::Statement::Query(_))
        ));
        assert!(matches!(
            &Parser::parse_sql(&SprigMySql, "EXPLAIN SELECT 1").unwrap()[..],
            [ast::Statement::Explain { verbose: false, .. }]
        ));
    }
}
