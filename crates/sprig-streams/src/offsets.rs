//! `CALL sprig.commit_offset(...)`: recognising the statement and
//! splitting its arguments, for the gateway's commit of a consumer group's
//! offset on the engine's stream records.

/// A `CALL sprig.commit_offset(...)` statement: its argument texts, split
/// at top-level commas. `None` for any other statement.
#[must_use]
pub fn parse_call(sql: &str) -> Option<Vec<String>> {
    let t = sql.trim().trim_end_matches(';').trim();
    let lower = t.to_ascii_lowercase();
    let rest = lower.strip_prefix("call")?;
    if !rest.starts_with(|c: char| c.is_whitespace()) {
        return None;
    }
    let off = t.len() - rest.len();
    let rest_t = t[off..].trim_start();
    let name_end = rest_t.find('(')?;
    let name: String = rest_t[..name_end]
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '`')
        .collect::<String>()
        .to_ascii_lowercase();
    if name != "sprig.commit_offset" {
        return None;
    }
    let args = rest_t[name_end + 1..].trim_end();
    let args = args.strip_suffix(')')?;
    Some(split_args(args))
}

/// Splits at commas outside parentheses, quotes and backticks.
fn split_args(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    let mut quote_ch: Option<char> = None;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if let Some(q) = quote_ch {
            cur.push(c);
            if c == '\\' && q != '`' {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            } else if c == q {
                if chars.peek() == Some(&q) {
                    cur.push(q);
                    chars.next();
                } else {
                    quote_ch = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                quote_ch = Some(c);
                cur.push(c);
            }
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => out.push(std::mem::take(&mut cur).trim().to_owned()),
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() || !out.is_empty() {
        out.push(cur.trim().to_owned());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_calls() {
        assert_eq!(
            parse_call("CALL sprig.commit_offset('billing', 'order_events', 3, 1205);"),
            Some(vec![
                "'billing'".to_owned(),
                "'order_events'".to_owned(),
                "3".to_owned(),
                "1205".to_owned()
            ])
        );
        assert_eq!(
            parse_call("call `sprig`.`commit_offset` ( 'a,b', CONCAT('x', 'y'), 0, @o + 1 )"),
            Some(vec![
                "'a,b'".to_owned(),
                "CONCAT('x', 'y')".to_owned(),
                "0".to_owned(),
                "@o + 1".to_owned()
            ])
        );
        assert_eq!(parse_call("CALL sprig.mq_ack('x')"), None);
        assert_eq!(parse_call("SELECT 1"), None);
        assert_eq!(parse_call("CALLsprig.commit_offset(1)"), None);
    }
}
