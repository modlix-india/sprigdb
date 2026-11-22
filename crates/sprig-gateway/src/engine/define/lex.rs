//! Tokens of a data definition statement, with where each starts (a syntax
//! error quotes the statement from the token the grammar stopped at).

/// A token's kind and text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum T {
    /// A bare word (keyword or name), as written.
    Word(String),
    /// A back-quoted name, unescaped.
    Quoted(String),
    /// A string literal (single or double quotes), unescaped.
    Str(String),
    /// A number as written (`12`, `1.5`, `.5`, `1e3`).
    Num(String),
    /// `x'4142'`: the hex digits.
    Hex(String),
    /// `0x4142`: the hex digits.
    HexNum(String),
    /// `b'101'` or `0b101`: the binary digits.
    Bin(String),
    /// An operator or punctuation: `(`, `)`, `,`, `.`, `;`, `=`, `<=>`,
    /// `<>`, `!=`, `<=`, `>=`, `<<`, `>>`, `&&`, `||`, `:=`, `+`, ...
    Op(&'static str),
    /// `@name` / `@@name`: kept whole.
    Var(String),
}

/// A token and the byte offset it starts at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Tok {
    pub t: T,
    pub at: usize,
}

impl Tok {
    /// The token is the bare word `w` (any case).
    pub(crate) fn is(&self, w: &str) -> bool {
        matches!(&self.t, T::Word(x) if x.eq_ignore_ascii_case(w))
    }

    /// The token is the operator `o`.
    pub(crate) fn op(&self, o: &str) -> bool {
        matches!(&self.t, T::Op(x) if *x == o)
    }
}

const OPS: [&str; 33] = [
    "<=>", "<>", "!=", "<=", ">=", "<<", ">>", "&&", "||", ":=", "->>", "->", "(", ")", ",", ".",
    ";", "=", "<", ">", "+", "-", "*", "/", "%", "!", "~", "^", "&", "|", "?", "{", "}",
];

/// The tokens of `s`; `Err(offset)` where a quote or comment never ends.
#[allow(clippy::too_many_lines)] // one branch per token kind
pub(crate) fn lex(s: &str) -> Result<Vec<Tok>, usize> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Comments: `-- ` to the end of the line, `#` to the end of the
        // line, `/* ... */` (executable comments were neutralized before).
        if c == b'#'
            || (c == b'-'
                && b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace))
        {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let start = i;
            i += 2;
            loop {
                if i + 1 >= b.len() {
                    return Err(start);
                }
                if b[i] == b'*' && b[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        let at = i;
        if c == b'`' {
            let mut v = Vec::new();
            i += 1;
            loop {
                let Some(&ch) = b.get(i) else { return Err(at) };
                if ch == b'`' {
                    if b.get(i + 1) == Some(&b'`') {
                        v.push(b'`');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(ch);
                i += 1;
            }
            out.push(Tok {
                t: T::Quoted(String::from_utf8_lossy(&v).into_owned()),
                at,
            });
            continue;
        }
        if c == b'\'' || c == b'"' {
            let q = c;
            let mut v = Vec::new();
            i += 1;
            loop {
                let Some(&ch) = b.get(i) else { return Err(at) };
                if ch == b'\\' {
                    let Some(&n) = b.get(i + 1) else {
                        return Err(at);
                    };
                    match n {
                        b'n' => v.push(b'\n'),
                        b't' => v.push(b'\t'),
                        b'0' => v.push(0),
                        b'r' => v.push(b'\r'),
                        b'b' => v.push(8),
                        b'Z' => v.push(0x1a),
                        // `\%` and `\_` keep their backslash (LIKE patterns).
                        b'%' | b'_' => v.extend_from_slice(&[b'\\', n]),
                        o => v.push(o),
                    }
                    i += 2;
                    continue;
                }
                if ch == q {
                    if b.get(i + 1) == Some(&q) {
                        v.push(q);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(ch);
                i += 1;
            }
            out.push(Tok {
                t: T::Str(String::from_utf8_lossy(&v).into_owned()),
                at,
            });
            continue;
        }
        // x'..', X'..', b'..', B'..'
        if (c == b'x' || c == b'X' || c == b'b' || c == b'B') && b.get(i + 1) == Some(&b'\'') {
            let hex = c == b'x' || c == b'X';
            let mut j = i + 2;
            while j < b.len() && b[j] != b'\'' {
                j += 1;
            }
            if j >= b.len() {
                return Err(at);
            }
            let digits = String::from_utf8_lossy(&b[i + 2..j]).into_owned();
            out.push(Tok {
                t: if hex { T::Hex(digits) } else { T::Bin(digits) },
                at,
            });
            i = j + 1;
            continue;
        }
        if c == b'0' && matches!(b.get(i + 1), Some(b'x' | b'b')) {
            let hex = b[i + 1] == b'x';
            let mut j = i + 2;
            while j < b.len()
                && (if hex {
                    b[j].is_ascii_hexdigit()
                } else {
                    b[j] == b'0' || b[j] == b'1'
                })
            {
                j += 1;
            }
            let ends_word = b
                .get(j)
                .is_none_or(|x| !(x.is_ascii_alphanumeric() || *x == b'_' || *x == b'$'));
            if j > i + 2 && ends_word {
                let digits = String::from_utf8_lossy(&b[i + 2..j]).into_owned();
                out.push(Tok {
                    t: if hex {
                        T::HexNum(digits)
                    } else {
                        T::Bin(digits)
                    },
                    at,
                });
                i = j;
                continue;
            }
        }
        if c.is_ascii_digit()
            || (c == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit) && !prev_is_name(&out))
        {
            let mut j = i;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if j < b.len() && b[j] == b'.' {
                j += 1;
                while j < b.len() && b[j].is_ascii_digit() {
                    j += 1;
                }
            }
            if j < b.len() && (b[j] == b'e' || b[j] == b'E') {
                let mut k = j + 1;
                if k < b.len() && (b[k] == b'+' || b[k] == b'-') {
                    k += 1;
                }
                if k < b.len() && b[k].is_ascii_digit() {
                    while k < b.len() && b[k].is_ascii_digit() {
                        k += 1;
                    }
                    j = k;
                }
            }
            // `1abc` is a name in MySQL's grammar.
            if j < b.len()
                && (b[j].is_ascii_alphabetic() || b[j] == b'_' || b[j] == b'$')
                && !b[i..j].contains(&b'.')
            {
                let mut k = j;
                while k < b.len() && is_name_byte(b[k]) {
                    k += 1;
                }
                out.push(Tok {
                    t: T::Word(String::from_utf8_lossy(&b[i..k]).into_owned()),
                    at,
                });
                i = k;
                continue;
            }
            out.push(Tok {
                t: T::Num(String::from_utf8_lossy(&b[i..j]).into_owned()),
                at,
            });
            i = j;
            continue;
        }
        if is_name_byte(c) {
            let mut j = i;
            while j < b.len() && is_name_byte(b[j]) {
                j += 1;
            }
            out.push(Tok {
                t: T::Word(String::from_utf8_lossy(&b[i..j]).into_owned()),
                at,
            });
            i = j;
            continue;
        }
        if c == b'@' {
            let mut j = i + 1;
            if b.get(j) == Some(&b'@') {
                j += 1;
            }
            while j < b.len() && (is_name_byte(b[j]) || b[j] == b'.') {
                j += 1;
            }
            out.push(Tok {
                t: T::Var(String::from_utf8_lossy(&b[i..j]).into_owned()),
                at,
            });
            i = j;
            continue;
        }
        let rest = &s[i..];
        let Some(op) = OPS.iter().find(|o| rest.starts_with(**o)) else {
            return Err(at);
        };
        out.push(Tok { t: T::Op(op), at });
        i += op.len();
    }
    Ok(out)
}

/// Bytes of a bare name: letters, digits, `_`, `$`, and anything not
/// ASCII.
fn is_name_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// The last token is a name (`t.5` isn't a number after a name).
fn prev_is_name(out: &[Tok]) -> bool {
    matches!(out.last().map(|t| &t.t), Some(T::Word(_) | T::Quoted(_)))
}
