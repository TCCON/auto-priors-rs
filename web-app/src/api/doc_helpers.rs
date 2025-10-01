use itertools::Itertools;

/// Write a [`serde_json::Value`] as a Python string - bool, int, float, None, list, or dict.
///
/// # Parameter
/// - `writer`: what to write the Python string into
/// - `value`: the value to convert
/// - `indent`: if `None`, the output will be all one line. If `Some(n)`,
///   each successive nested value will be indented by `n`.
pub(super) fn json_to_python<W: std::fmt::Write>(
    writer: &mut W,
    value: serde_json::Value,
    indent: Option<usize>,
) -> std::fmt::Result {
    json_to_python_inner(writer, value, indent, 0, true)
}

/// Inner helper for [`json_to_python`].
///
/// `curr_indent` is the current indentation level when pretty-printing.
/// `insert_first_indent` should be set to `false` if no indent should
/// be added before the next value and `true` otherwise. Setting that
/// to `false` is to be used to avoid indenting a value that should start
/// on the same line as the previous write, e.g. the value following a
/// key in a dict.
fn json_to_python_inner<W: std::fmt::Write>(
    writer: &mut W,
    value: serde_json::Value,
    pretty_indent: Option<usize>,
    mut curr_indent: usize,
    insert_first_indent: bool,
) -> std::fmt::Result {
    if let Some(n_spaces) = pretty_indent {
        if insert_first_indent {
            for _ in 0..curr_indent {
                write!(writer, " ")?;
            }
        }
        curr_indent += n_spaces;
    }
    match value {
        serde_json::Value::Null => write!(writer, "None")?,
        serde_json::Value::Bool(b) => {
            if b {
                write!(writer, "True")?
            } else {
                write!(writer, "False")?
            }
        }
        serde_json::Value::Number(number) => write!(writer, "{number}")?,
        serde_json::Value::String(s) => write_string(writer, s)?,
        serde_json::Value::Array(values) => {
            write_values(writer, values, pretty_indent, curr_indent)?
        }
        serde_json::Value::Object(map) => write_map(writer, map, pretty_indent, curr_indent)?,
    }

    Ok(())
}

fn write_values<W: std::fmt::Write>(
    writer: &mut W,
    values: Vec<serde_json::Value>,
    pretty_indent: Option<usize>,
    curr_indent: usize,
) -> std::fmt::Result {
    write!(writer, "[")?;
    if pretty_indent.is_some() {
        write!(writer, "\n")?;
    }
    for pos_val in values.into_iter().with_position() {
        let (val, add_comma) = match pos_val {
            itertools::Position::First(v) => (v, true),
            itertools::Position::Middle(v) => (v, true),
            itertools::Position::Last(v) => (v, false),
            itertools::Position::Only(v) => (v, false),
        };

        json_to_python_inner(writer, val, pretty_indent, curr_indent, true)?;
        if add_comma {
            write!(writer, ",")?;
        }
        if pretty_indent.is_some() {
            // Always add a newline for pretty printing - the closing bracket will go on the next line
            write!(writer, "\n")?;
        } else if add_comma {
            // If printing on one line, only add a space if we have another element coming
            write!(writer, " ")?;
        }
    }
    if let Some(n_spaces) = pretty_indent {
        for _ in 0..(curr_indent - n_spaces) {
            write!(writer, " ")?;
        }
    }
    write!(writer, "]")?;
    Ok(())
}

fn write_map<W: std::fmt::Write>(
    writer: &mut W,
    map: serde_json::Map<String, serde_json::Value>,
    pretty_indent: Option<usize>,
    curr_indent: usize,
) -> std::fmt::Result {
    write!(writer, "{{")?;
    if pretty_indent.is_some() {
        write!(writer, "\n")?;
    }
    for pos_pair in map.into_iter().with_position() {
        let (key, val, add_comma) = match pos_pair {
            itertools::Position::First((k, v)) => (k, v, true),
            itertools::Position::Middle((k, v)) => (k, v, true),
            itertools::Position::Last((k, v)) => (k, v, false),
            itertools::Position::Only((k, v)) => (k, v, false),
        };

        if pretty_indent.is_some() {
            for _ in 0..curr_indent {
                write!(writer, " ")?;
            }
        }

        write_string(writer, key)?;
        write!(writer, ": ")?;
        json_to_python_inner(writer, val, pretty_indent, curr_indent, false)?;
        if add_comma {
            write!(writer, ",")?;
        }
        if pretty_indent.is_some() {
            // Always add a newline for pretty printing - the closing bracket will go on the next line
            write!(writer, "\n")?;
        } else if add_comma {
            // If printing on one line, only add a space if we have another element coming
            write!(writer, " ")?;
        }
    }
    if let Some(n_spaces) = pretty_indent {
        for _ in 0..(curr_indent - n_spaces) {
            write!(writer, " ")?;
        }
    }
    write!(writer, "}}")?;
    Ok(())
}

fn write_string<W: std::fmt::Write>(writer: &mut W, s: String) -> std::fmt::Result {
    let has_sq = s.contains("'");
    let has_dq = s.contains('"');

    if has_sq && has_dq {
        let s2 = s.replace("'", "\\'");
        return write!(writer, "'{s2}'");
    }

    if has_sq {
        return write!(writer, "\"{s}\"");
    }

    write!(writer, "'{s}'")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::api::doc_helpers::json_to_python;

    #[test]
    fn test_string_double_quote() {
        let mut py_s = String::new();
        json_to_python(
            &mut py_s,
            serde_json::Value::String("It's me!".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(py_s, "\"It's me!\"");
    }

    #[test]
    fn test_string_escaped_quote() {
        let mut py_s = String::new();
        json_to_python(
            &mut py_s,
            serde_json::Value::String(r#"I said, "It's me!""#.to_string()),
            None,
        )
        .unwrap();
        assert_eq!(py_s, r#"'I said, "It\'s me!"'"#);
    }

    #[test]
    fn test_list() {
        let val = json!([true, false, null]);
        let mut py_s = String::new();
        json_to_python(&mut py_s, val, None).unwrap();
        assert_eq!(py_s, "[True, False, None]");
    }

    #[test]
    fn test_pretty_list() {
        let val = json!([true, false, null]);
        let mut py_s = String::new();
        json_to_python(&mut py_s, val, Some(2)).unwrap();
        assert_eq!(py_s, "[\n  True,\n  False,\n  None\n]");
    }

    #[test]
    fn test_map() {
        let val = json!({"a": 1, "b": true, "c": null});
        let mut py_s = String::new();
        json_to_python(&mut py_s, val, None).unwrap();
        assert_eq!(py_s, "{'a': 1, 'b': True, 'c': None}");
    }

    #[test]
    fn test_pretty_map() {
        let val = json!({"a": 1, "b": true, "c": null});
        let mut py_s = String::new();
        json_to_python(&mut py_s, val, Some(2)).unwrap();
        assert_eq!(py_s, "{\n  'a': 1,\n  'b': True,\n  'c': None\n}");
    }

    #[test]
    fn test_nested_maps() {
        let val = json!({"sites": [{"id": "aa"}, {"id": "bb"}]});
        let mut py_s = String::new();
        json_to_python(&mut py_s, val, Some(2)).unwrap();
        let expected = r#"{
  'sites': [
    {
      'id': 'aa'
    },
    {
      'id': 'bb'
    }
  ]
}"#;
        assert_eq!(py_s, expected)
    }
}
