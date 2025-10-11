use std::borrow::Cow;

use itertools::Itertools;
use utoipa::openapi::{
    example::{self, Example},
    Content, RefOr,
};

/// Helper function for templates to check if a parameter is required
///
/// The `p.required` field is not a simple boolean, so templates cannot
/// use it in `{% if ... %}` checks. Instead, they can call this function
/// as `self::param_required(param)`.
pub(crate) fn param_required(p: &utoipa::openapi::path::Parameter) -> bool {
    match p.required {
        utoipa::openapi::Required::True => true,
        utoipa::openapi::Required::False => false,
    }
}

pub(crate) fn fmt_string(f: &utoipa::openapi::schema::SchemaFormat) -> Cow<'_, str> {
    match f {
        utoipa::openapi::SchemaFormat::KnownFormat(known_format) => {
            serde_json::to_string(known_format)
                .map(|s| Cow::Owned(s.trim_matches('"').to_string()))
                .unwrap_or(Cow::Borrowed("UNKNOWN FORMAT"))
        }
        utoipa::openapi::SchemaFormat::Custom(s) => Cow::Borrowed(s.as_str()),
    }
}

pub(crate) fn schema_type_string(t: &utoipa::openapi::schema::SchemaType) -> Cow<'static, str> {
    fn inner_type_string(ti: &utoipa::openapi::schema::Type) -> &'static str {
        match ti {
            utoipa::openapi::Type::Object => "object",
            utoipa::openapi::Type::String => "string",
            utoipa::openapi::Type::Integer => "integer",
            utoipa::openapi::Type::Number => "real number",
            utoipa::openapi::Type::Boolean => "boolean (<code>true</code> or <code>false</code>)",
            utoipa::openapi::Type::Array => "array",
            utoipa::openapi::Type::Null => "null",
        }
    }

    match t {
        utoipa::openapi::schema::SchemaType::Type(inner) => Cow::Borrowed(inner_type_string(inner)),
        utoipa::openapi::schema::SchemaType::Array(items) => {
            let s = items.iter().map(|i| inner_type_string(i)).join(" or ");
            Cow::Owned(s)
        }
        utoipa::openapi::schema::SchemaType::AnyValue => Cow::Borrowed("any"),
    }
}

pub(crate) fn comp_schema_id(name: &str) -> String {
    format!("comp-schema-{name}")
}

pub(crate) fn endpoint_id(group: &str, name: &str) -> String {
    format!("{group}-{name}")
}

pub(crate) fn reference_id(reference: &str) -> askama::Result<String> {
    if reference.starts_with("#/components/schemas/") {
        let name = reference.split('/').last().ok_or_else(|| {
            askama::Error::custom(format!("No component name in reference: {reference}"))
        })?;
        Ok(comp_schema_id(name))
    } else {
        Err(askama::Error::custom(format!(
            "Prefix of reference not implemented: {reference}"
        )))
    }
}

pub(crate) fn reference_name(reference: &str) -> &str {
    reference.split('/').last().unwrap_or(reference)
}

/// Get example values and their names from OpenAPI content.
/// This will prefer the `examples` field if available. If not,
/// it will use the `example` field. The returned vec will be
/// empty if neither provided examples.
pub(crate) fn get_example_values(content: &Content) -> Vec<(&str, &serde_json::Value)> {
    if !content.examples.is_empty() {
        let mut out = vec![];
        for (key, ref_or_ex) in content.examples.iter() {
            if let RefOr::T(ex) = ref_or_ex {
                if let Some(val) = &ex.value {
                    out.push((key.as_str(), val))
                }
            }
        }
        return out;
    }

    if let Some(val) = &content.example {
        return vec![("example", val)];
    }

    return vec![];
}

/// Write a [`serde_json::Value`] as a Python string - bool, int, float, None, list, or dict.
///
/// # Parameter
/// - `writer`: what to write the Python string into
/// - `value`: the value to convert
/// - `indent`: if `None`, the output will be all one line. If `Some(n)`,
///   each successive nested value will be indented by `n`.
pub(super) fn json_to_python<W: std::fmt::Write>(
    writer: &mut W,
    value: &serde_json::Value,
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
    value: &serde_json::Value,
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
            if *b {
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
    values: &[serde_json::Value],
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
    map: &serde_json::Map<String, serde_json::Value>,
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

fn write_string<W: std::fmt::Write>(writer: &mut W, s: &str) -> std::fmt::Result {
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

    use super::*;

    #[test]
    fn test_string_double_quote() {
        let mut py_s = String::new();
        json_to_python(
            &mut py_s,
            &serde_json::Value::String("It's me!".to_string()),
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
            &serde_json::Value::String(r#"I said, "It's me!""#.to_string()),
            None,
        )
        .unwrap();
        assert_eq!(py_s, r#"'I said, "It\'s me!"'"#);
    }

    #[test]
    fn test_list() {
        let val = json!([true, false, null]);
        let mut py_s = String::new();
        json_to_python(&mut py_s, &val, None).unwrap();
        assert_eq!(py_s, "[True, False, None]");
    }

    #[test]
    fn test_pretty_list() {
        let val = json!([true, false, null]);
        let mut py_s = String::new();
        json_to_python(&mut py_s, &val, Some(2)).unwrap();
        assert_eq!(py_s, "[\n  True,\n  False,\n  None\n]");
    }

    #[test]
    fn test_map() {
        let val = json!({"a": 1, "b": true, "c": null});
        let mut py_s = String::new();
        json_to_python(&mut py_s, &val, None).unwrap();
        assert_eq!(py_s, "{'a': 1, 'b': True, 'c': None}");
    }

    #[test]
    fn test_pretty_map() {
        let val = json!({"a": 1, "b": true, "c": null});
        let mut py_s = String::new();
        json_to_python(&mut py_s, &val, Some(2)).unwrap();
        assert_eq!(py_s, "{\n  'a': 1,\n  'b': True,\n  'c': None\n}");
    }

    #[test]
    fn test_nested_maps() {
        let val = json!({"sites": [{"id": "aa"}, {"id": "bb"}]});
        let mut py_s = String::new();
        json_to_python(&mut py_s, &val, Some(2)).unwrap();
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
