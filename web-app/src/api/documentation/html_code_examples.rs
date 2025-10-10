use std::{borrow::Cow, collections::BTreeMap};

use askama::Template;
use itertools::Itertools;
use utoipa::openapi::{
    example::Example,
    path::{Operation, Parameter},
    schema::SchemaType,
    RefOr, Schema, Type,
};

use crate::api::documentation::helpers;

/// These define specific substitutions to use for certain parameters where
/// [`default_example_values_for_type`] does not make sense.
const URL_EXAMPLE_SUBS: &'static [(&'static str, &'static str)] =
    &[("{site_id}", "xx"), ("{date}", "2025-01-19")];

fn build_concrete_url<'u>(url_template: &'u str, parameters: Option<&[Parameter]>) -> Cow<'u, str> {
    let mut url = Cow::Borrowed(url_template);
    for (placeholder, value) in URL_EXAMPLE_SUBS {
        if url.contains(placeholder) {
            url = Cow::Owned(url.replace(placeholder, &value))
        }
    }
    if let Some(params) = parameters {
        for param in params {
            let placeholder = format!("{{{}}}", param.name);
            if url.contains(&placeholder) {
                match &param.schema {
                    Some(RefOr::T(schema)) => {
                        let val = default_example_url_values_for_schema(schema);
                        url = Cow::Owned(url.replace(&placeholder, &val));
                    }
                    Some(RefOr::Ref(_)) => (),
                    None => (),
                }
            }
        }
    }
    url
}

fn default_example_url_values_for_schema(s: &Schema) -> Cow<'static, str> {
    match s {
        Schema::Array(array) => match &array.items {
            utoipa::openapi::schema::ArrayItems::RefOrSchema(ref_or) => match ref_or.as_ref() {
                RefOr::Ref(_) => Cow::Borrowed("#REF"),
                RefOr::T(schema) => default_example_url_values_for_schema(schema),
            },
            utoipa::openapi::schema::ArrayItems::False => Cow::Borrowed("FALSE"),
        },
        Schema::Object(object) => default_example_url_values_for_type(&object.schema_type),
        Schema::OneOf(one_of) => Cow::Borrowed("TODO"),
        Schema::AllOf(all_of) => Cow::Borrowed("TODO"),
        Schema::AnyOf(any_of) => Cow::Borrowed("TODO"),
        _ => todo!(),
    }
}

fn default_example_url_values_for_type(st: &SchemaType) -> Cow<'static, str> {
    fn type_ex(t: &Type) -> &'static str {
        match t {
            Type::Object => "{}",
            Type::String => r#""alpha""#,
            Type::Integer => "1",
            Type::Number => "-42.0",
            Type::Boolean => "false",
            Type::Array => "[]",
            Type::Null => "null",
        }
    }

    match st {
        SchemaType::Type(t) => Cow::Borrowed(type_ex(&t)),
        SchemaType::Array(items) => {
            let inner = items.iter().map(|t| type_ex(t)).join(", ");
            Cow::Owned(format!("[{inner}]"))
        }
        SchemaType::AnyValue => Cow::Borrowed("ANY"),
    }
}

pub(super) fn make_get_examples<'u>(
    url_template: &'u str,
    parameters: Option<&[Parameter]>,
) -> BTreeMap<&'static str, String> {
    let url = build_concrete_url(url_template, parameters);
    BTreeMap::from_iter([
        (
            "cURL",
            CurlGetExample { url: url.clone() }
                .render()
                .unwrap_or_else(|e| format!("ERROR: {e}")),
        ),
        (
            "Python",
            PythonGetExample { url }
                .render()
                .unwrap_or_else(|e| format!("ERROR: {e}")),
        ),
    ])
}

#[derive(Template)]
#[template(path = "docs/code-python-get.txt")]
pub(super) struct PythonGetExample<'u> {
    url: Cow<'u, str>,
}

#[derive(Template)]
#[template(path = "docs/code-curl-get.txt")]
pub(super) struct CurlGetExample<'u> {
    url: Cow<'u, str>,
}

pub(super) fn make_post_examples<'u>(
    url_template: &'u str,
    operation: &Operation,
) -> BTreeMap<&'static str, String> {
    let url = build_concrete_url(url_template, operation.parameters.as_deref());
    BTreeMap::from_iter([
        (
            "cURL",
            CurlPostExample::new(url.clone(), operation)
                .render()
                .unwrap_or_else(|e| format!("ERROR: {e}")),
        ),
        (
            "Python",
            PythonPostExample::new(url, operation)
                .render()
                .unwrap_or_else(|e| format!("ERROR: {e}")),
        ),
    ])
}

#[derive(Template)]
#[template(path = "docs/code-curl-post.txt")]
pub(super) struct CurlPostExample<'u> {
    url: Cow<'u, str>,
    request_body: String,
}

impl<'u> CurlPostExample<'u> {
    fn new(url: Cow<'u, str>, operation: &Operation) -> Self {
        let json_example = operation
            .request_body
            .as_ref()
            .and_then(|reqs| reqs.content.get("application/json"))
            .and_then(|cont| cont.examples.first_key_value().map(|(_, v)| v));

        let request_body = match json_example {
            Some(RefOr::T(ex)) => example_to_json_string(ex),
            Some(RefOr::Ref(_)) => unimplemented!("example as reference"),
            None => {
                log::warn!(
                    "No application/json example on {}",
                    operation.operation_id.as_deref().unwrap_or("???")
                );
                "{}".to_string()
            }
        };

        Self { url, request_body }
    }
}

#[derive(Template)]
#[template(path = "docs/code-python-post.txt")]
pub(super) struct PythonPostExample<'u> {
    url: Cow<'u, str>,
    request_body: String,
}

impl<'u> PythonPostExample<'u> {
    fn new(url: Cow<'u, str>, operation: &Operation) -> Self {
        let json_example = operation
            .request_body
            .as_ref()
            .and_then(|reqs| reqs.content.get("application/json"))
            .and_then(|cont| cont.examples.first_key_value().map(|(_, v)| v));

        let request_body = match json_example {
            Some(RefOr::T(ex)) => {
                let mut s = String::new();
                if let Some(val) = &ex.value {
                    helpers::json_to_python(&mut s, val, Some(4)).unwrap_or_else(|e| {
                        log::error!("Error while converting example JSON body to Python: {e}")
                    })
                }
                s
            }
            Some(RefOr::Ref(_)) => unimplemented!("example as reference"),
            None => {
                log::warn!(
                    "No application/json example on {}",
                    operation.operation_id.as_deref().unwrap_or("???")
                );
                "{}".to_string()
            }
        };

        Self { url, request_body }
    }
}

fn example_to_json_string(ex: &Example) -> String {
    if let Some(value) = &ex.value {
        serde_json::to_string(value).unwrap_or_else(|e| format!("ERROR: {e}"))
    } else {
        log::warn!("Missing example value on: {}", ex.summary);
        "EXAMPLE NEEDED".to_string()
    }
}
