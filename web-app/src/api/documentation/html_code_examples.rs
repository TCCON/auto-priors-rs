//! Template components for code examples.
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
/// [`default_example_values_for_type`] does not make sense. In each tuple,
/// the first element is the placeholder, and the second is the example value.
/// Note that the placeholder must include the enclosing curly braces.
const URL_EXAMPLE_SUBS: &'static [(&'static str, &'static str)] =
    &[("{site_id}", "xx"), ("{date}", "2025-01-19")];

/// Create a concrete URL from a path by replacing any parameters with real values.
/// These values will be taken first from [`URL_EXAMPLE_SUBS`], then defaults defined
/// by [`default_example_url_values_for_schema`].
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

/// Provides a default value for a URL property based on its schema.
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
        Schema::OneOf(_one_of) => Cow::Borrowed("TODO"),
        Schema::AllOf(_all_of) => Cow::Borrowed("TODO"),
        Schema::AnyOf(_any_of) => Cow::Borrowed("TODO"),
        _ => todo!(),
    }
}

/// Provides a default value for a scalar valued URL property based on its type.
/// Use [`default_example_url_values_for_schema`] if you have the property's
/// overall schema instead.
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

/// Create the list of code examples for a HTTP GET endpoint.
///
/// # Parameters
/// - `url_template`: the path defined for the endpoint, with placeholders.
/// - `parameters`: the optional list of URL parameters defined for the [`Operation`].
///   These will be used to insert concrete values into the URL for the examples.
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

/// Template for an example of calling an HTTP GET with Python
#[derive(Template)]
#[template(path = "docs/code-python-get.txt")]
struct PythonGetExample<'u> {
    url: Cow<'u, str>,
}

/// Template for an example of calling an HTTP GET with cURL
#[derive(Template)]
#[template(path = "docs/code-curl-get.txt")]
struct CurlGetExample<'u> {
    url: Cow<'u, str>,
}

/// Create the list of code examples for a HTTP POST endpoint.
///
/// # Parameters
/// - `url_template`: the path defined for the endpoint, with placeholders.
/// - `operation`: the definition of the POST call from OpenAPI; it will
///   be used to insert both concrete values into the URL and to create the
///   request body example. This requires that the [`utoipa::path`] macro
///   defines an `examples` field for the request body.
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

/// Template for an example of calling an HTTP POST with cURL
#[derive(Template)]
#[template(path = "docs/code-curl-post.txt")]
struct CurlPostExample<'u> {
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

/// Template for an example of calling an HTTP POST with Python
#[derive(Template)]
#[template(path = "docs/code-python-post.txt")]
struct PythonPostExample<'u> {
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

/// Create a JSON string of an [`Example`], handling the case of a missing value.
fn example_to_json_string(ex: &Example) -> String {
    if let Some(value) = &ex.value {
        serde_json::to_string(value).unwrap_or_else(|e| format!("ERROR: {e}"))
    } else {
        log::warn!("Missing example value on: {}", ex.summary);
        "EXAMPLE NEEDED".to_string()
    }
}
