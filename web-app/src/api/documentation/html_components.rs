//! Templates for displaying components of the OpenAPI structure in HTML
//!
//! This focuses on data types, not endpoints.
use std::borrow::Cow;

use askama::Template;
use utoipa::openapi::{request_body::RequestBody, Array, Object, OneOf, RefOr, Response, Schema};

/// How long the description of a component should be in the API page
#[derive(Debug, Clone, Copy)]
pub(crate) enum DisplayLength {
    /// Display all details, intended for top-level components. May span multiple lines.
    Full,
    /// Display a condensed version of the item, aiming to fit on one line. Should not include any line breaks.
    Short,
    /// Display an extremely compact description of the item, intended to fit within another line. Must not have
    /// line breaks.
    Minimal,
}

/// A template to show a schema from the OpenAPI structure that may be a reference to
/// another component in the structure.
#[derive(Template)]
#[template(path = "docs/ref-or-schema.html")]
pub(super) struct HtmlRefOrSchema<'o> {
    inner: &'o RefOr<Schema>,
    length: DisplayLength,
}

impl<'o> HtmlRefOrSchema<'o> {
    pub(crate) fn new(inner: &'o RefOr<Schema>, length: DisplayLength) -> Self {
        Self { inner, length }
    }
}

/// A template to show a reference from the OpenAPI structure that may be a reference to
/// another component in the structure.
#[derive(Template)]
#[template(path = "docs/ref-or-response.html")]
pub(super) struct HtmlRefOrResponse<'o> {
    inner: &'o RefOr<Response>,
}

impl<'o> HtmlRefOrResponse<'o> {
    pub(crate) fn new(inner: &'o RefOr<Response>) -> Self {
        Self { inner }
    }
}

/// The template to display a schema.
///
/// Schema represent a wide breadth of concrete data types. This will
/// describe the type of the schema in a range of detail, determined by
/// the [`DisplayLength`] enum stored on most of the variants.
///
/// Note that this currently only handles the `Array`, `Object`, and `OneOf`
/// variants of the schema, as I had no example of an `AllOf` or `AnyOf` schema
/// to build around.
#[derive(Template)]
#[template(path = "docs/component-schema.html")]
pub(super) enum HtmlSchema<'o> {
    Array(HtmlArraySchema<'o>),
    Object(HtmlObjectSchema<'o>),
    OneOf(HtmlOneOfSchema<'o>),
    Unknown(HtmlUnknownSchema),
}

impl<'o> HtmlSchema<'o> {
    pub(super) fn new(inner: &'o Schema, length: DisplayLength) -> Self {
        match inner {
            Schema::Array(array) => Self::Array(HtmlArraySchema { array, length }),
            Schema::Object(obj) => Self::Object(HtmlObjectSchema { obj, length }),
            Schema::OneOf(one_of) => Self::OneOf(HtmlOneOfSchema { one_of, length }),
            Schema::AllOf(_all_of) => Self::Unknown(HtmlUnknownSchema {
                type_descr: "all_of",
            }),
            Schema::AnyOf(_any_of) => Self::Unknown(HtmlUnknownSchema {
                type_descr: "any_of",
            }),
            _ => Self::Unknown(HtmlUnknownSchema {
                type_descr: "UNDEFINED",
            }),
        }
    }
}

/// Template to display an `Object` schema.
///
/// These represent a single JSON-style object, and can be
/// simple values (boolean, integer, string), an array,
/// or a structured set of fields.
#[derive(Template)]
#[template(path = "docs/component-schema-object.html")]
pub(super) struct HtmlObjectSchema<'o> {
    obj: &'o Object,
    length: DisplayLength,
}

/// Template to display an `Array` schema.
///
/// It is not clear in what circumstances we use this top-level
/// array schema compared to the array type for an `Object` schema.
/// This schema does seem to require that all the elements be of the
/// same type, while the `Object` array does not.
#[derive(Template)]
#[template(path = "docs/component-schema-array.html")]
pub(super) struct HtmlArraySchema<'o> {
    array: &'o Array,
    length: DisplayLength,
}

impl<'o> HtmlArraySchema<'o> {
    pub(super) fn items_type(&self) -> askama::Result<Cow<'o, str>> {
        match &self.array.items {
            utoipa::openapi::schema::ArrayItems::RefOrSchema(ref_or) => {
                let item = HtmlRefOrSchema::new(&ref_or, DisplayLength::Short);
                Ok(Cow::Owned(item.render()?))
            }
            utoipa::openapi::schema::ArrayItems::False => Ok(Cow::Borrowed("N/A")),
        }
    }
}

/// Template to display a `OneOf` schema.
///
/// These schema seem to be used when there are a selection of
/// types that a single value may be, such as with a Rust enum.
#[derive(Template)]
#[template(path = "docs/component-schema-one-of.html")]
pub(super) struct HtmlOneOfSchema<'o> {
    one_of: &'o OneOf,
    length: DisplayLength,
}

/// Template to display an unknown schema.
///
/// This is a placeholder schema to represent one that we do not know
/// yet how to represent accurately.
#[derive(Template)]
#[template(source = "Unknown schema: '{{ type_descr }}'", ext = "html")]
pub(super) struct HtmlUnknownSchema {
    type_descr: &'static str,
}

/// Template to display a response.
#[derive(Template)]
#[template(path = "docs/response.html")]
pub(super) struct HtmlResponse<'o> {
    inner: &'o Response,
}

impl<'o> HtmlResponse<'o> {
    pub(super) fn new(inner: &'o Response) -> Self {
        Self { inner }
    }
}

/// Template to display the body of a request, potentially
/// including examples. It uses [`super::helpers::get_example_values`]
/// to get the list of examples to show.
#[derive(Template)]
#[template(path = "docs/path-request-body.html")]
pub(super) struct HtmlRequestBody<'o> {
    body: &'o RequestBody,
}

impl<'o> HtmlRequestBody<'o> {
    pub(super) fn new(body: &'o RequestBody) -> Self {
        Self { body }
    }
}
