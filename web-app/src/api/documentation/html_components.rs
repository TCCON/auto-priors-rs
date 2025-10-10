use std::borrow::Cow;

use askama::Template;
use utoipa::openapi::{request_body::RequestBody, Array, Object, OneOf, RefOr, Schema};

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
            Schema::AllOf(all_of) => Self::Unknown(HtmlUnknownSchema {
                type_descr: "all_of",
            }),
            Schema::AnyOf(any_of) => Self::Unknown(HtmlUnknownSchema {
                type_descr: "any_of",
            }),
            _ => Self::Unknown(HtmlUnknownSchema {
                type_descr: "UNDEFINED",
            }),
        }
    }
}

#[derive(Template)]
#[template(path = "docs/component-schema-object.html")]
pub(super) struct HtmlObjectSchema<'o> {
    obj: &'o Object,
    length: DisplayLength,
}

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

#[derive(Template)]
#[template(path = "docs/component-schema-one-of.html")]
pub(super) struct HtmlOneOfSchema<'o> {
    one_of: &'o OneOf,
    length: DisplayLength,
}

#[derive(Template)]
#[template(source = "Unknown schema: '{{ type_descr }}'", ext = "html")]
pub(super) struct HtmlUnknownSchema {
    type_descr: &'static str,
}

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
