use serde::{Deserialize, Serialize};

pub(crate) mod check;
pub(crate) mod documentation;
pub(crate) mod download;
pub(crate) mod jobs;
pub(crate) mod middleware;
pub(crate) mod query;

/// A wrapper type for [`NaiveDate`] that implements [`utoipa::ToSchema`]
/// for use in APIs.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ApiNaiveDate(chrono::NaiveDate);

impl ApiNaiveDate {
    pub fn into_date(self) -> chrono::NaiveDate {
        self.0
    }
}

impl std::ops::Deref for ApiNaiveDate {
    type Target = chrono::NaiveDate;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl utoipa::PartialSchema for ApiNaiveDate {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(
            utoipa::openapi::ObjectBuilder::new()
                .schema_type(utoipa::openapi::Type::String)
                .format(Some(utoipa::openapi::SchemaFormat::KnownFormat(
                    utoipa::openapi::KnownFormat::Date,
                )))
                .pattern(Some(r"\d{4}-\d{2}-\d{2}"))
                .description(Some("A date in YYYY-MM-DD format"))
                .build(),
        ))
    }
}

impl utoipa::ToSchema for ApiNaiveDate {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("NaiveDate")
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ApiNaiveDateTime(chrono::NaiveDateTime);

impl std::ops::Deref for ApiNaiveDateTime {
    type Target = chrono::NaiveDateTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl utoipa::PartialSchema for ApiNaiveDateTime {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(
            utoipa::openapi::ObjectBuilder::new()
                .schema_type(utoipa::openapi::Type::String)
                .format(Some(utoipa::openapi::SchemaFormat::KnownFormat(
                    utoipa::openapi::KnownFormat::DateTime,
                )))
                .pattern(Some(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"))
                .description(Some("A date and time in YYYY-MM-DDTHH:MM:SS format"))
                .build(),
        ))
    }
}

impl utoipa::ToSchema for ApiNaiveDateTime {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("NaiveDateTime")
    }
}
