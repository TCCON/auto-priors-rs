use askama::Template;
use orm::auth::User;
use serde::Deserialize;

use crate::templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink};

pub(crate) mod check;
pub(crate) mod documentation;
pub(crate) mod download;
pub(crate) mod jobs;
pub(crate) mod middleware;
pub(crate) mod query;

/// A wrapper type for [`NaiveDate`] that implements [`utoipa::ToSchema`]
/// for use in APIs.
#[derive(Debug, Clone, Copy, Deserialize)]
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

#[derive(Template)]
#[template(path = "api-docs.html")]
struct ApiDocsContext {
    root_uri: String,
    user: Option<User>,
}

impl ApiDocsContext {
    pub(crate) fn new(root_uri: String, user: Option<User>) -> Self {
        Self { root_uri, user }
    }

    pub(crate) fn curl_get_example(&self, path: &str) -> String {
        self.curl_get_args_example(path, "")
    }

    pub(crate) fn curl_get_args_example(&self, path: &str, args: &str) -> String {
        let path = path.trim_start_matches("/");
        format!(
            r#"curl -H "Authorization: Bearer $(cat ~/.priors-api-key)" {}/{path} {args}"#,
            self.root_uri
        )
    }

    pub(crate) fn curl_post_example(
        &self,
        path: &str,
        content_type: &str,
        content: &str,
    ) -> String {
        let path = path.trim_start_matches("/");
        format!(
            r#"
# We wrap the data argument in single quotes because the JSON string must use
# double-quoted strings; therefore, putting the JSON string inside single quotes
# saves us from needing to escape those double quotes. If you need to use shell
# variables in the data, remember that most shells do not expand variables inside
# single-quoted strings!

curl {}/{path}
--request POST \
--header "Authorization: Bearer $(cat ~/.priors-api-key)" \
--header "Content-Type: {content_type}" \
--data '{content}'"#,
            self.root_uri
        )
    }

    pub(crate) fn py_get_example(&self, path: &str) -> String {
        let root = &self.root_uri;
        let path = path.trim_start_matches("/");
        format!(
            r#"from pathlib import Path
import requests

with open(Path("~/.priors-api-key").expanduser()) as f:
    # .strip() ensures no training newline, which can cause invalid header errors
    jwt = f.read().strip()

headers = {{"Authorization": f"Bearer {{jwt}}"}}
result = requests.get("{root}/{path}", headers=headers")"#
        )
    }

    pub(crate) fn py_download_example(&self, path: &str) -> String {
        let root = &self.root_uri;
        let path = path.trim_start_matches("/");
        format!(
            r#"from pathlib import Path
import requests

with open(Path("~/.priors-api-key").expanduser()) as f:
    # .strip() ensures no training newline, which can cause invalid header errors
    jwt = f.read().strip()

headers = {{"Authorization": f"Bearer {{jwt}}"}}

# This approach downloads the whole file at once, which is usually fine
# since the tarballs are fairly small (just over than 0.1 MB for a single 
# site and date). If you need to download in chunks, see the requests
# stream option: https://requests.readthedocs.io/en/latest/user/advanced/#body-content-workflow
result = requests.get("{root}/{path}", headers=headers)
with open('priors.tgz', 'wb') as f:
    f.write(result.content)"#
        )
    }

    pub(crate) fn py_post_json_example(&self, path: &str, json: &str) -> String {
        let root = &self.root_uri;
        let path = path.trim_start_matches("/");
        format!(
            r#"from pathlib import Path
import requests

with open(Path("~/.priors-api-key").expanduser()) as f:
    # .strip() ensures no training newline, which can cause invalid header errors
    jwt = f.read().strip()

headers = {{"Authorization": f"Bearer {{jwt}}"}}
input_dict = {json}
result = requests.post("{root}/{path}", json=input_dict, headers=headers)

# result will have the JSON string returned as the .text attribute,
# it can be transformed into a dict with the .json() method.
"#
        )
    }
}

impl BaseContext for ApiDocsContext {
    fn subtitle(&self) -> &str {
        "API Documentation"
    }

    fn page_id(&self) -> &str {
        "api-docs-old"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }

    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }
}

impl ContextWithSidebar for ApiDocsContext {
    fn sblink(
        &self,
        resource_uri: &str,
        text: &str,
        curr_page_id: &str,
        link_page_id: &str,
    ) -> Sblink {
        sblink_inner(
            &self.root_uri,
            resource_uri,
            text,
            curr_page_id,
            link_page_id,
        )
    }
}

pub(crate) mod get {
    use askama::Template;
    use axum::{extract::State, http::StatusCode, response::Html};

    use crate::{api::ApiDocsContext, auth::AuthSession, server_error, AppStateRef};

    pub(crate) async fn api_docs(
        State(state): AppStateRef,
        session: AuthSession,
    ) -> Result<Html<String>, StatusCode> {
        let context = ApiDocsContext::new(state.root_uri.clone(), session.user);
        let raw = server_error(context.render())?;
        Ok(Html(raw))
    }
}
