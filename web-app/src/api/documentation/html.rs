use std::{borrow::Cow, collections::HashMap};

use askama::Template;
use itertools::Itertools;
use orm::auth::User;

use crate::templates_common::{BaseContext, ContextWithSidebar};

/// The root context for the HTML API documentation
#[derive(Template)]
#[template(path = "docs/api-documentation.html")]
pub(super) struct ApiDocsContext<'o> {
    root_uri: String,
    user: Option<User>,
    endpoints: Vec<(&'o str, Vec<DocEndpoint<'o>>)>,
}

impl<'o> ApiDocsContext<'o> {
    pub(super) fn new(
        root_uri: String,
        user: Option<User>,
        api: &'o utoipa::openapi::OpenApi,
    ) -> Self {
        let endpoints = Self::collect_endpoints(api);

        Self {
            root_uri,
            user,
            endpoints,
        }
    }

    fn collect_endpoints(
        api: &'o utoipa::openapi::OpenApi,
    ) -> Vec<(&'o str, Vec<DocEndpoint<'o>>)> {
        let endpoints_list = DocEndpoint::list_from_openapi(api);
        let mut grouped_endpoints = HashMap::new();
        for endpoint in endpoints_list {
            let key = endpoint.group;
            let entry: &mut Vec<DocEndpoint<'o>> = grouped_endpoints.entry(key).or_default();
            entry.push(endpoint);
        }

        let mut endpoints = grouped_endpoints.into_iter().collect_vec();
        endpoints.sort_by_key(|grp| grp.0);
        endpoints
    }
}

impl<'o> BaseContext for ApiDocsContext<'o> {
    fn subtitle(&self) -> &str {
        "API Documentation"
    }

    fn page_id(&self) -> &str {
        "api-docs"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }

    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }
}

impl<'o> ContextWithSidebar for ApiDocsContext<'o> {
    fn sblink(
        &self,
        resource_uri: &str,
        text: &str,
        curr_page_id: &str,
        link_page_id: &str,
    ) -> crate::templates_common::Sblink {
        crate::templates_common::sblink_inner(
            &self.root_uri,
            resource_uri,
            text,
            curr_page_id,
            link_page_id,
        )
    }
}

pub(crate) trait CodeExample {
    fn example(&self, path: &str, request_body: Option<&str>) -> Cow<'static, str>;

    fn concrete_example(
        &self,
        path: &str,
        request_body: Option<&str>,
        params: &HashMap<String, String>,
    ) -> String {
        let template = self.example(path, request_body);
        // TODO: replace {PARAM} values with entries from `params`
        return template.to_string();
    }
}

/// The context for a single endpoint's documentation section.
#[derive(askama::Template)]
#[template(path = "docs/endpoint-doc.html")]
struct DocEndpoint<'o> {
    group: &'o str,
    endpoint_name: &'o str,
    url: &'o str,
    description: String,
    request_type: axum::http::method::Method,
    request_body: Option<String>,
    code_examples: Vec<(String, String)>,
    output: String,
}

impl<'o> DocEndpoint<'o> {
    fn list_from_openapi(api: &'o utoipa::openapi::OpenApi) -> Vec<Self> {
        let mut endpoints = vec![];

        for (url, item) in api.paths.paths.iter() {
            if let Some(operation) = item.get.as_ref() {
                endpoints.push(Self::from_operation_and_examples(
                    url,
                    axum::http::method::Method::GET,
                    operation,
                    &DocCodeExampleTemplates::default(),
                ));
            }

            if let Some(operation) = item.post.as_ref() {
                endpoints.push(Self::from_operation_and_examples(
                    url,
                    axum::http::method::Method::POST,
                    operation,
                    &DocCodeExampleTemplates::default(),
                ))
            }
        }

        endpoints
    }

    fn from_operation_and_examples(
        url: &'o str,
        request_type: axum::http::method::Method,
        operation: &'o utoipa::openapi::path::Operation,
        examples: &DocCodeExampleTemplates,
    ) -> Self {
        // We assume we have at most one tag, because I'm using the tag to group
        // endpoints.
        let group = operation
            .tags
            .as_deref()
            .unwrap_or_default()
            .get(0)
            .map(|s| s.as_str())
            .unwrap_or_default();

        let endpoint_name = operation.operation_id.as_deref().unwrap_or(url);

        let description = match (
            operation.summary.as_deref(),
            operation.description.as_deref(),
        ) {
            (None, None) => Cow::Borrowed(""),
            (None, Some(desc)) => Cow::Borrowed(desc),
            (Some(summ), None) => Cow::Borrowed(summ),
            (Some(summ), Some(desc)) => Cow::Owned(format!("{summ}\n\n{desc}")),
        };

        let description = markdown::to_html(&description);

        // TODO: get the request body and output out of the schema
        let code_examples = examples.make_examples(&request_type, url, None);

        Self {
            group,
            endpoint_name,
            url,
            description,
            request_type,
            request_body: None,
            code_examples,
            output: "".to_string(),
        }
    }
}

/// A collection of code example templates that show how to call the endpoints.
#[derive(Default, askama::Template)]
#[template(path = "docs/code-examples.html")]
struct DocCodeExampleTemplates {
    examples: HashMap<axum::http::method::Method, Vec<(String, Box<dyn CodeExample>)>>,
}

impl DocCodeExampleTemplates {
    fn add_example<S: ToString, E: CodeExample + 'static>(
        &mut self,
        method: axum::http::method::Method,
        language: S,
        example: E,
    ) {
        let examples = self.examples.entry(method).or_default();
        examples.push((language.to_string(), Box::new(example)));
    }

    fn add_get_example<S: ToString, E: CodeExample + 'static>(&mut self, language: S, example: E) {
        self.add_example(axum::http::Method::GET, language, example);
    }

    fn add_post_example<S: ToString, E: CodeExample + 'static>(&mut self, language: S, example: E) {
        self.add_example(axum::http::Method::POST, language, example);
    }

    fn make_examples(
        &self,
        method: &axum::http::Method,
        path: &str,
        request_body: Option<&str>,
    ) -> Vec<(String, String)> {
        let it = self
            .examples
            .get(&method)
            .map(|v| v.as_slice())
            .unwrap_or_default();
        let mut examples = vec![];
        for (lang, code_ex) in it {
            examples.push((
                lang.to_string(),
                code_ex.example(path, request_body).to_string(),
            ));
        }
        examples
    }
}

// ------------------------------ //
// API-specific example templates //
// ------------------------------ //

pub(crate) struct CurlGetExample;

impl CodeExample for CurlGetExample {
    fn example(&self, path: &str, request_body: Option<&str>) -> Cow<'static, str> {
        if let Some(body) = request_body {
            format!(
                r#"# We wrap the data argument in single quotes because the JSON string must use
# double-quoted strings; therefore, putting the JSON string inside single quotes
# saves us from needing to escape those double quotes. If you need to use shell
# variables in the data, remember that most shells do not expand variables inside
# single-quoted strings!

curl {path} \
--header "Authorization: Bearer $(cat ~/.priors-api-key)" \
--header "Content-Type: application/json" \
--data '{body}'"#,
            )
            .into()
        } else {
            format!(r#"curl --header "Authorization: Bearer $(cat ~/.priors-api-key)" {path}"#)
                .into()
        }
    }
}

pub(crate) struct CurlPostExample;

impl CodeExample for CurlPostExample {
    fn example(&self, path: &str, request_body: Option<&str>) -> Cow<'static, str> {
        if let Some(body) = request_body {
            format!(
                r#"# We wrap the data argument in single quotes because the JSON string must use
# double-quoted strings; therefore, putting the JSON string inside single quotes
# saves us from needing to escape those double quotes. If you need to use shell
# variables in the data, remember that most shells do not expand variables inside
# single-quoted strings!

curl {path} \
--request POST \
--header "Authorization: Bearer $(cat ~/.priors-api-key)" \
--header "Content-Type: application/json" \
--data '{body}'"#,
            )
            .into()
        } else {
            format!(r#"curl --request POST --header "Authorization: Bearer $(cat ~/.priors-api-key)" {path}"#)
                .into()
        }
    }
}

/// Get all the examples that would be shown in an OpenAPI structure.
///
/// Following the utoipa documentation, this will use `content.examples` if it is not empty,
/// then `content.example` if it is not `None`. If neither contain an example, the returned
/// `Vec` will be empty.
fn get_examples(content: &utoipa::openapi::content::Content) -> Vec<(&str, &serde_json::Value)> {
    let mut examples = vec![];
    if content.examples.is_empty() {
        if let Some(ex) = &content.example {
            examples.push(("example", ex));
        }
    } else {
        for (key, ref_or_ex) in content.examples.iter() {
            let opt_val = match ref_or_ex {
                utoipa::openapi::RefOr::Ref(_) => todo!(),
                utoipa::openapi::RefOr::T(ex) => &ex.value,
            };

            if let Some(val) = opt_val {
                examples.push((key.as_str(), val));
            }
        }
    }

    examples
}

/// Get a single example from those that would be show in an OpenAPI structure.
///
/// Following the utoipa documentation, this will use `content.examples` if it is not empty,
/// then `content.example` if it is not `None`. If neither contain an example, the returned
/// value will be `None`. Compared to [`get_examples`], this avoids allocating a vector just
/// to get a single value.
fn get_one_example(
    content: &utoipa::openapi::content::Content,
) -> Option<(&str, &serde_json::Value)> {
    if content.examples.is_empty() {
        content.example.as_ref().map(|val| ("example", val))
    } else {
        let (key, ref_or_ex) = content.examples.iter().next()?;
        let val = match ref_or_ex {
            utoipa::openapi::RefOr::Ref(_) => todo!(),
            utoipa::openapi::RefOr::T(ex) => ex.value.as_ref()?,
        };
        Some((key.as_str(), val))
    }
}
