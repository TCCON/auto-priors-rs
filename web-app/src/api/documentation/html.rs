use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
};

use askama::{DynTemplate, Template};
use itertools::Itertools;
use orm::auth::User;
use utoipa::openapi::RefOr;

use crate::{
    api::documentation::{html_code_examples, html_components::HtmlSchema},
    templates_common::{BaseContext, ContextWithSidebar},
};

/// The root context for the HTML API documentation
#[derive(Template)]
#[template(path = "docs/api-documentation.html")]
pub(super) struct ApiDocsContext<'o> {
    root_uri: String,
    user: Option<User>,
    endpoints: Vec<(&'o str, Vec<DocEndpoint<'o>>)>,
    schemas: BTreeMap<&'o str, HtmlSchema<'o>>,
}

impl<'o> ApiDocsContext<'o> {
    pub(super) fn new(
        root_uri: String,
        user: Option<User>,
        api: &'o utoipa::openapi::OpenApi,
    ) -> Self {
        let endpoints = Self::collect_endpoints(api);
        let schemas = Self::collect_component_schema(api);

        Self {
            root_uri,
            user,
            endpoints,
            schemas,
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

    fn collect_component_schema(
        api: &'o utoipa::openapi::OpenApi,
    ) -> BTreeMap<&'o str, HtmlSchema<'o>> {
        let mut schema = BTreeMap::new();
        if let Some(components) = api.components.as_ref() {
            for (name, comp_schema) in components.schemas.iter() {
                match comp_schema {
                    // Because this is for the list of components, we just skip references -
                    // no point in having a reference in the list. Eventually, we might handle
                    // this if we refer to an outside schema, but the priors API doesn't do that.
                    RefOr::Ref(_) => (),
                    RefOr::T(sch) => {
                        schema.insert(
                            name.as_str(),
                            HtmlSchema::new(sch, super::html_components::DisplayLength::Full),
                        );
                    }
                }
            }
        }
        schema
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

/// The context for a single endpoint's documentation section.
#[derive(askama::Template)]
#[template(path = "docs/endpoint-doc.html")]
struct DocEndpoint<'o> {
    group: &'o str,
    endpoint_name: &'o str,
    url: &'o str,
    description: String,
    request_type: axum::http::method::Method,
    request_body: Option<&'o utoipa::openapi::request_body::RequestBody>,
    parameters: Option<&'o [utoipa::openapi::path::Parameter]>,
    code_examples: BTreeMap<&'static str, String>,
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
                    html_code_examples::make_get_examples(&url, operation.parameters.as_deref()),
                ));
            }

            if let Some(operation) = item.post.as_ref() {
                endpoints.push(Self::from_operation_and_examples(
                    url,
                    axum::http::method::Method::POST,
                    operation,
                    html_code_examples::make_post_examples(&url, operation),
                ))
            }
        }

        endpoints
    }

    fn from_operation_and_examples(
        url: &'o str,
        request_type: axum::http::method::Method,
        operation: &'o utoipa::openapi::path::Operation,
        code_examples: BTreeMap<&'static str, String>,
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

        let request_body = operation.request_body.as_ref();
        // TODO: get the request body and output out of the schema

        Self {
            group,
            endpoint_name,
            url,
            description,
            request_type,
            request_body: request_body,
            parameters: operation.parameters.as_deref(),
            code_examples,
            output: "".to_string(),
        }
    }
}
