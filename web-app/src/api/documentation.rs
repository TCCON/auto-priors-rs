use std::sync::Arc;

use askama::Template;
use axum::{
    extract::State, http::StatusCode, response::Html, routing::get, Extension, Json, Router,
};
use orm::auth::AuthSession;

use crate::{server_error, AppState, AppStateRef};

mod helpers;
mod html;
mod html_code_examples;
mod html_components;
pub(crate) mod json_examples;

pub(crate) struct DocAllEndpoints {
    api: utoipa::openapi::OpenApi,
    json_url: Option<String>,
    html_url: Option<String>,
}

impl From<DocAllEndpoints> for Router<Arc<AppState>> {
    fn from(value: DocAllEndpoints) -> Self {
        let router = Router::new();
        let router = if let Some(url) = value.json_url {
            router.route(&url, get(openapi_json))
        } else {
            router
        };
        let router = if let Some(url) = value.html_url {
            router.route(&url, get(openapi_html))
        } else {
            router
        };
        // We add the API as an extension state, rather than part of the general
        // app state because (a) this avoid a circularity issue, since the endpoints
        // need the app state passed into the custom auth middleware and (b) this
        // should make it easier to turn this documentation module into its own crate
        // in the future.
        let router = router.layer(Extension(value.api));
        router
    }
}

pub(crate) struct DocAllEndpointBuilder {
    api: utoipa::openapi::OpenApi,
    json_url: Option<String>,
    html_url: Option<String>,
}

impl DocAllEndpointBuilder {
    pub(crate) fn new(api: utoipa::openapi::OpenApi) -> Self {
        Self {
            api,
            json_url: None,
            html_url: None,
        }
    }

    pub(crate) fn json_url<S: ToString>(mut self, url: S) -> Self {
        self.json_url = Some(url.to_string());
        self
    }

    pub(crate) fn html_url<S: ToString>(mut self, url: S) -> Self {
        self.html_url = Some(url.to_string());
        self
    }

    pub(crate) fn build(self) -> DocAllEndpoints {
        DocAllEndpoints {
            api: self.api,
            json_url: self.json_url,
            html_url: self.html_url,
        }
    }
}

pub(crate) async fn openapi_json(
    Extension(api): Extension<utoipa::openapi::OpenApi>,
) -> Json<utoipa::openapi::OpenApi> {
    Json(api)
}

pub(crate) async fn openapi_html(
    State(state): AppStateRef,
    session: AuthSession,
    Extension(api): Extension<utoipa::openapi::OpenApi>,
) -> Result<Html<String>, StatusCode> {
    let context = html::ApiDocsContext::new(state.root_uri.clone(), session.user, &api);
    let raw = server_error(context.render())?;
    Ok(Html(raw))
}
