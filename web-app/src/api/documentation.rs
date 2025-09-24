use axum::{routing::get, Extension, Json, Router};

pub(crate) struct DocEndpoint {
    api: utoipa::openapi::OpenApi,
    json_url: Option<String>,
}

impl<S: Clone + Send + Sync + 'static> From<DocEndpoint> for Router<S> {
    fn from(value: DocEndpoint) -> Self {
        let router = Router::new();
        let router = if let Some(url) = value.json_url {
            router.route(&url, get(openapi_json))
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

pub(crate) struct DocEndpointBuilder {
    api: utoipa::openapi::OpenApi,
    json_url: Option<String>,
}

impl DocEndpointBuilder {
    pub(crate) fn new(api: utoipa::openapi::OpenApi) -> Self {
        Self {
            api,
            json_url: None,
        }
    }

    pub(crate) fn json_url<S: ToString>(mut self, url: S) -> Self {
        self.json_url = Some(url.to_string());
        self
    }

    pub(crate) fn build(self) -> DocEndpoint {
        DocEndpoint {
            api: self.api,
            json_url: self.json_url,
        }
    }
}

pub(crate) async fn openapi_json(
    Extension(api): Extension<utoipa::openapi::OpenApi>,
) -> Json<utoipa::openapi::OpenApi> {
    Json(api)
}
