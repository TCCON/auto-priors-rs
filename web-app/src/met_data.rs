use askama_axum::Template;

use crate::templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink};

#[derive(Debug, Template)]
#[template(path = "met-data.html")]
struct MetDataContext {
    root_uri: String
}

impl MetDataContext {
    fn new(root_uri: String) -> Self {
        Self { root_uri }
    }
}

impl BaseContext for MetDataContext {
    fn subtitle(&self) -> &str {
        "Met data"
    }

    fn page_id(&self) -> &str {
        "met-data"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }
}

impl ContextWithSidebar for MetDataContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

pub(crate) mod get {
    use askama_axum::IntoResponse;
    use axum::{extract::State, http::StatusCode};

    use crate::AppStateRef;

    use super::MetDataContext;

    pub(crate) async fn met_data(State(state): AppStateRef) -> Result<impl IntoResponse, StatusCode> {
        let context = MetDataContext::new(state.root_uri.clone());
        Ok(context)
    }
}