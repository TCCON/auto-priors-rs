use askama_axum::Template;

use crate::templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink};

#[derive(Template)]
#[template(path="home.html")]
struct HomeContext {
    root_uri: String,

}

impl HomeContext {
    pub(crate) fn new(root_uri: String) -> Self {
        Self { root_uri }
    }
}

impl BaseContext for HomeContext {
    fn subtitle(&self) -> &str {
        "Home"
    }

    fn page_id(&self) -> &str {
        "home"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }
}

impl ContextWithSidebar for HomeContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

pub(crate) mod get {
    use askama_axum::IntoResponse;
    use axum::{extract::State, http::StatusCode};

    use crate::{home::HomeContext, AppStateRef};

    pub(crate) async fn home(State(state): AppStateRef) -> Result<impl IntoResponse, StatusCode> {
        let context = HomeContext::new(state.root_uri.clone());
        Ok(context)
    }
}