use askama_axum::Template;

use crate::{auth::User, templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink}};

#[derive(Debug, Template)]
#[template(path = "met-data.html")]
struct MetDataContext {
    root_uri: String,
    user: Option<User>
}

impl MetDataContext {
    fn new(root_uri: String, user: Option<User>) -> Self {
        Self { root_uri, user }
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
    
    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
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

    use crate::{auth::AuthSession, AppStateRef};

    use super::MetDataContext;

    pub(crate) async fn met_data(State(state): AppStateRef, session: AuthSession) -> Result<impl IntoResponse, StatusCode> {
        let context = MetDataContext::new(state.root_uri.clone(), session.user);
        Ok(context)
    }
}