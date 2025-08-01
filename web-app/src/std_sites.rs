use askama::Template;

use crate::{
    auth::User,
    templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink},
};

#[derive(Debug, Template)]
#[template(path = "std-sites.html")]
struct StdSitesContext {
    root_uri: String,
    user: Option<User>,
}

impl StdSitesContext {
    fn new(root_uri: String, user: Option<User>) -> Self {
        Self { root_uri, user }
    }
}

impl BaseContext for StdSitesContext {
    fn subtitle(&self) -> &str {
        "Standard sites"
    }

    fn page_id(&self) -> &str {
        "std-sites"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }

    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }
}

impl ContextWithSidebar for StdSitesContext {
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
    use axum::{extract::State, http::StatusCode};

    use crate::{auth::AuthSession, server_error, AppStateRef};

    use super::StdSitesContext;

    pub(crate) async fn std_sites(
        State(state): AppStateRef,
        session: AuthSession,
    ) -> Result<String, StatusCode> {
        let context = StdSitesContext::new(state.root_uri.clone(), session.user);
        server_error(context.render())
    }
}
