use askama::Template;

use crate::{
    auth::User,
    templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink},
};

#[derive(Template)]
#[template(path = "home.html")]
struct HomeContext {
    root_uri: String,
    user: Option<User>,
}

impl HomeContext {
    pub(crate) fn new(root_uri: String, user: Option<User>) -> Self {
        Self { root_uri, user }
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

    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }
}

impl ContextWithSidebar for HomeContext {
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

    use crate::{auth::AuthSession, home::HomeContext, server_error, AppStateRef};

    pub(crate) async fn home(
        State(state): AppStateRef,
        session: AuthSession,
    ) -> Result<Html<String>, StatusCode> {
        let context = HomeContext::new(state.root_uri.clone(), session.user);
        let raw = server_error(context.render())?;
        Ok(Html(raw))
    }
}
