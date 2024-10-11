use askama_axum::Template;
use axum::{Form, extract::Query, http::StatusCode, response::{IntoResponse, Redirect}};
use serde::Deserialize;
use crate::{auth::{AuthSession, Credentials}, templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink}};

// Following https://github.com/maxcountryman/axum-login/blob/main/examples/sqlite/src/web/auth.rs
#[derive(Debug, Deserialize)]
pub(crate) struct NextUrl {
    next: Option<String>
}

#[derive(Debug, Template)]
#[template(path="login.html")]
struct LoginContext {
    root_uri: String,
    next_url: Option<String>,
}

impl LoginContext {
    fn new(root_uri: String, next_url: Option<String>) -> Self {
        Self { root_uri, next_url }
    }
}

impl BaseContext for LoginContext {
    fn subtitle(&self) -> &str {
        "Login"
    }

    fn page_id(&self) -> &str {
        "login"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }
}

impl ContextWithSidebar for LoginContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

pub(crate) mod get {
    use axum::extract::State;

    use crate::AppStateRef;

    use super::*;

    pub(crate) async fn login(Query(NextUrl { next }): Query<NextUrl>, State(state): AppStateRef) -> Result<impl IntoResponse, StatusCode> {
        // let mut context = make_base_context("Login", "login");
        // context.insert("has_next", &next.is_some());
        // context.insert("next_url", &next.unwrap_or_default());
        // let page_source = TEMPLATES.render("login.html", &context).unwrap();
        // Ok(Html(page_source))

        let context = LoginContext::new(state.root_uri.clone(), next);
        Ok(context)
    }

    pub(crate) async fn logout(mut auth_session: AuthSession) -> impl IntoResponse {
        match auth_session.logout().await {
            Ok(_) => Redirect::to("/").into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub(crate) mod post {
    use super::*;

    pub(crate) async fn login(mut auth_session: AuthSession, Form(creds): Form<Credentials>) -> impl IntoResponse {
        let user = match auth_session.authenticate(creds.clone()).await {
            Ok(Some(user)) => user,
            Ok(None) => {
                // TODO: add error message
                let login_url = if let Some(next) = creds.next {
                    format!("/login?next={next}")
                } else {
                    "/login".to_string()
                };

                return Redirect::to(&login_url).into_response();
            },
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response()
        };

        if auth_session.login(&user).await.is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        
        if let Some(next) = creds.next {
            Redirect::to(&next).into_response()
        } else {
            Redirect::to("/").into_response()
        }
    }
}