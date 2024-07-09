use axum::{Form, extract::Query, http::StatusCode, response::{Html, IntoResponse, Redirect}};
use serde::Deserialize;
use crate::{auth::{AuthSession, Credentials}, templates::{make_base_context, TEMPLATES}};

// Following https://github.com/maxcountryman/axum-login/blob/main/examples/sqlite/src/web/auth.rs
#[derive(Debug, Deserialize)]
pub(crate) struct NextUrl {
    next: Option<String>
}

pub(crate) mod get {
    use super::*;

    pub(crate) async fn login(Query(NextUrl { next }): Query<NextUrl>) -> Result<Html<String>, StatusCode> {
        let mut context = make_base_context("Login", "login");
        context.insert("has_next", &next.is_some());
        context.insert("next_url", &next.unwrap_or_default());
        let page_source = TEMPLATES.render("login.html", &context).unwrap();
        Ok(Html(page_source))
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