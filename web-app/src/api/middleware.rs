// I thought I needed to implement an all-up service following https://github.com/tower-rs/tower/blob/master/guides/building-a-middleware-from-scratch.md
// so that I can have different permissions for different routes and access to the database.
// However, it looks like axum's [from_fn_with_state](https://docs.rs/axum/latest/axum/middleware/fn.from_fn_with_state.html)
// would allow access to the database, then I would just create wrapper functions for each permission.

use std::sync::Arc;

use axum::response::IntoResponse;
use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use jsonwebtoken::DecodingKey;
use orm::error::ApiAuthError;
use orm::{
    auth::{api::authenticate_refresh_token, Permission},
    MySqlConn,
};

use crate::AppState;

// Note: the middleware functions cannot use crate::AppStateRef because, according to Gemini,
// the type alias hides the `State` extractor in such a way that axum cannot figure out how
// to convert a reference to the router's state into the State extractor.

pub(crate) async fn api_has_query_perm(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    api_has_perm(&state, headers, Permission::Query, request, next).await
}

pub(crate) async fn api_has_submit_perm(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    api_has_perm(&state, headers, Permission::Submit, request, next).await
}

pub(crate) async fn api_has_download_perm(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    api_has_perm(&state, headers, Permission::Download, request, next).await
}

async fn api_has_perm(
    state: &AppState,
    headers: HeaderMap,
    perm: Permission,
    request: Request,
    next: Next,
) -> Response {
    let token = match get_token_from_header(&headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let mut conn = if let Ok(conn) = state.pool.get_connection().await {
        conn
    } else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };

    match key_has_perm(&mut conn, token, &state.decoding_key, perm).await {
        Ok(true) => (),
        Ok(false) => return StatusCode::FORBIDDEN.into_response(),
        Err(ApiAuthError::TokenExpiredOnServer) | Err(ApiAuthError::TokenInvalidExpired) => {
            let resp = Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body("ERROR: Token has expired".into())
                .expect("Response must be of valid construction");
            return resp;
        }
        Err(ApiAuthError::TokenInvalidOther(_)) => {
            let resp = Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body("ERROR: Token is invalid".into())
                .expect("Response must be of valid construction");
            return resp;
        }
        Err(ApiAuthError::TokenNotFound) => {
            let resp = Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body("ERROR: Token is not associated with a user".into())
                .expect("Response must be of valid construction");
            return resp;
        }
        Err(ApiAuthError::SqlError(e)) => {
            log::error!("An SQL error occurred while validating a user API token: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        Err(ApiAuthError::Other(e)) => {
            log::error!("An unexpected error occurred while validating a user API token: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    next.run(request).await
}

fn get_token_from_header<'h>(headers: &'h HeaderMap) -> Result<&'h str, Response> {
    let auth_header = if let Some(header) = headers.get("Authorization") {
        header
    } else {
        let resp = Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body("ERROR: Request must contain an 'Authorization' header".into())
            .expect("Response must be of valid construction");
        return Err(resp);
    };

    let auth_header = if let Ok(header) = auth_header.to_str() {
        header
    } else {
        let resp = Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body("ERROR: Request 'Authorization' header must be a valid UTF-8 string".into())
            .expect("Response must be of valid construction");
        return Err(resp);
    };

    let (scheme, token) = if let Some((s, t)) = auth_header.split_once(' ') {
        (s, t)
    } else {
        let resp = Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(
                "ERROR: Request 'Authorization' value must be a scheme and token, separated by a space"
                    .into(),
            )
            .expect("Response must be of valid construction");
        return Err(resp);
    };

    if scheme.trim() != "Bearer" {
        let resp = Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body("ERROR: Request 'Authorization' header must have the 'Bearer' scheme".into())
            .expect("Response must be of valid construction");
        return Err(resp);
    }

    Ok(token.trim())
}

async fn key_has_perm(
    conn: &mut MySqlConn,
    token: &str,
    decoding_key: &DecodingKey,
    perm: Permission,
) -> Result<bool, ApiAuthError> {
    let user_perms = authenticate_refresh_token(conn, token, decoding_key).await?;
    Ok(user_perms.has_perm(&perm))
}
