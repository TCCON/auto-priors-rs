use axum::{http::StatusCode, response::IntoResponse};
use chrono::NaiveDate;
use orm::{
    error::JobAddError,
    jobs::{MapFmt, ModFmt, RequestSite, VmrFmt},
};
use serde::{Deserialize, Serialize};

pub(crate) mod post {
    use std::sync::Arc;

    use axum::{
        extract::{self, State},
        Extension,
    };
    use orm::{auth::User, jobs::Job};

    use crate::{
        api::jobs::{ApiJobRequest, ApiJobResult},
        AppState,
    };

    pub(crate) async fn submit_job(
        State(state): State<Arc<AppState>>,
        Extension(user): Extension<User>,
        extract::Json(job_request): extract::Json<ApiJobRequest>,
    ) -> super::ApiJobRequestResponse {
        if job_request.sites.len() == 0 {
            return super::ApiJobRequestResponse::RequestError(super::RequestError::NoSites);
        }

        let res = state.clone_pool().get_connection().await;
        let mut conn = match res {
            Ok(c) => c,
            Err(e) => return super::ApiJobRequestResponse::ServerError(e.into()),
        };
        let res = Job::add_job_from_request(
            &mut conn,
            &state.config,
            job_request.sites,
            job_request.start_date,
            job_request.end_date,
            Some(user.email),
            job_request.mod_fmt,
            job_request.vmr_fmt,
            job_request.map_fmt,
            job_request.reanalysis.as_deref(),
            job_request.is_egi,
        )
        .await;

        match res {
            Ok(job_ids) => super::ApiJobRequestResponse::Success(ApiJobResult { job_ids }),
            Err(e) => super::ApiJobRequestResponse::from(e),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ApiJobRequest {
    start_date: NaiveDate,
    end_date: NaiveDate,
    sites: Vec<RequestSite>,
    // #[serde(default)] // TODO: update database and job completion code to handle multiple emails
    // alternate_emails: Vec<String>,
    #[serde(default)]
    mod_fmt: Option<ModFmt>,
    #[serde(default)]
    vmr_fmt: Option<VmrFmt>,
    #[serde(default)]
    map_fmt: Option<MapFmt>,
    #[serde(default)]
    reanalysis: Option<String>,
    #[serde(default)]
    is_egi: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ApiJobResult {
    job_ids: Vec<i32>,
}

pub(crate) enum ApiJobRequestResponse {
    Success(ApiJobResult),
    RequestError(RequestError),
    ServerError(anyhow::Error),
}

impl IntoResponse for ApiJobRequestResponse {
    fn into_response(self) -> axum::response::Response {
        match self {
            ApiJobRequestResponse::Success(res) => axum::response::Json(res).into_response(),
            ApiJobRequestResponse::RequestError(request_error) => request_error.into_response(),
            ApiJobRequestResponse::ServerError(err) => {
                log::error!("Error occurred while processing job submission request: {err:?}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

impl From<JobAddError> for ApiJobRequestResponse {
    fn from(value: JobAddError) -> Self {
        if value.is_server_error() {
            Self::ServerError(value.into())
        } else {
            Self::RequestError(value.into())
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RequestError {
    #[error("No sites/locations specified")]
    NoSites,
    #[error("{0}")]
    JobAddError(#[from] JobAddError),
}

impl IntoResponse for RequestError {
    fn into_response(self) -> axum::response::Response {
        axum::response::Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(self.to_string().into())
            .expect("Response construction should not fail")
    }
}
