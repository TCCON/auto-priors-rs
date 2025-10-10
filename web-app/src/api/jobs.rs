use axum::{http::StatusCode, response::IntoResponse};
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

    /// Submit a job request.
    ///
    /// This endpoint allows users to programmatically request priors be generated
    /// for one or more locations for a given time range.
    #[utoipa::path(
        post,
        path = "/api/v1/jobs/submit",
        responses(
            (status = StatusCode::OK, description = "Job submitted successfully", body = super::ApiJobRequestResponse, example = json!({"successful": true, "job_ids": [1], "error_reason": null})),
            (status = StatusCode::BAD_REQUEST, description = "The submission could not be processed due to incorrect parameters", body = super::ApiJobRequestResponse),
            (status = StatusCode::INTERNAL_SERVER_ERROR, description = "The submission could not be processed due to an internal server error", body = super::ApiJobRequestResponse)
        ),
        request_body(
            content=ApiJobRequest, examples(
                ("simple request" = (value=json!({"start_date": "2025-01-01", "end_date": "2025-01-02", "sites": [{"site_id": "aa", "lat": 12.34, "lon": -56.78}]}))),
                ("complex request" = (value=json!({"start_date": "2025-01-01", "end_date": "2025-01-02", "sites": [{"site_id": "aa", "lat": 12.34, "lon": -56.78}, {"site_id": "bb", "lat": -43.21, "lon": 87.65}], "mod_fmt": "none", "vmr_fmt": "none", "map_fmt": "netcdf"})))
            )
        ),
        tag = "submission"
    )]
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
            Err(e) => return super::ApiJobRequestResponse::ServerError(e.to_string()),
        };
        let res = Job::add_job_from_request(
            &mut conn,
            &state.config,
            job_request.sites,
            job_request.start_date.into_date(),
            job_request.end_date.into_date(),
            Some(user.email),
            job_request.mod_fmt,
            job_request.vmr_fmt,
            job_request.map_fmt,
            job_request.reanalysis.as_deref(),
            job_request.is_egi,
        )
        .await;

        match res {
            Ok(job_ids) => {
                super::ApiJobRequestResponse::Success(ApiJobResult::new_success(job_ids))
            }
            Err(e) => super::ApiJobRequestResponse::from(e),
        }
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(crate) struct ApiJobRequest {
    start_date: crate::api::ApiNaiveDate,
    end_date: crate::api::ApiNaiveDate,
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

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ApiJobResult {
    successful: bool,
    job_ids: Option<Vec<i32>>,
    error_reason: Option<String>,
}

impl ApiJobResult {
    fn new_success(job_ids: Vec<i32>) -> Self {
        Self {
            successful: true,
            job_ids: Some(job_ids),
            error_reason: None,
        }
    }

    fn new_failure<R: ToString>(reason: R) -> Self {
        Self {
            successful: false,
            job_ids: None,
            error_reason: Some(reason.to_string()),
        }
    }
}

#[derive(Debug, utoipa::ToSchema)]
pub(crate) enum ApiJobRequestResponse {
    Success(ApiJobResult),
    RequestError(RequestError),
    ServerError(String),
}

impl IntoResponse for ApiJobRequestResponse {
    fn into_response(self) -> axum::response::Response {
        match self {
            ApiJobRequestResponse::Success(res) => axum::response::Json(res).into_response(),
            ApiJobRequestResponse::RequestError(request_error) => {
                let res = ApiJobResult::new_failure(request_error);
                let mut resp = axum::response::Json(res).into_response();
                *resp.status_mut() = StatusCode::BAD_REQUEST;
                resp
            }
            ApiJobRequestResponse::ServerError(err) => {
                log::error!("Error occurred while processing job submission request: {err:?}");
                let res = ApiJobResult::new_failure("Internal server error");
                let mut resp = axum::response::Json(res).into_response();
                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                resp
            }
        }
    }
}

impl From<JobAddError> for ApiJobRequestResponse {
    fn from(value: JobAddError) -> Self {
        if value.is_server_error() {
            Self::ServerError(value.to_string())
        } else {
            Self::RequestError(value.into())
        }
    }
}

impl From<anyhow::Error> for ApiJobRequestResponse {
    fn from(value: anyhow::Error) -> Self {
        Self::ServerError(format!("{value:?}"))
    }
}

#[derive(Debug, thiserror::Error, utoipa::ToSchema)]
pub(crate) enum RequestError {
    #[error("No sites/locations specified")]
    NoSites,
    #[error("{0}")]
    JobAddError(String),
}

impl IntoResponse for RequestError {
    fn into_response(self) -> axum::response::Response {
        axum::response::Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(self.to_string().into())
            .expect("Response construction should not fail")
    }
}

impl From<JobAddError> for RequestError {
    fn from(value: JobAddError) -> Self {
        Self::JobAddError(value.to_string())
    }
}
