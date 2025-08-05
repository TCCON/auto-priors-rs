use axum::{http::StatusCode, response::IntoResponse};
use chrono::NaiveDate;
use orm::{
    error::JobAddError,
    jobs::{MapFmt, ModFmt, VmrFmt},
};
use serde::{Deserialize, Serialize};

pub(crate) mod post {
    use std::sync::Arc;

    use axum::{
        extract::{self, State},
        Extension,
    };
    use itertools::Itertools;
    use orm::{auth::User, jobs::Job};

    use crate::{api::jobs::ApiJobRequest, AppState};

    pub(crate) async fn submit_job(
        State(state): State<Arc<AppState>>,
        Extension(user): Extension<User>,
        extract::Json(job_request): extract::Json<ApiJobRequest>,
    ) -> super::ApiJobRequestResponse {
        if job_request.sites.len() == 0 {
            return super::ApiJobRequestResponse::RequestError(super::RequestError::NoSites);
        }

        let site_lats = job_request.sites.iter().map(|site| site.lat).collect_vec();
        let site_lons = job_request.sites.iter().map(|site| site.lon).collect_vec();
        let site_ids = job_request
            .sites
            .into_iter()
            .map(|site| site.site_id)
            .collect_vec();

        let res = state.clone_pool().get_connection().await;
        let mut conn = match res {
            Ok(c) => c,
            Err(e) => return super::ApiJobRequestResponse::ServerError(e.into()),
        };
        let res = Job::add_job_from_request(
            &mut conn,
            &state.config,
            site_ids,
            job_request.start_date,
            job_request.end_date,
            Some(user.email),
            site_lats,
            site_lons,
            job_request.mod_fmt,
            job_request.vmr_fmt,
            job_request.map_fmt,
            job_request.is_egi,
        )
        .await;

        match res {
            Ok(job_id) => super::ApiJobRequestResponse::Success(super::ApiJobResult { job_id }),
            Err(e) => e.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ApiJobRequest {
    start_date: NaiveDate,
    end_date: NaiveDate,
    sites: Vec<super::ApiJobSite>,
    // #[serde(default)] // TODO: update database and job completion code to handle multiple emails
    // alternate_emails: Vec<String>,
    mod_fmt: Option<ModFmt>,
    vmr_fmt: Option<VmrFmt>,
    map_fmt: Option<MapFmt>,
    #[serde(default)]
    is_egi: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ApiJobResult {
    job_id: i32,
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
        match value {
            JobAddError::DifferentNumSidLatLon {
                n_sid: _,
                n_lat: _,
                n_lon: _,
            } => Self::RequestError(value.into()),
            JobAddError::HalfNullCoord => Self::RequestError(value.into()),
            JobAddError::UnknownStdSid(items) => {
                Self::RequestError(JobAddError::UnknownStdSid(items).into())
            }
            JobAddError::InvalidUtf(_) => Self::RequestError(value.into()),
            JobAddError::SqlError(error) => Self::ServerError(error.into()),
            JobAddError::SerializationError(error) => Self::ServerError(error.into()),
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
