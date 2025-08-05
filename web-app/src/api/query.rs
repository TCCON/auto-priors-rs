use chrono::{NaiveDate, NaiveDateTime};
use itertools::Itertools;
use orm::jobs::{Job, JobState, MapFmt, ModFmt, VmrFmt};
use serde::{Deserialize, Serialize};

use crate::api::download::download_url_for_job;

pub(crate) mod get {
    use std::sync::Arc;

    use axum::{
        extract::{Path, Query, State},
        http::StatusCode,
        Extension, Json,
    };
    use chrono::NaiveDate;
    use itertools::Itertools;
    use orm::{auth::User, jobs::Job};

    use crate::{
        api::query::{ApiDisplayJob, JobQueryDates},
        server_error, AppState,
    };

    /// Endpoint to return all jobs a user has submitted in the past, including ones that have been
    /// cleaned up. It accepts "before" and "after" query parameters to limit by submission time:
    /// e.g., "?after=YYYY-MM-DD"&before=YYYY-MM-DD" - both are optional.
    pub(crate) async fn query_all_jobs(
        State(state): State<Arc<AppState>>,
        Query(dates): Query<JobQueryDates>,
        Extension(user): Extension<User>,
    ) -> Result<Json<Vec<ApiDisplayJob>>, StatusCode> {
        let mut conn = server_error(state.pool.get_connection().await)?;
        let root_uri = &state.root_uri;
        log::debug!("Getting all jobs for user {}", user.username);
        let jobs = server_error(
            Job::get_jobs_for_user(&mut conn, &user.email, dates.after, dates.before).await,
        )?;
        let res: Result<Vec<ApiDisplayJob>, _> = jobs
            .into_iter()
            .map(|j| ApiDisplayJob::try_from_job(j, root_uri))
            .try_collect();
        let display_jobs = server_error(res)?;
        Ok(Json(display_jobs))
    }

    /// Endpoint to return jobs a user has submitted in the past, excluding ones that have been
    /// cleaned up. It accepts "before" and "after" query parameters to limit by submission time:
    /// e.g., "?after=YYYY-MM-DD"&before=YYYY-MM-DD" - both are optional.
    pub(crate) async fn query_active_jobs(
        State(state): State<Arc<AppState>>,
        Query(dates): Query<JobQueryDates>,
        Extension(user): Extension<User>,
    ) -> Result<Json<Vec<ApiDisplayJob>>, StatusCode> {
        server_error(query_active_jobs_inner(state, user, dates.after, dates.before).await)
    }

    /// Inner method for `query_active_jobs` to simplify error handling.
    async fn query_active_jobs_inner(
        state: Arc<AppState>,
        user: User,
        submitted_after: Option<NaiveDate>,
        submitted_before: Option<NaiveDate>,
    ) -> anyhow::Result<Json<Vec<ApiDisplayJob>>> {
        let mut conn = state.pool.get_connection().await?;
        let jobs = Job::get_active_jobs_for_user_submitted_between(
            &mut conn,
            &user.email,
            submitted_after,
            submitted_before,
        )
        .await?;

        let root_uri = &state.root_uri;
        let display_jobs: Vec<ApiDisplayJob> = jobs
            .into_iter()
            .map(|j| ApiDisplayJob::try_from_job(j, root_uri))
            .try_collect()?;
        Ok(Json(display_jobs))
    }

    /// Endpoint to query the status of a specific job by its database ID.
    /// The job ID must be given as part of its URL path.
    pub(crate) async fn query_job(
        State(state): State<Arc<AppState>>,
        Extension(user): Extension<User>,
        Path(job_id): Path<i32>,
    ) -> Result<Json<Option<ApiDisplayJob>>, StatusCode> {
        let mut conn = server_error(state.pool.get_connection().await)?;
        let root_uri = &state.root_uri;
        let res = Job::get_job_with_id(&mut conn, job_id).await;
        let job = match res {
            Ok(job) => job,
            Err(e) => {
                log::info!(
                    "User {} query for job ID {job_id} returned error ({e}), returning null",
                    user.username
                );
                return Ok(Json(None));
            }
        };

        // TODO: replace with check against username (will require update to the jobs database)
        // TODO: implement "user groups" that would allow users to access each other's jobs.
        if job
            .email
            .as_deref()
            .is_some_and(|email| email == &user.email)
        {
            let display_job = server_error(ApiDisplayJob::try_from_job(job, root_uri))?;
            Ok(Json(Some(display_job)))
        } else {
            log::debug!("User {} query for job ID {job_id} returned a job for a different user, returning null", user.username);
            Ok(Json(None))
        }
    }
}

/// Structure to deserialize "before" and "after" query parameters in URLs.
#[derive(Debug, Deserialize)]
pub(crate) struct JobQueryDates {
    after: Option<NaiveDate>,
    before: Option<NaiveDate>,
}

/// Structure used to serialize job information to return a JSON result to users.
#[derive(Debug, Serialize)]
pub(crate) struct ApiDisplayJob {
    job_id: i32,
    state: JobState,
    sites: Vec<super::ApiJobSite>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    email: Option<String>,
    met_key: Option<String>,
    ginput_key: Option<String>,
    mod_fmt: ModFmt,
    vmr_fmt: VmrFmt,
    map_fmt: MapFmt,
    submit_time: NaiveDateTime,
    complete_time: Option<NaiveDateTime>,
    download_url: Option<String>,
}

impl ApiDisplayJob {
    /// Convert a database job to an API display job. The resulting display job
    /// will have `download_url = `Some(_)` if it has completed, and that
    /// will be constructed from the given `root_uri`.
    fn try_from_job(value: Job, root_uri: &str) -> anyhow::Result<Self> {
        if value.site_id.len() != value.lat.len() {
            anyhow::bail!("Site IDs and lats are not equal in length")
        }
        if value.site_id.len() != value.lon.len() {
            anyhow::bail!("Site IDs and lons are not equal in length")
        }

        let download_url = download_url_for_job(&value, root_uri);
        let sites = value
            .site_id
            .into_iter()
            .enumerate()
            .map(|(i, sid)| super::ApiJobSite {
                site_id: sid,
                lat: value.lat[i],
                lon: value.lon[i],
            })
            .collect_vec();

        Ok(Self {
            job_id: value.job_id,
            state: value.state,
            sites,
            start_date: value.start_date,
            end_date: value.end_date,
            email: value.email,
            met_key: value.met_key,
            ginput_key: value.ginput_key,
            mod_fmt: value.mod_fmt,
            vmr_fmt: value.vmr_fmt,
            map_fmt: value.map_fmt,
            submit_time: value.submit_time,
            complete_time: value.complete_time,
            download_url,
        })
    }
}
