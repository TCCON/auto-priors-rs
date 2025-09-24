use std::path::PathBuf;

use axum::{body::Body, http::StatusCode, response::IntoResponse};
use orm::{auth::User, jobs::Job};
use utoipa::{
    openapi::{Object, Schema, SchemaFormat},
    PartialSchema,
};

use crate::{api::ApiNaiveDate, server_error};

/// A type used to indicate that an API endpoint returns a gzipped TAR file.
pub(crate) struct TgzFileStream {
    output_file: PathBuf,
    body: Body,
}

impl TgzFileStream {
    async fn new(output_file: PathBuf) -> Result<Self, StatusCode> {
        let body = make_download_body(&output_file).await?;
        Ok(Self { output_file, body })
    }
}

impl IntoResponse for TgzFileStream {
    fn into_response(self) -> axum::response::Response {
        let content_type = "application/gzip".to_string();
        let headers = match make_download_headers(&self.output_file, content_type) {
            Ok(headers) => headers,
            Err(code) => return code.into_response(),
        };
        (headers, self.body).into_response()
    }
}

impl utoipa::PartialSchema for TgzFileStream {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        // This schema is intended to match the recommendation at
        // https://swagger.io/docs/specification/v3_0/describing-responses/#response-that-returns-a-file
        let binary_string = Object::builder()
            .schema_type(utoipa::openapi::Type::String)
            .format(Some(SchemaFormat::KnownFormat(
                utoipa::openapi::KnownFormat::Binary,
            )))
            .build();

        utoipa::openapi::RefOr::T(Schema::Object(binary_string))
    }
}

impl utoipa::ToSchema for TgzFileStream {
    fn schemas(
        schemas: &mut Vec<(
            String,
            utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
        )>,
    ) {
        schemas.push((ApiNaiveDate::name().into(), ApiNaiveDate::schema()));
    }
}

pub(crate) mod get {
    use std::sync::Arc;

    use axum::{
        body::Body,
        extract::{Path, State},
        http::{HeaderName, StatusCode},
        Extension,
    };
    use chrono::NaiveDate;
    use orm::{auth::User, jobs::Job, stdsitejobs::StdSiteJob};

    use crate::{
        api::download::{user_can_access_job, TgzFileStream},
        api::ApiNaiveDate,
        server_error, AppState,
    };

    /// Return a .tgz file with job output to the user.
    ///
    /// # Success
    /// Returns a response containing the file stream
    ///
    /// # Failure
    /// Returns the following status codes on failure:
    /// - `BAD_REQUEST` (400) is returned if:
    ///     1. the job requested could not be found or an error occurred,
    ///        assuming that the issue is that the job ID was not in the database, or
    ///     2. the job requested output its data as a directory, as such jobs are
    ///        not supported through this interface.
    /// - `FORBIDDEN` (403) if the job requested is not one the user is allowed to access.
    /// - `GONE` (410) if the job has already been cleaned up.
    /// - `NO_CONTENT` (204) if the job has not completed or the output file otherwise
    ///   does not exist. A 2xx status code is used as the user request was correct but
    ///   could not be fulfilled yet.
    #[utoipa::path(
        get,
        path = "/api/v1/download/job/{job_id}",
        responses(
            (status = StatusCode::OK, description = "Download request succeeded", body = TgzFileStream),
            (status = StatusCode::FORBIDDEN, description = "User does not have permission to access the requested job"),
            (status = StatusCode::GONE, description = "Output from the job was cleaned up and is no longer available"),
            (status = StatusCode::NO_CONTENT, description = "The output from the job is not available yet (usually because the job is not yet complete).")
        ),
        params(
            ("job_id" = i32, Path, description = "The ID of the job to download the output from.")
        ),
        tag = "download"
    )]
    pub(crate) async fn download_job_output(
        State(state): State<Arc<AppState>>,
        Extension(user): Extension<User>,
        Path(job_id): Path<i32>,
    ) -> Result<TgzFileStream, StatusCode> {
        let mut conn = server_error(state.pool.get_connection().await)?;
        let res = Job::get_job_with_id(&mut conn, job_id).await;
        let job = match res {
            Ok(job) => job,
            Err(e) => {
                log::info!(
                    "User {} request for job ID {job_id} returned error ({e}), returning bad request",
                    user.username
                );
                return Err(StatusCode::BAD_REQUEST);
            }
        };

        // The user does not have the permission to access the output from this job.
        // I've decided to control user's access to other users' jobs to protect
        // user's information from each other by default.
        if !user_can_access_job(&user, &job) {
            return Err(StatusCode::FORBIDDEN);
        }

        // Check that the job has produced an output file and that file exists
        // (i.e., the job hasn't been cleaned up).
        let output_file = if let Some(p) = job.output_file {
            p
        } else {
            return Err(StatusCode::NO_CONTENT);
        };

        if job.state == orm::jobs::JobState::Cleaned {
            return Err(StatusCode::GONE);
        } else if !output_file.exists() {
            return Err(StatusCode::NO_CONTENT);
        }

        if output_file.extension().is_some_and(|ext| ext == "tgz") {
            // The job has a tarball, so we can send the file.
            let resp = TgzFileStream::new(output_file).await?;
            Ok(resp)
        } else {
            Err(StatusCode::BAD_REQUEST)
        }
    }

    /// Return a .tgz file with standard site output to the user.
    ///
    /// # Success
    /// Returns a response containing the file stream
    ///
    /// # Failure
    /// Returns the following status codes on failure:
    /// - `BAD_REQUEST` (400) is returned if there if there is no entry in the standard site job table
    ///   for the given site ID and date
    /// - `NO_CONTENT` (204) is returned if the tarfile field in the database was null. This usually means
    ///   that the job has not been finished yet.
    #[utoipa::path(
        get,
        path = "/api/v1/download/stdsite/{site_id}/{date}",
        responses(
            (status = StatusCode::OK, description = "Download request succeeded", body = TgzFileStream),
            (status = StatusCode::BAD_REQUEST, description = "The site and/or date was not in the database"),
            (status = StatusCode::NO_CONTENT, description = "The data for this site is not ready yet")
        ),
        params(
            ("site_id" = String, Path, description = "The two-character site ID of the site for which to download data"),
            ("date" = ApiNaiveDate, Path, description = "The date for which to download data, in YYYY-MM-DD format")
        ),
        tag = "download"
    )]
    pub(crate) async fn download_std_site_output(
        State(state): State<Arc<AppState>>,
        Path((site_id, date)): Path<(String, NaiveDate)>,
    ) -> Result<([(HeaderName, String); 2], Body), StatusCode> {
        let mut conn = server_error(state.pool.get_connection().await)?;

        // Unlike the user-submitted jobs, we allow anyone to download the standard site files
        // since the site locations are well-known.
        let site_job = server_error(
            StdSiteJob::get_std_job_for_site_on_date(&mut conn, &site_id, date).await,
        )?
        .ok_or(StatusCode::BAD_REQUEST)?;

        let tar_file = site_job.tarfile.ok_or(StatusCode::NO_CONTENT)?;
        if tar_file.extension().is_some_and(|ext| ext == "tgz") {
            // Standard sites should ALWAYS be tarballs; if not, then that is a server error.
            let content_type = "application/gzip".to_string();
            let headers = super::make_download_headers(&tar_file, content_type)?;
            let body = super::make_download_body(&tar_file).await?;
            Ok((headers, body))
        } else {
            log::error!("Standard site '{site_id}' on {date} had a 'tar' file with an unexpected extension, file was '{}'", tar_file.display());
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn make_download_headers(
    output_file: &std::path::Path,
    content_type: String,
) -> Result<[(axum::http::header::HeaderName, String); 2], StatusCode> {
    let name = output_file
        .file_name()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
        .to_string_lossy();
    let name = urlencoding::encode(&name);
    let content_disp = format!(r#"attachment; filename="{name}""#);
    let headers = [
        (axum::http::header::CONTENT_TYPE, content_type),
        (axum::http::header::CONTENT_DISPOSITION, content_disp),
    ];
    Ok(headers)
}

async fn make_download_body(output_file: &std::path::Path) -> Result<axum::body::Body, StatusCode> {
    let file = server_error(tokio::fs::File::open(output_file).await)?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = axum::body::Body::from_stream(stream);
    Ok(body)
}

pub(crate) fn download_url_for_job(job: &Job, root_uri: &str) -> Option<String> {
    if job.output_file.is_none() {
        None
    } else {
        let root_uri = root_uri.trim_end_matches('/');
        Some(format!("{root_uri}/api/v1/download/job/{}", job.job_id))
    }
}

pub(crate) fn user_can_access_job(user: &User, job: &Job) -> bool {
    // TODO: update with user name check and (eventually) access group check

    // For now, we rely on the email submitted for the job being the email
    // defined by the user. Once I modify the database, we can check the user
    // ID of the submitted job instead, which will be more robust. Further,
    // we eventually want to permit access by different users to the same job,
    // to support groups where different people might submit and access the
    // job.
    if let Some(job_email) = &job.email {
        job_email == &user.email
    } else {
        false
    }
}
