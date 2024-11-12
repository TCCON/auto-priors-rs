use std::{collections::HashMap, path::PathBuf};

use askama_axum::Template;
use chrono::NaiveDate;
use itertools::{izip, Itertools};

use orm::{input_files::{InputJob, InputJobBuilder}, jobs::{FairShare, Job, JobState, MapFmt, ModFmt, VmrFmt}, utils, MySqlConn};

use crate::{auth::User, templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink}, AppState};

#[derive(Debug, serde::Serialize)]
#[allow(dead_code)]
pub(crate) struct DisplayJob {
    pub(crate) state: String,
    pub(crate) short_site_locs: String,
    pub(crate) long_site_locs: String,
    pub(crate) start_date: String,
    pub(crate) end_date: String,
    pub(crate) email: String,
    pub(crate) delete_time: String,
    pub(crate) met_key: String,
    pub(crate) ginput_key: String,
    pub(crate) save_tarball: String,
    pub(crate) mod_fmt: String,
    pub(crate) vmr_fmt: String,
    pub(crate) map_fmt: String,
    pub(crate) submit_time: String,
    pub(crate) complete_time: String,
    output_file: Option<PathBuf>
}

impl DisplayJob {
    fn column_head(column_name: &str) -> &str {
        match column_name {
            "state" => "State",
            "short_site_locs" => "Locations",
            "long_site_locs" => "Locations",
            "start_date" => "Start Date",
            "end_date" => "End Date",
            "email" => "Submitter",
            "delete_time" => "Delete time (US/Pacific)",
            "met_key" => "Met",
            "ginput_key" => "Ginput version",
            "save_tarball" => "Output format",
            "mod_fmt" => ".mod format",
            "vmr_fmt" => ".vmr format",
            "map_fmt" => ".map format",
            "submit_time" => "Submission time (US/Pacific)",
            "complete_time" => "Completion time (US/Pacific)",
            _ => column_name
        }
    }

    fn get(&self, column_name: &str) -> &str {
        match column_name {
            "state" => &self.state,
            "short_site_locs" => &self.short_site_locs,
            "long_site_locs" => &self.long_site_locs,
            "start_date" => &self.start_date,
            "end_date" => &self.end_date,
            "email" => &self.email,
            "delete_time" => &self.delete_time,
            "met_key" => &self.met_key,
            "ginput_key" => &self.ginput_key,
            "save_tarball" => &self.save_tarball,
            "mod_fmt" => &self.mod_fmt,
            "vmr_fmt" => &self.vmr_fmt,
            "map_fmt" => &self.map_fmt,
            "submit_time" => &self.submit_time,
            "complete_time" => &self.complete_time,
            _ => "N/A"
        }
    }
}

impl From<Job> for DisplayJob {
    fn from(job: Job) -> Self {
        let state = job.state.to_string();
        // 20 characters should be enough for 1 site with lat and lon.
        let (short_site_locs, long_site_locs) = format_locs(&job.site_id, &job.lat, &job.lon, 20);
        let start_date = job.start_date.to_string();
        let end_date = job.end_date.to_string();
        let email = job.email.unwrap_or_else(|| "".to_string());
        let delete_time = job.delete_time.map(|dt| dt.to_string()).unwrap_or_else(|| "None".to_string());
        let met_key = job.met_key.unwrap_or_else(|| "Default".to_string());
        let ginput_key = job.ginput_key.unwrap_or_else(|| "Default".to_string());
        let save_tarball = job.save_tarball.to_string();
        let mod_fmt = job.mod_fmt.to_string();
        let vmr_fmt = job.vmr_fmt.to_string();
        let map_fmt = job.map_fmt.to_string();
        let submit_time = job.submit_time.to_string();
        let complete_time = job.complete_time.map(|dt| dt.to_string()).unwrap_or_else(|| "".to_string());
        let output_file = job.output_file;
        
        Self { 
            state,
            short_site_locs,
            long_site_locs,
            start_date,
            end_date,
            email,
            delete_time,
            met_key,
            ginput_key,
            save_tarball,
            mod_fmt,
            vmr_fmt,
            map_fmt,
            submit_time,
            complete_time,
            output_file
        }
    }
}

fn format_locs(site_ids: &[String], lats: &[Option<f32>], lons: &[Option<f32>], max_chars: usize) -> (String, String) {
    let mut long_form = String::new();
    for (idx, (sid, &lat, &lon)) in izip!(site_ids, lats, lons).enumerate() {
        let coord_string = match (lat, lon) {
            (None, None) => sid.to_string(),
            (None, Some(_)) => sid.to_string(),
            (Some(_), None) => sid.to_string(),
            (Some(y), Some(x)) => {
                let lat_str = utils::format_lat_str(y, 1);
                let lon_str = utils::format_lon_str(x, 1);
                format!("{sid} ({lat_str}, {lon_str})")
            },
        };

        if idx != 0 {
            long_form.push_str(", ");
        }
        long_form.push_str(&coord_string);
    }

    if long_form.len() <= max_chars {
        (long_form.clone(), long_form)
    } else {
        let short_form = if site_ids.len() == 1 {
            format!("{} location", site_ids.len())
        } else {
            format!("{} locations", site_ids.len())
        };
        (short_form, long_form)
    }
}

pub(crate) async fn make_queue_jobs_list(conn: &mut MySqlConn, config: &orm::config::Config) -> anyhow::Result<Vec<DisplayJob>> {
    let sub_queue = config.get_queue(&config.execution.submitted_job_queue)
        .unwrap_or_default();
    let jobs = sub_queue.fair_share_policy.list_jobs_in_order(
        conn, &config.execution.submitted_job_queue, Some(&[JobState::Pending, JobState::Running, JobState::Complete])
    ).await?;
    let jobs = jobs.into_iter().map(|j| DisplayJob::from(j)).collect_vec();
    Ok(jobs)
}

async fn get_user_jobs(conn: &mut MySqlConn, config: &orm::config::Config, user: &User, cleaned_and_errored_submitted_after: chrono::NaiveDateTime) -> anyhow::Result<UserJobs> {
    let all_emails = user.all_associated_emails(conn).await?;
    let mut jobs = vec![];
    for email in all_emails.iter() {
        let these_jobs = orm::jobs::Job::get_jobs_for_user(conn, email).await?;
        jobs.extend(these_jobs.into_iter());
    }
    let mut ready_jobs = vec![];
    let mut running = vec![];
    let mut pending = vec![];
    let mut cleaned = vec![];
    let mut errored = vec![];
    for job in jobs {
        match job.state {
            orm::jobs::JobState::Pending => pending.push(DisplayJob::from(job)),
            orm::jobs::JobState::Running => running.push(DisplayJob::from(job)),
            orm::jobs::JobState::Complete => ready_jobs.push(DisplayJob::from(job)),
            orm::jobs::JobState::Errored => {
                if job.submit_time >= cleaned_and_errored_submitted_after {
                    errored.push(DisplayJob::from(job));
                }
            },
            orm::jobs::JobState::Cleaned => {
                if job.submit_time >= cleaned_and_errored_submitted_after {
                    cleaned.push(DisplayJob::from(job));
                }
            },
        }
    }
    
    let ftp_download_server = config.execution.ftp_download_server.clone();
    let ftp_root = config.execution.ftp_download_root.clone();
    Ok(UserJobs{
        emails: all_emails, submitted_after: cleaned_and_errored_submitted_after, ready_for_download: ready_jobs, 
        running, pending, errored, cleaned, ftp_download_server, ftp_root
    })
}

struct UserJobs {
    emails: Vec<String>,
    ready_for_download: Vec<DisplayJob>,
    running: Vec<DisplayJob>,
    pending: Vec<DisplayJob>,
    errored: Vec<DisplayJob>,
    cleaned: Vec<DisplayJob>,
    submitted_after: chrono::NaiveDateTime,
    ftp_download_server: url::Url,
    ftp_root: PathBuf
}

impl UserJobs {
    fn all_ftp_download_links(&self) -> String {
        let mut links = String::with_capacity(self.ready_for_download.len() * 32);
        for (ijob, job) in self.ready_for_download.iter().enumerate() {
            if let Ok(link) = self.ftp_link(job) {
                if ijob > 0 { links.push('\n'); }
                links.push_str(link.as_str());
            } else {
                log::warn!("Tried to get an FTP link for a job without an output file");
            }
        }
        links
    }

    fn ftp_link(&self, job: &DisplayJob) -> anyhow::Result<url::Url> {
        if let Some(output) = &job.output_file {
            orm::jobs::get_ftp_path_from_dirs(&output, &self.ftp_download_server, &self.ftp_root)
                .map_err(|e| {
                    log::error!("error getting FTP link for job: {e}");
                    e
                })
        } else {
            anyhow::bail!("job has no output file yet")
        }
    }
}

#[derive(Template)]
#[template(path="queue_table.html")]
pub(crate) struct QueueTableContext<'a> {
    jobs: &'a[DisplayJob],
    columns: &'a[&'a str]
}

impl<'a> QueueTableContext<'a> {
    fn colname(&self, column: &'a str) -> &str {
        DisplayJob::column_head(column)
    }
}

fn make_jobs_queue_html(jobs: &[DisplayJob], columns: &[&str]) -> anyhow::Result<String> {
    let context = QueueTableContext{ jobs, columns };
    Ok(context.render()?)
}


#[derive(Debug, Template)]
#[template(path="job-queue.html")]
struct JobQueueContext {
    root_uri: String,
    user: Option<User>,
    queue_table: String,
}

impl JobQueueContext {
    async fn new_from_db(root_uri: String, user: Option<User>, state: std::sync::Arc<AppState>, columns: &[&str]) -> anyhow::Result<Self> {
        let mut conn = state.pool.get_connection().await?;
        let config = crate::load_automation_config()?;
        let jobs_list = make_queue_jobs_list(&mut conn, &config).await?;
        log::debug!("jobs_list.len() = {}", jobs_list.len());
        let queue_table = make_jobs_queue_html(&jobs_list, columns)?;
        Ok(Self { root_uri, user, queue_table })
    }
}

impl BaseContext for JobQueueContext {
    fn subtitle(&self) -> &str {
        "Job queue"
    }

    fn page_id(&self) -> &str {
        "job-queue"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }
    
    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }

    
}

impl ContextWithSidebar for JobQueueContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

#[derive(Template)]
#[template(path = "job-download.html")]
struct JobDownloadContext {
    root_uri: String,
    user: Option<User>,
    user_jobs: UserJobs
}

impl BaseContext for JobDownloadContext {
    fn subtitle(&self) -> &str {
        "Job downloads"
    }

    fn page_id(&self) -> &str {
        "job-downloads"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }

    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }
}

impl ContextWithSidebar for JobDownloadContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

#[derive(Debug, Template)]
#[template(path = "submit-job.html")]
struct SubmitJobContext {
    root_uri: String,
    user: Option<User>,
    errors: Vec<String>
}

impl SubmitJobContext {
    fn new(root_uri: String, user: Option<User>) -> Self {
        Self { root_uri, user, errors: vec![] }
    }

    fn new_with_errors(root_uri: String, user: Option<User>, errors: Vec<String> ) -> Self {
        Self { root_uri, user, errors }
    }
}

impl BaseContext for SubmitJobContext {
    fn subtitle(&self) -> &str {
        "Submit job"
    }

    fn page_id(&self) -> &str {
        "submit-job"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }
    
    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }

    
}

impl ContextWithSidebar for SubmitJobContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SubmitJobForm {
    #[serde(alias="start-date")]
    start_date: NaiveDate,
    #[serde(alias="end-date")]
    end_date: NaiveDate,
    #[serde(alias="mod-fmt")]
    mod_fmt: ModFmt,
    #[serde(alias="vmr-fmt")]
    vmr_fmt: VmrFmt,
    #[serde(alias="map-fmt")]
    map_fmt: MapFmt,
    #[serde(alias="confirmation-email")]
    confirmation: Option<String>,
    #[serde(flatten)]
    locations: HashMap<String, String>
}


impl SubmitJobForm {
    async fn to_input_job(self, user_email: String, conn: &mut MySqlConn, config: &orm::config::Config) -> Result<InputJob, Vec<String>> {
        let mut errors = vec![];
        // First we need to extract the site IDs and coordinates from the hash map.
        // Because we don't know how many locations there are, their form entries
        // have to be flattened into the hash map then reconstructed here, rather
        // than being explicit structure fields. Note that the forms labels start at 1.
        let mut loc_ind = 1;
        let mut site_ids = vec![];
        let mut site_lats = vec![];
        let mut site_lons = vec![];
        loop {
            let this_sid = self.locations.get(&format!("site-id-{loc_ind}"));
            if this_sid.is_none() {
                break;
            }

            let this_sid = this_sid.unwrap().trim().to_string();
            let this_lat = self.locations.get(&format!("site-lat-{loc_ind}"))
                .map(|s| if let Ok(v) = s.parse::<f32>() {
                    Some(v)
                } else {
                    errors.push(format!("latitude for site {this_sid} could not be parsed as a number"));
                    None
                }).flatten();

            let this_lon = self.locations.get(&format!("site-lon-{loc_ind}"))
                .map(|s| if let Ok(v) = s.parse::<f32>() {
                    Some(v)
                } else {
                    errors.push(format!("latitude for site {this_sid} could not be parsed as a number"));
                    None
                }).flatten();

            site_ids.push(this_sid);
            site_lats.push(this_lat);
            site_lons.push(this_lon);
            loc_ind += 1;
        }

        let mut builder = InputJobBuilder::default();
        builder.with_site_id(site_ids);
        builder.with_lats(site_lats);
        builder.with_lons(site_lons);
        builder.with_start_date(self.start_date);
        builder.with_end_date(self.end_date);
        builder.with_email(user_email);
        builder.with_mod_fmt(self.mod_fmt);
        builder.with_vmr_fmt(self.vmr_fmt);
        builder.with_map_fmt(self.map_fmt);
        builder.with_confirmation(self.confirmation.is_some()); // checkboxes are dumb are just aren't included in the form if not checked

        let job = match builder.finalize(conn, config).await {
            Ok(j) => j,
            Err(problems) => {
                errors.extend(problems);
                return Err(errors);
            }
        };
        
        if !errors.is_empty() {
            Err(errors)
        } else {
            Ok(job)
        }

    }
}

pub(crate) mod get {
    use askama_axum::IntoResponse;
    use axum::{extract::State, http::StatusCode};

    use crate::{auth::AuthSession, load_automation_config, server_error, AppStateRef};

    use super::{get_user_jobs, JobDownloadContext, JobQueueContext, SubmitJobContext};

    pub(crate) async fn job_queue(State(state): AppStateRef, session: AuthSession) -> Result<impl IntoResponse, StatusCode> {
        let res = JobQueueContext::new_from_db(
            state.root_uri.clone(),
            session.user,
            state,
            &["submit_time", "state", "short_site_locs", "start_date", "end_date", "email"]
        ).await;
        let context = server_error(res)?;
        Ok(context)
    }

    pub(crate) async fn job_download(State(state): AppStateRef, auth_session: AuthSession) -> Result<impl IntoResponse, StatusCode> {
        let user = if let Some(u) = auth_session.user {
            u
        } else {
            return Err(StatusCode::FORBIDDEN);
        };

        let mut conn = server_error(state.pool.get_connection().await)?;
        let config = server_error(load_automation_config())?;
        let after = chrono::Local::now().naive_local() - chrono::TimeDelta::weeks(2);

        // TODO: verify that running/pending, errored, and cleaned jobs show up correctly.
        let user_jobs = server_error(get_user_jobs(&mut conn, &config, &user, after).await)?;
        let context = JobDownloadContext{ root_uri: state.root_uri.clone(), user: Some(user), user_jobs };
        Ok(context)
    }

    pub(crate) async fn submit_job(State(state): AppStateRef, session: AuthSession) -> Result<impl IntoResponse, StatusCode> {
        let context = SubmitJobContext::new(state.root_uri.clone(), session.user);
        Ok(context)
    }
}

pub(crate) mod post {
    use askama_axum::IntoResponse;
    use axum::{extract::State, http::StatusCode, Form};

    use crate::{auth::AuthSession, load_automation_config, server_error, AppStateRef};

    use super::{SubmitJobContext, SubmitJobForm};

    pub(crate) async fn submit_job(State(state): AppStateRef, session: AuthSession, Form(job_req): Form<SubmitJobForm>)
    -> Result<impl IntoResponse, StatusCode>
    {
        let user_email = if let Some(user) = &session.user {
            user.email.clone()
        } else {
            return Err(StatusCode::UNAUTHORIZED)
        };

        let mut conn = server_error(state.pool.get_connection().await)?;
        let config = server_error(load_automation_config())?;

        let res = job_req.to_input_job(
            user_email,
            &mut conn,
            &config
        ).await;

        match res {
            // need to decouple blacklist and other final conversion logic from
            // orm::input_files::add_job_from_input_files
            Ok(job) => todo!(),
            Err(problems) => {
                let context = SubmitJobContext::new_with_errors(
                    state.root_uri.clone(),
                    session.user,
                    problems
                );
                Ok(context)
            }
        }
    }
}