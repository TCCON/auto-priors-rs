use askama_axum::Template;
use itertools::{izip, Itertools};

use orm::{MySqlConn, jobs::{Job, FairShare}, utils};

use crate::{templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink}, AppState};

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
    pub(crate) complete_time: String,
    pub(crate) download_url: String,
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
            "delete_time" => "Delete time",
            "met_key" => "Met",
            "ginput_key" => "Ginput version",
            "save_tarball" => "Output format",
            "mod_fmt" => ".mod format",
            "vmr_fmt" => ".vmr format",
            "map_fmt" => ".map format",
            "complete_time" => "Completion time",
            "download_url" => "Download URL",
            _ => column_name
        }
    }

    fn get(&self, column_name: &str) -> &str {
        match column_name {
            "state" => &self.state,
            _ => "N/A"
        }
    }
}

impl From<Job> for DisplayJob {
    fn from(job: Job) -> Self {
        let state = job.state.to_string();
        // 16 characters should be enough for 1 site with lat and lon.
        let (short_site_locs, long_site_locs) = format_locs(&job.site_id, &job.lat, &job.lon, 16);
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
        let complete_time = job.complete_time.map(|dt| dt.to_string()).unwrap_or_else(|| "".to_string());
        let download_url = "TBD".to_string(); // todo: if output present as file, transform into a link.
        
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
            complete_time,
            download_url
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
        let short_form = format!("{} locations", site_ids.len());
        (short_form, long_form)
    }
}

pub(crate) async fn make_queue_jobs_list(conn: &mut MySqlConn, config: &orm::config::Config) -> anyhow::Result<Vec<DisplayJob>> {
    let sub_queue = config.get_queue(&config.execution.submitted_job_queue)
        .unwrap_or_default();
    let jobs = sub_queue.fair_share_policy.list_jobs_in_order(
        conn, &config.execution.submitted_job_queue, true
    ).await?;
    let jobs = jobs.into_iter().map(|j| DisplayJob::from(j)).collect_vec();
    Ok(jobs)
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

/* pub(crate) fn make_jobs_queue_html(jobs: &[DisplayJob], columns: &[&str]) -> Result<String, axum::http::StatusCode> {
    let colnames = columns.iter()
        .map(|n| DisplayJob::column_head(n))
        .collect_vec();
    let mut context = Context::new();
    context.insert("jobs", jobs);
    context.insert("columns", columns);
    context.insert("colnames", &colnames);

    // TODO: handle mobile format too (not sure how yet)
    server_error(TEMPLATES.render("queue_table.html", &context))
} */

fn make_jobs_queue_html(jobs: &[DisplayJob], columns: &[&str]) -> anyhow::Result<String> {
    let context = QueueTableContext{ jobs, columns };
    Ok(context.render()?)
}

#[derive(Debug, Template)]
#[template(path="job-statuses.html")]
struct JobStatusContext {
    root_uri: String
}

impl JobStatusContext {
    fn new(root_uri: String) -> Self {
        Self { root_uri }
    }
}

impl BaseContext for JobStatusContext {
    fn subtitle(&self) -> &str {
        "Job statuses"
    }

    fn page_id(&self) -> &str {
        "job-statuses"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }
}

impl ContextWithSidebar for JobStatusContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

#[derive(Debug, Template)]
#[template(path="job-queue.html")]
struct JobQueueContext {
    root_uri: String,
    queue_table: String,
}

impl JobQueueContext {
    async fn new_from_db(root_uri: String, state: std::sync::Arc<AppState>, columns: &[&str]) -> anyhow::Result<Self> {
        let mut conn = state.pool.get_connection().await?;
        let config = crate::load_automation_config()?;
        let jobs_list = make_queue_jobs_list(&mut conn, &config).await?;
        let queue_table = make_jobs_queue_html(&jobs_list, columns)?;
        Ok(Self { root_uri, queue_table })
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
}

impl ContextWithSidebar for JobQueueContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

#[derive(Debug, Template)]
#[template(path = "submit-job.html")]
struct SubmitJobContext {
    root_uri: String
}

impl SubmitJobContext {
    fn new(root_uri: String) -> Self {
        Self { root_uri }
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
}

impl ContextWithSidebar for SubmitJobContext {
    fn sblink(&self, resource_uri: &str, text: &str, curr_page_id: &str, link_page_id: &str) -> Sblink {
        sblink_inner(&self.root_uri, resource_uri, text, curr_page_id, link_page_id)
    }
}

pub(crate) mod get {
    use askama_axum::IntoResponse;
    use axum::{extract::State, http::StatusCode};

    use crate::{server_error, AppStateRef};

    use super::{JobQueueContext, JobStatusContext, SubmitJobContext};

    pub(crate) async fn job_statuses(State(state): AppStateRef) -> Result<impl IntoResponse, StatusCode> {
        let context = JobStatusContext::new(state.root_uri.clone());
        Ok(context)
    }

    pub(crate) async fn job_queue(State(state): AppStateRef) -> Result<impl IntoResponse, StatusCode> {
        let res = JobQueueContext::new_from_db(
            state.root_uri.clone(),
            state,
            &["state", "short_site_locs", "start_date", "end_date", "email", "mod_fmt", "vmr_fmt", "map_fmt"]
        ).await;
        let context = server_error(res)?;
        Ok(context)
    }

    pub(crate) async fn submit_job(State(state): AppStateRef) -> Result<impl IntoResponse, StatusCode> {
        let context = SubmitJobContext::new(state.root_uri.clone());
        Ok(context)
    }
}