use orm::jobs::Job;

pub(crate) fn download_url_for_job(job: &Job, root_uri: &str) -> Option<String> {
    if job.output_file.is_none() {
        None
    } else {
        let root_uri = root_uri.trim_end_matches('/');
        Some(format!("{root_uri}/{}", job.job_id))
    }
}
