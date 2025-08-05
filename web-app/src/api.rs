use serde::{Deserialize, Serialize};

pub(crate) mod check;
pub(crate) mod download;
pub(crate) mod jobs;
pub(crate) mod middleware;
pub(crate) mod query;

/// Inner structure to serialize the locations requested for a job
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ApiJobSite {
    site_id: String,
    lat: Option<f32>,
    lon: Option<f32>,
}
