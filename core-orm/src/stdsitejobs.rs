use std::vec;

use chrono::NaiveDate;
use futures::TryStreamExt;
use serde::Serialize;
use sqlx::{self, FromRow, Type};

use crate::MySqlPC;


#[derive(Debug, Type, Clone, Copy, Serialize)]
pub enum StdSiteJobState {
    /// Indicates an unexpected value for state
    Unknown = -99, 
    /// Indicates this site/date combination is not present in the table
    Missing = -2,
    /// Indicates the site was not operational on this date and priors will never be generated for it
    Nonop = -1,
    /// Indicates that priors will need to be generated for this site
    Pending = 0,
    /// Indicates that priors have been generated for this site
    Complete = 1
}

impl Default for StdSiteJobState {
    fn default() -> Self {
        return Self::Unknown
    }
}

impl From<i8> for StdSiteJobState {
    fn from(val: i8) -> Self {
        match val {
            -1 => Self::Nonop,
            0 => Self::Pending,
            1 => Self::Complete,
            _ => Self::Unknown
        }
    }
}

#[derive(Debug)]
pub struct StdSiteJob {
    pub id: i32,
    pub site_id: String,
    pub date: NaiveDate,
    pub state: StdSiteJobState,
    pub job: Option<i32>
}

impl From<QStdSiteJob> for StdSiteJob {
    fn from(query_job: QStdSiteJob) -> Self {
        let site_id = query_job.site_id.unwrap_or("??".to_owned());
        return Self { 
            id: query_job.id, 
            site_id: site_id, 
            date: query_job.date,
            state: query_job.state.into(),
            job: query_job.job
        }
    }
}

impl StdSiteJob {
    pub async fn get_std_site_availability(conn: &mut MySqlPC, start_date: NaiveDate, end_date: Option<NaiveDate>, site_id: Option<&str>) -> anyhow::Result<Vec<StdSiteJob>>{
        let end_date = if let Some(e) = end_date {
            e
        }else{
            // Because the standard site jobs *cannot* be prepared for future dates, setting the end
            // date to a few days in the future is the same as not restricting the query on it.
            (chrono::Utc::today() + chrono::Duration::days(10)).naive_local()
        };

        let mut jobs = if let Some(sid) = site_id {
            sqlx::query_as!(
                QStdSiteJob,
                "SELECT * FROM v_StdSiteJobs WHERE date >= ? AND date <= ? AND site_id = ?",
                start_date,
                end_date,
                sid
            ).fetch(conn)
        }else{
            sqlx::query_as!(
                QStdSiteJob,
                "SELECT * FROM v_StdSiteJobs WHERE date >= ? AND date <= ?",
                start_date,
                end_date
            ).fetch(conn)
        };

        let mut avail_std_site_days = vec![];
        loop {
            let job = jobs.try_next().await?;
            if let Some(j) = job {
                avail_std_site_days.push(StdSiteJob::from(j))
            }else{
                break
            }
        }

        return Ok(avail_std_site_days)
    }
}

#[derive(Debug, FromRow)]
struct QStdSiteJob {
    id: i32,
    site: i32,
    site_id: Option<String>,
    date: NaiveDate,
    state: i8,
    job: Option<i32>
}

