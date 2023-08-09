use std::collections::HashSet;
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Duration};
use futures::TryStreamExt;
use log::{warn, info};
use serde::Serialize;
use sqlx::{self, FromRow, Type, Acquire};

use crate::{MySqlConn, config};
use crate::{utils,met,jobs,siteinfo};


#[derive(Debug, Type, Clone, Copy, Serialize)]
#[repr(i8)]
pub enum StdSiteJobState {
    /// Indicates an unexpected value for state
    Unknown = -99, 
    /// Indicates this site/date combination is not present in the table and the GEOS FP-IT data is not available
    MissingGeosUnavailable = -3,
    /// Indicates this site/date combination is not present in the table, but the GEOS FP-IT data needed to generate it has been downloaded.
    MissingGeosPresent = -2,
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
    pub job: Option<i32>,
    pub tarfile: Option<PathBuf>
}

impl From<QStdSiteJob> for StdSiteJob {
    fn from(query_job: QStdSiteJob) -> Self {
        let site_id = query_job.site_id.unwrap_or("??".to_owned());
        return Self { 
            id: query_job.id, 
            site_id: site_id, 
            date: query_job.date,
            state: query_job.state.into(),
            job: query_job.job,
            tarfile: query_job.tarfile.map(|s| PathBuf::from(s))
        }
    }
}

impl StdSiteJob {
    pub async fn add_std_site_job_row_from_args(conn: &mut MySqlConn, site_id: &str, date: NaiveDate, state: StdSiteJobState, job: Option<i32>) -> anyhow::Result<()> {
        let site_prim_key = siteinfo::StdSite::site_id_to_primary_key(conn, site_id).await?;
        sqlx::query!(
            "INSERT INTO StdSiteJobs (site, date, state, job) VALUES (?, ?, ?, ?)",
            site_prim_key,
            date,
            state,
            job
        ).execute(conn)
        .await?;

        return Ok(())
    }

    /// Return information about whether jobs for standard sites have been queued, completed, etc.
    /// 
    /// # Parameters
    /// * `conn` - a connection to the MySQL database containing the standard site jobs
    /// * `start_date` - the earliest date to get information about
    /// * `end_date` - the last date to get information about (inclusive). If `None`, then
    ///   all information for days on or after `start_date` are included.
    /// * `site_id` - if not `None`, then the two-letter site ID of a specific site to get information
    ///   about.
    /// 
    /// # Returns
    /// * A `Vec` of `StdSiteJob` structures. These will include all rows from the `StdSiteJobs` table
    ///   with a date after `start_date` and before `end_date` if the latter is given. If `site_id`
    ///   is not `None`, then only rows for that site are included. Missing dates are not filled in,
    ///   you have to assume that any site/date combinations missing from the vector have not been
    ///   added to the `StdSiteJobs` table yet.
    /// 
    /// # Errors
    /// Returns an `Err` if the database query operation fails at any point. 
    pub async fn get_std_site_availability(conn: &mut MySqlConn, start_date: NaiveDate, end_date: Option<NaiveDate>, site_id: Option<&str>) -> anyhow::Result<Vec<StdSiteJob>>{
        let end_date = if let Some(e) = end_date {
            e
        }else{
            // Because the standard site jobs *cannot* be prepared for future dates, setting the end
            // date to a few days in the future is the same as not restricting the query on it.
            chrono::Utc::now().naive_local().date() + chrono::Duration::days(10)
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

    /// Add jobs for standard sites for a single date
    /// 
    /// # Parameters
    /// * `date` - the date to add jobs for.
    /// 
    /// # Returns
    /// If this date is not present in the `StdSiteJobs` table, the return will be a 
    /// `Some<AddStdJobSummary>`, and the structure will include which sites are included
    /// in the new job and the ID of the job. If the date was already present, then this
    /// returns `None`.
    /// 
    /// # Errors
    /// Returns an `Err` if any of the database queries fail. Should any of the queries to create
    /// the job or the new standard site job rows fail, all of the insert queries should be rolled back.
    pub async fn add_new_std_jobs_for_date(conn: &mut MySqlConn, date: NaiveDate, save_dir: &Path) -> anyhow::Result<Option<AddStdJobSummary>> {
        // First check if this date already has any sites - if so, return None (this function is not intended for backfilling)
        let date_count = sqlx::query!("SELECT COUNT(*) as count FROM StdSiteJobs WHERE date = ?", date)
            .fetch_one(&mut *conn)
            .await?
            .count;

        if date_count > 0 { return Ok(None) }

        // Now figure out which sites need priors generated and which are not operational
        let active_site_info = siteinfo::SiteInfo::get_site_info_for_date(&mut *conn, date, true)
            .await?;

        let mut active_sites = HashSet::new();
        for info in active_site_info {
            if let Some(sid) = info.site_id {
                active_sites.insert(sid);
            }else{
                warn!("An active site (id = {}) does not have a two-letter ID defined", info.id);
            }
        }

        let all_sites = siteinfo::StdSite::get_site_ids(&mut *conn, None)
            .await?;
        let mut job_sites = vec![];
        let mut nonop_sites = vec![];
        for site in all_sites {
            if active_sites.contains(&site) {
                job_sites.push(site);
            }else{
                nonop_sites.push(site);
            }
        }

        // Create a new job for the active sites, then add elements to the StdSiteJob table for both
        // the operation and non-operational sites.
        let latlon: Vec<Option<f32>> = (0..job_sites.len()).map(|_| None).collect();

        let mut transaction = conn.begin().await?;
        let trans_conn = transaction.acquire().await?;

        let new_job_id = jobs::Job::add_job_from_args(
            &mut *trans_conn, 
            job_sites.clone(),
            date,
            date + Duration::days(1),
            save_dir.to_owned(),
            None,
            latlon.clone(),
            latlon,
            Some(jobs::ModFmt::Text),
            Some(jobs::VmrFmt::Text),
            Some(jobs::MapFmt::None), // TODO: will need to be text for EM27s potentially
            Some(10),
            None,
            Some(jobs::TarChoice::No)
        ).await?;

        for site in job_sites.iter() {
            Self::add_std_site_job_row_from_args(
                &mut *trans_conn,
                site,
                date,
                StdSiteJobState::Pending,
                Some(new_job_id)
            ).await?;
        }

        for site in nonop_sites.iter() {
            Self::add_std_site_job_row_from_args(
                &mut *trans_conn,
                site,
                date,
                StdSiteJobState::Nonop,
                Some(new_job_id)
            ).await?;
        }
        transaction.commit().await?;
        
        info!("Added job {new_job_id} for standard sites ({} active, {} nonoperational)", job_sites.len(), nonop_sites.len());
        Ok(Some(AddStdJobSummary{
            job_id: new_job_id,
            sites_included: job_sites
        }))
    }

    /// Add a batch of new standard site jobs from the last existing date up to some future date
    /// 
    /// # Parameters
    /// * `pool` - a pool of MySQL connections
    /// * `date` - the last date to add standard site jobs for. If not given, then this is inferred
    ///   from the available GEOS eta and surface files.
    /// * `save_dir` - directory to create the output directories in.
    /// 
    /// # Returns
    /// If successful, returns a vector of `AddStdJobSummary` instances, one per date added.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// * any of the database operations fail
    /// * there are no existing standard site jobs, so cannot determine the starting date
    /// * `date` is `None` and it cannot determine the last date with the full required suite of GEOS files
    pub async fn add_new_std_jobs_up_to_date(conn: &mut MySqlConn, cfg: &config::Config, date: Option<NaiveDate>, save_dir: &Path) -> anyhow::Result<Vec<AddStdJobSummary>> {
        let last_std_site_date = sqlx::query!(
            "SELECT MAX(date) as date FROM StdSiteJobs"
        ).fetch_one(&mut *conn)
        .await?
        .date
        .ok_or(anyhow::Error::msg("Found no existing standard site jobs, cannot use the function add_new_std_jobs_up_to_date"))?;

        let last_date = if let Some(d) = date {
            d
        }else{
            // let default_opts = defaultopts::DefaultOptions::get_defaults_for_date(conn, date).await?;

            met::MetFile::get_last_complete_date_for_default_mets(
                &mut *conn, 
                cfg
            ).await?
            .ok_or(anyhow::Error::msg("Could not determine most recent date with all required GEOS files, cannot use the function add_new_std_jobs_up_to_date"))?
        };

        let mut added = vec![];
        for date in utils::date_range(last_std_site_date + Duration::days(1), last_date + Duration::days(1)) {
            if let Some(res) = Self::add_new_std_jobs_for_date(conn, date, save_dir).await? {
                added.push(res);
            }
        }

        return Ok(added)
    }
}

pub struct AddStdJobSummary {
    pub job_id: i32,
    pub sites_included: Vec<String>
}

#[derive(Debug, FromRow)]
struct QStdSiteJob {
    id: i32,
    site: i32,
    site_id: Option<String>,
    date: NaiveDate,
    state: i8,
    job: Option<i32>,
    tarfile: Option<String>
}

