use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Context;
use chrono::{NaiveDate, Duration};
use futures::TryStreamExt;
use itertools::Itertools;
use log::{info, warn};
use serde::Serialize;
use sqlx::{self, FromRow, Type, Connection};
use tabled::Tabled;

use crate::config::Config;
use crate::jobs;
use crate::met;
use crate::siteinfo::{self, SiteType, StdOutputStructure};
use crate::utils::DateIterator;
use crate::MySqlConn;


#[derive(Debug, Type, Clone, Copy, Serialize, PartialEq, Eq)]
#[repr(i8)]
pub enum StdSiteJobState {
    /// Indicates an unexpected value for state
    Unknown = -99, 
    /// Indicates this site/date combination is not present in the table, but the GEOS FP-IT data needed to generate it has been downloaded.
    MissingMet = -10,
    /// Indicates that output for this day is no longer needed because the site was actually not operational
    NonopNeeded = -3,
    /// Indicates that the priors for this day need regenerated (old files will be removed if needed)
    RegenNeeded = -2,
    /// Indicates that a job to generate priors is needed for this site
    JobNeeded = 0,
    /// Indicates that a job has been submitted for this site
    InProgress = 1,
    /// Indicates that priors have been generated for this site
    Complete = 2,
}

// TODO: update the database converter tool to properly map the old states to match the new ones (complete = 1 in old, 2 in new)

impl Default for StdSiteJobState {
    fn default() -> Self {
        return Self::Unknown
    }
}

impl From<i8> for StdSiteJobState {
    fn from(val: i8) -> Self {
        match val {
            -10 => Self::MissingMet,
            -3 => Self::NonopNeeded,
            -2 => Self::RegenNeeded,
            0 => Self::JobNeeded,
            1 => Self::InProgress,
            2 => Self::Complete,
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
    pub site_type: SiteType,
    pub job: Option<i32>,
    pub tarfile: Option<PathBuf>,
    pub output_structure: StdOutputStructure,
}

impl From<QStdSiteJob> for StdSiteJob {
    fn from(query_job: QStdSiteJob) -> Self {
        let site_id = query_job.site_id.unwrap_or_else(|| "??".to_owned());
        let site_type = query_job.site_type
            .map(|s| SiteType::try_from(s).unwrap())
            .unwrap_or(SiteType::Unknown);
        let output_structure = query_job.output_structure
            .map(|s| StdOutputStructure::from_str(&s).ok())
            .flatten()
            .unwrap_or_default();

        return Self { 
            id: query_job.id, 
            site_id: site_id, 
            site_type: site_type,
            date: query_job.date,
            state: query_job.state.into(),
            job: query_job.job,
            tarfile: query_job.tarfile.map(|s| PathBuf::from(s)),
            output_structure
        }
    }
}

// New workflow:
//   Missing days get added to the table with state JobNeeded
//   Days flagged for regeneration have any output deleted and their state changed to JobNeeded
//   Entries with state == JobNeeded are grouped by date and jobs submitted for them
//   Jobs from lines with state == InProgress are checked for when they are done, files are moved into std site tarballs, and the state changed to Complete

impl StdSiteJob {
    pub async fn update_std_site_job_table(conn: &mut MySqlConn, config: &Config, not_before: Option<NaiveDate>) -> anyhow::Result<()> {
        let first_date = if let Some(d) = not_before {
            d
        } else if let Some(d) = met::MetFile::get_first_complete_day_for_default_mets(conn, config).await
        .context("Error occurred while trying to identify the first complete day for default meteorologies")? {
            d
        } else {
            warn!("No available met data, nothing to be done to update the standard sites table");
            return Ok(());
        };

        let last_met_date = if let Some(d) = met::MetFile::get_last_complete_date_for_default_mets(conn, config).await
            .context("Error occurred while trying to identify the last complete day for default meteorologies")? {
                d
            } else {
                warn!("No available met data, nothing to be done to update the standard sites table");
                return Ok(());
            };

        Self::fill_missing_dates_for_all_sites(conn, config, first_date, last_met_date).await
            .map_err(|e| {
                let n = e.len();
                let msg = e.into_iter()
                    .map(|(sid, err)| format!("  - {sid}, {err}"))
                    .join("\n");
                anyhow::anyhow!("{n} sites had an error while filling in missing dates:\n{msg}")
            })
            .context("Error occurred while filling in missing days in the standard site jobs table")?;
        Self::reset_rows_for_regen(conn).await
            .context("Error occurred while resetting rows flagged for regeneration in the standard site jobs table")?;
        Self::try_reset_days_missing_met(conn, config).await
            .context("Error occurred while checking for days previous missing meteorology in the standard site job table")?;

        Ok(())
    }

    pub async fn add_std_site_job_row_from_args(conn: &mut MySqlConn, site_id: &str, date: NaiveDate, state: StdSiteJobState, job: Option<i32>) -> anyhow::Result<i32> {
        let site_prim_key = siteinfo::StdSite::site_id_to_primary_key(conn, site_id).await?;
        let res = sqlx::query!(
            "INSERT INTO StdSiteJobs (site, date, state, job) VALUES (?, ?, ?, ?)",
            site_prim_key,
            date,
            state,
            job
        ).execute(conn)
        .await?;

        let rid: i32 = res.last_insert_id().try_into()?;

        return Ok(rid)
    }

    pub async fn fill_missing_dates_for_site(conn: &mut MySqlConn, config: &Config, site_id: &str, first_date: NaiveDate, last_met_date: NaiveDate) -> anyhow::Result<()> {
        // First we get dates for which there is already an entry (in any state) for this site in the table
        let extant_site_dates: HashSet<_> = sqlx::query!(
            "SELECT date FROM v_StdSiteJobs WHERE site_id = ?",
            site_id,
        ).fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|r| r.date)
        .collect();

        // Now we get all the expected date for this site
        let locations = siteinfo::SiteInfo::get_site_locations(conn, site_id).await
            .with_context(|| format!("Failed to get site locations for site {site_id} while filling missing dates for standard site jobs"))?;

        if locations.is_empty() {
            anyhow::bail!("No locations defined for site {site_id}");
        }

        let nopen = locations.iter()
            .fold(0, |acc, loc| {
                if loc.end_date.is_none() {
                    acc + 1
                } else {
                    acc
                }
            });

        if nopen > 1 {
            anyhow::bail!("Multiple open-ended locations for site {site_id}, this is an invalid configuration for a site");
        }

        let date_ranges: Vec<_> = locations.iter()
            .map(|loc| (loc.start_date, loc.end_date.unwrap_or_else(|| last_met_date + Duration::days(1))))
            .collect();

        // Last, we insert an "JobNeeded" entry for this site & date for each date missing. 
        // We could use a transaction here, but it's not actually critical that we revert if only some 
        // adds fail.  We don't worry about missing met here, we'll do that when we submit jobs
        let date_iter = DateIterator::new_with_bounds(date_ranges, Some(first_date), Some(last_met_date + Duration::days(1)));

        let mut ndates = 0;
        for date in date_iter {
            if !extant_site_dates.contains(&date) {
                let rid = Self::add_std_site_job_row_from_args(conn, site_id, date, StdSiteJobState::JobNeeded, None).await?;
                ndates += 1;

                if !crate::met::MetFile::is_date_complete_for_default_mets(conn, config, date).await?.is_complete() {
                    warn!("Missing met data for date {}, setting standard site job table row for site {site_id} to 'MissingMet'", date);
                    Self::set_state_by_id(conn, StdSiteJobState::MissingMet, rid).await
                            .with_context(|| format!("Error occurred while setting row {rid} in the standard site jobs table to state 'MissingMet'"))?;
                }
            };

            
        }

        info!("{ndates} row in StdSiteJobs added for site {site_id}");
        Ok(())
    }

    /// Fill in date & site combinations missing from the standard site jobs table.
    /// 
    /// This takes all the sites defined in the standard sites table, and for each one checks that all the dates defined by
    /// its locations are present in the standard site jobs table. Any missing dates will have a row added in the state
    /// "JobNeeded" to later have a job submitted to fulfill it.
    /// 
    /// Note that this will try all sites, even if an error occurs while processing one. If any site errors, the returned `Err`
    /// will contain the individual error messages from each site. 
    pub async fn fill_missing_dates_for_all_sites(conn: &mut MySqlConn, config: &Config, first_date: NaiveDate, last_met_date: NaiveDate) -> Result<(), Vec<(String, anyhow::Error)>> {
        let site_ids = siteinfo::StdSite::get_site_ids(conn, None).await
            .map_err(|e| vec![("all".to_string(), e)])?;
        let mut errors = Vec::new();
        for site_id in site_ids {
            if let Err(e) = Self::fill_missing_dates_for_site(conn, config, &site_id, first_date, last_met_date).await {
                errors.push((site_id, e))
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Reset any rows flagged for regeneration
    /// 
    /// This will run [`clear_output_for_regen`] on each row in the standard site jobs table
    /// with state equal to "RegenNeeded". That will delete the output tarfile associated with
    /// that job (if it exists), set the state to "JobNeeded", and clear the job ID associated
    /// with this row. 
    pub async fn reset_rows_for_regen(conn: &mut MySqlConn) -> anyhow::Result<()> {
        let stdjobs: Vec<_> = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE state = ?",
            StdSiteJobState::RegenNeeded
        ).fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|q| StdSiteJob::try_from(q))
        .try_collect()?;

        let clearjobs: Vec<_> = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE state = ?",
            StdSiteJobState::NonopNeeded
        ).fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|q| StdSiteJob::try_from(q))
        .try_collect()?;

        let mut njobs = 0;
        for mut job in stdjobs {
            job.clear_output_for_regen(conn, true).await?;
            njobs += 1;
        }

        for mut job in clearjobs {
            job.clear_output_for_regen(conn, false).await?;
            njobs += 1;
        }

        info!("{njobs} rows in StdSiteJobs changed from 'RegenNeeded' to 'JobNeeded' (outputs cleared if they were present)");
        Ok(())
    }

    /// For any days in the standard site jobs table flagged as missing meteorology, check if the meteorology is available
    /// and set their state to "JobNeeded" if it is.
    /// 
    /// Note that this assumes that any day that was missing meteorology will not have an output tarball nor an associated job ID.
    /// If that is not the case, those tarballs will be left in place and the row will have the wrong job ID until a new job is
    /// submitted.
    pub async fn try_reset_days_missing_met(conn: &mut MySqlConn, config: &Config) -> anyhow::Result<()> {
        // First identify all the unique days flagged as missing meteorology
        let dates_missing_met = sqlx::query!(
            "SELECT DISTINCT(date) as udate FROM StdSiteJobs WHERE state = ?",
            StdSiteJobState::MissingMet
        ).fetch_all(&mut *conn)
        .await?
        .iter()
        .map(|r| r.udate)
        .collect_vec();

        for date in dates_missing_met {
            if let met::MetDayState::Complete = met::MetFile::is_date_complete_for_default_mets(conn, config, date).await? {
                // Assumes that if a date was missing met, it can't have an output file, so we only need to set the state
                let res = sqlx::query!(
                    "UPDATE StdSiteJobs SET state = ? WHERE date = ? AND state = ?",
                    StdSiteJobState::JobNeeded,
                    date,
                    StdSiteJobState::MissingMet
                ).execute(&mut *conn)
                .await?;
                info!("{} rows in StdSiteJobs for {date} changed from 'MissingMet' to 'JobNeeded'", res.rows_affected());
            }
        }
        Ok(())
    }

    /// Adds jobs needed to generate priors for standard site dates missing their priors
    /// 
    /// This will submit one job per date; this allows ginput to reuse certain expensive global calculations
    /// for every site that needs that day. It requests jobs to generate all the output files, the desired
    /// files will be selected while the standard site tarballs are created. If there is an error while
    /// adding the job for a date or assigning that job ID to the table rows in the standard sites job table,
    /// that job should be rescinded from the jobs table (as each date's database updates are handled in a
    /// transaction).
    pub async fn add_jobs_for_pending_rows(conn: &mut MySqlConn, config: &Config) -> anyhow::Result<()> {
        // This will give us a series of records, one per date, that lists the site IDs and std. site job
        // table row IDs that need jobs for each data. We want to submit one job per date because ginput
        // can then calculate the EqL interpolators once for all the sites, rather than repeating that
        // work for every site.
        let query = sqlx::query!(
            r#"SELECT date,GROUP_CONCAT(DISTINCT site_id SEPARATOR ",") AS site_ids,GROUP_CONCAT(DISTINCT id SEPARATOR ",") AS ids FROM v_StdSiteJobs WHERE state = ? GROUP BY date;"#,
            StdSiteJobState::JobNeeded
        ).fetch_all(&mut *conn)
        .await?;

        for rec in query {
            // Really the site_ids and ids values should never be None; the way the above query works, we should only get a record if there are rows with sites that 
            // need a job. However, we can't prove that the GROUP_CONCAT operation will produce something, so we double check that these aren't Nones. Also, we return
            // an error instead of panicking because we don't want to crash the service if this does happen, we want to log it so we can fix it.
            let sids = rec.site_ids.as_deref()
                .ok_or_else(|| anyhow::anyhow!("No site IDs collected needing jobs for date {} in standard site jobs table; this may mean a standard site is missing a location or otherwise defined incorrectly in the database", rec.date))?
                .split(",").collect_vec();

            let rids: Vec<i32> = rec.ids
                .ok_or_else(|| anyhow::anyhow!("No row IDs collected needing jobs for date {} in standard site jobs table; this may mean a standard site is missing a location or otherwise defined incorrectly in the database", rec.date))?
                .split(",")
                .map(|v| v.parse())
                .try_collect()
                .with_context(|| format!("Could not convert standard site job table row IDs back from string for date {} (this shouldn't happen)", rec.date))?;
                
            if !crate::met::MetFile::is_date_complete_for_default_mets(conn, config, rec.date).await?.is_complete() {
                warn!("Default meteorology not available for {}, setting these rows' states to MissingMet", rec.date);
                for rid in rids {
                    Self::set_state_by_id(conn, StdSiteJobState::MissingMet, rid).await
                        .with_context(|| format!("Error occurred while setting row {rid} in the standard site jobs table to state 'MissingMet'"))?;
                }

            } else {
                // For all the jobs, we'll just make all the possible output files, then put only the ones we want
                // into the tarballs later. This way we can still use one job per date, rather than splitting that
                // job up by what the different sites want.
                let mut trans = conn.begin().await?;

                let job_id = jobs::Job::add_job_from_args(
                    &mut trans, 
                    sids.iter().map(|s| s.to_string()).collect_vec(), 
                    rec.date, 
                    rec.date + Duration::days(1), 
                    config.execution.std_sites_output_base.clone(), 
                    None, 
                    vec![None; sids.len()], 
                    vec![None; sids.len()], 
                    &config.execution.std_site_job_queue, 
                    Some(jobs::ModFmt::Text), 
                    Some(jobs::VmrFmt::Text), 
                    Some(jobs::MapFmt::TextAndNetCDF), 
                    None, 
                    None, 
                    Some(jobs::TarChoice::No)
                ).await.with_context(|| format!("Error occurred while adding standard sites job for date {}", rec.date))?;
                
                for rid in rids {
                    Self::set_job_by_id(&mut trans, rid, job_id).await
                    .with_context(|| format!("Error occurred while trying to update standard site job table row {rid} with job ID {job_id}"))?;
                }
                
                trans.commit().await?;
                let s_sids = rec.site_ids.as_deref().unwrap_or("?");
                info!("Created job #{job_id} for standard sites {s_sids} on date {}", rec.date);

            }
        }

        Ok(())
    }

    pub async fn make_standard_site_tarballs(conn: &mut MySqlConn, config: &Config) -> anyhow::Result<()> {
        // Some things to keep in mind: (1) we should skip rows where the state != InProgress, because those
        // were probably updated while the jobs were running. (2) Different sites will have different output
        // structures, and we need to handle each one.
        let std_job_rows: Vec<StdSiteJob> = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE state = ? AND job IS NOT NULL;",
            StdSiteJobState::InProgress
        ).fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|q| StdSiteJob::try_from(q))
        .try_collect()?;

        let mut job_ids = HashSet::new();
        for mut row in std_job_rows {
            let jid = row.job.unwrap(); // SQL query ensures the job ID is not None
            let job = jobs::Job::get_job_with_id(conn, jid).await?;
            match job.state {
                jobs::JobState::Complete => (), // ready to make tarball
                jobs::JobState::Pending | jobs::JobState::Running => {
                    info!("Job {jid} still running, cannot make standard site tarball from it yet");
                    continue;
                },
                jobs::JobState::Errored => {
                    anyhow::bail!("Job {jid} required for standard sites had an error");
                },
                jobs::JobState::Cleaned => {
                    anyhow::bail!("Job {jid} required for standard sites was previous cleaned up");
                },
            }
            
            // Always want to clean up finished jobs, as long as there isn't an error
            job_ids.insert(jid); 

            if row.state != StdSiteJobState::InProgress {
                warn!("Not making tarball for standard site job table row {}, state is not 'InProgress' suggesting a flag for regeneration or other change while the job was processing", row.id);
                continue;
            }

            info!("Making standard site tarball ({} format) for {} on {}", row.output_structure, row.site_id, row.date);
            let output_tarballs = row.output_structure.make_std_site_tarball(&config.execution.std_sites_tar_output, &row.site_id, &job, config)?;
            if output_tarballs.len() > 1 {
                warn!("Multiple tarballs were created for standard site job {}, only the last one will be listed in the job's output file field", job.job_id);
            }

            let mut output_tarballs = output_tarballs.into_iter();
            if let Some(output_tarball) = output_tarballs.next() {
                row.set_complete(conn, output_tarball).await?;
            } else {
                anyhow::bail!("No output tarball path was returned while tarring job {}", job.job_id)
            }
        }

        for jid in job_ids {
            let mut job = jobs::Job::get_job_with_id(conn, jid).await?;
            job.set_cleaned(conn).await?;
            info!("Standard site job {jid} cleaned up");
        }

        Ok(())
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

    /// Flag rows in the standard site jobs table for a given site and a date range for priors regeneration.
    /// 
    /// If `end_date` is `None`, then all rows from `start_date` on are flagged. Otherwise, rows up to but not
    /// including `end_date` are flagged. Returns the number of rows affected if successful. Returns an `Err`
    /// if the database query fails.
    pub async fn set_regen_flag(conn: &mut MySqlConn, site_id: &str, start_date: NaiveDate, end_date: Option<NaiveDate>, clear_output: bool) -> anyhow::Result<u64> {
        let new_state = if clear_output {
            StdSiteJobState::NonopNeeded
        } else {
            StdSiteJobState::RegenNeeded
        };

        let q = if let Some(end) = end_date {
            sqlx::query!(
                "UPDATE v_StdSiteJobs SET state = ? WHERE site_id = ? AND date >= ? AND date < ?",
                new_state,
                site_id,
                start_date,
                end
            ).execute(conn)
            .await?
        } else {
            sqlx::query!(
                "UPDATE v_StdSiteJobs SET state = ? WHERE site_id = ? AND date >= ?",
                new_state,
                site_id,
                start_date
            ).execute(conn)
            .await?
        };

        Ok(q.rows_affected())
    }

    pub async fn set_job_by_id(conn: &mut MySqlConn, row_id: i32, job_id: i32) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteJobs SET state = ?, job = ? WHERE id = ?",
            StdSiteJobState::InProgress,
            job_id,
            row_id
        ).execute(conn)
        .await?;

        Ok(())
    }

    pub async fn set_state_by_id(conn: &mut MySqlConn, state: StdSiteJobState, row_id: i32) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteJobs SET state = ? WHERE id = ?",
            state,
            row_id
        ).execute(conn)
        .await?;

        Ok(())
    }

    /// Update the state of this instance and the corresponding row in the database.
    pub async fn set_state(&mut self, state: StdSiteJobState, conn: &mut MySqlConn) -> anyhow::Result<()> {
        Self::set_state_by_id(conn, state, self.id).await?;
        self.state = state;
        Ok(())
    }

    pub async fn set_complete(&mut self, conn: &mut MySqlConn, tarball: PathBuf) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteJobs SET state = ?, tarfile = ? WHERE id = ?",
            StdSiteJobState::Complete,
            tarball.to_str().ok_or_else(|| anyhow::anyhow!("Could not convert tarball path to valid unicode"))?,
            self.id
        ).execute(conn)
        .await?;

        self.state = StdSiteJobState::Complete;
        self.tarfile = Some(tarball);
        Ok(())
    }

    /// For this row in the standard site table, reset its state to "JobNeeded" and delete the output file, if present
    /// 
    /// If an error occurs while deleting the output file, the database and this instance will not be updated. If the
    /// reset is successful, both the database and this instance will have the state set to "JobNeeded" and the output
    /// `tarfile` set to `None`.
    async fn clear_output_for_regen(&mut self, conn: &mut MySqlConn, needs_new_job: bool) -> anyhow::Result<()> {
        let mut trans = conn.begin().await?;

        if needs_new_job {
            sqlx::query!(
                "UPDATE StdSiteJobs SET state = ?, job = ?, tarfile = ? WHERE id = ?",
                StdSiteJobState::JobNeeded,
                None::<i32>,
                None::<String>,
                self.id
            ).execute(&mut *trans)
            .await
            .with_context(|| format!("Error occurred trying to set state for row {} in StdSiteJobs table to {:?}", self.id, StdSiteJobState::JobNeeded))?;
        } else {
            sqlx::query!(
                "DELETE FROM StdSiteJobs WHERE id = ?",
                self.id
            ).execute(&mut *trans)
            .await
            .with_context(|| format!("Error occurred trying to remove row {} from StdSiteJobs table", self.id))?;
        }

        if let Some(output) = &self.tarfile {
            std::fs::remove_file(output)
                .with_context(|| format!("Failed removing output file ({}) for standard site job entry {}", output.display(), self.id))?;
        }

        // Using the transaction should ensure that if we can't remove the output file, the database doesn't think the file is missing when it
        // actually isn't
        trans.commit().await?;
        
        self.state = StdSiteJobState::JobNeeded;
        self.tarfile = None;

        Ok(())
    }

    /// Provide a summary the sites generated/pending/etc. for a given date
    pub async fn summarize_date(conn: &mut MySqlConn, date: NaiveDate) -> anyhow::Result<StdJobDateSummary> {
        let stdsites = siteinfo::SiteInfo::get_site_info_for_date(conn, date, false).await?;
        let all_site_ids = stdsites.iter()
            .map(|s| s.site_id.as_deref().unwrap_or("??"))
            .collect_vec();

        let rows = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE date = ?",
            date
        ).fetch_all(conn)
        .await
        .context("Error occurred while trying to query the standard site jobs for date {date}")?
        .into_iter()
        .map(|q| StdSiteJob::from(q))
        .collect_vec();

        // First categorize all the sites that show up in the job rows
        let mut summary = StdJobDateSummary::new(date);
        let mut accounted_sites = HashSet::new();
        for row in rows.iter() {
            match row.state {
                StdSiteJobState::Unknown | StdSiteJobState::MissingMet => summary.other_sites.insert(row.site_id.clone()),
                StdSiteJobState::RegenNeeded | StdSiteJobState::NonopNeeded => summary.regen_sites.insert(row.site_id.clone()),
                StdSiteJobState::JobNeeded => summary.pending_sites.insert(row.site_id.clone()),
                StdSiteJobState::InProgress => summary.in_prog_sites.insert(row.site_id.clone()),
                StdSiteJobState::Complete => summary.complete_sites.insert(row.site_id.clone()),
            };

            accounted_sites.insert(row.site_id.as_str());
        }

        // Then catch any sites that didn't have a row in the jobs table - these are non-operational
        for sid in all_site_ids {
            if !accounted_sites.contains(sid) {
                summary.nonop_sites.insert(sid.to_string());
            }
        }

        Ok(summary)
    }
}

pub struct AddStdJobSummary {
    pub job_id: i32,
    pub sites_included: Vec<String>
}

#[derive(Debug, FromRow)]
struct QStdSiteJob {
    id: i32,
    #[allow(dead_code)]
    site: i32,
    site_id: Option<String>,
    site_type: Option<String>,
    date: NaiveDate,
    state: i8,
    job: Option<i32>,
    tarfile: Option<String>,
    output_structure: Option<String>,
}

pub struct StdJobDateSummary {
    date: NaiveDate,
    complete_sites: HashSet<String>,
    pending_sites: HashSet<String>,
    in_prog_sites: HashSet<String>,
    regen_sites: HashSet<String>,
    nonop_sites: HashSet<String>,
    other_sites: HashSet<String>,
}

impl StdJobDateSummary {
    fn new(date: NaiveDate) -> Self {
        Self { 
            date,
            complete_sites: HashSet::new(),
            pending_sites: HashSet::new(),
            in_prog_sites: HashSet::new(),
            regen_sites: HashSet::new(),
            nonop_sites: HashSet::new(),
            other_sites: HashSet::new(),
        }
    }

    fn field_to_string(field: &HashSet<String>) -> String {
        field.iter()
            .sorted()
            .join(",")
    }
}

impl Tabled for StdJobDateSummary {
    const LENGTH: usize = 8;

    fn fields(&self) -> Vec<std::borrow::Cow<'_, str>> {
        vec![
            self.date.to_string().into(),
            Self::field_to_string(&self.complete_sites).into(),
            Self::field_to_string(&self.pending_sites).into(),
            Self::field_to_string(&self.in_prog_sites).into(),
            Self::field_to_string(&self.regen_sites).into(),
            Self::field_to_string(&self.nonop_sites).into(),
            Self::field_to_string(&self.other_sites).into(),
        ]
    }

    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            "Date".into(),
            "Complete".into(),
            "Pending".into(),
            "In progress".into(),
            "Regen/clear".into(),
            "Nonop/unlisted".into(),
            "Other".into(),
        ]
    }
}