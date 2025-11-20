use std::collections::HashSet;
use std::fmt::Display;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Context;
use chrono::Utc;
use chrono::{Duration, NaiveDate};
use itertools::Itertools;
use log::debug;
use log::{info, warn};
use serde::Deserialize;
use serde::Serialize;
use sqlx::{self, Connection, FromRow, Type};
use tabled::Tabled;

use crate::config::Config;
use crate::config::ProcCfgKey;
use crate::jobs;
use crate::jobs::JobProcKey;
use crate::met;
use crate::siteinfo::SiteInfo;
use crate::siteinfo::{self, SiteType, StdOutputStructure};
use crate::utils::DateIterator;
use crate::MySqlConn;

#[derive(Debug, Type, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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

impl Default for StdSiteJobState {
    fn default() -> Self {
        return Self::Unknown;
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
            _ => Self::Unknown,
        }
    }
}

impl Display for StdSiteJobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StdSiteJobState::Unknown => write!(f, "unknown"),
            StdSiteJobState::MissingMet => write!(f, "missing met"),
            StdSiteJobState::NonopNeeded => write!(f, "nonop needed"),
            StdSiteJobState::RegenNeeded => write!(f, "regen needed"),
            StdSiteJobState::JobNeeded => write!(f, "job needed"),
            StdSiteJobState::InProgress => write!(f, "in progress"),
            StdSiteJobState::Complete => write!(f, "complete"),
        }
    }
}

#[derive(Debug)]
pub struct StdSiteJob {
    /// The ID for this row in the table
    pub id: i32,
    /// The TCCON-style two-character site ID for this job
    pub site_id: String,
    /// The date for which this job produces priors
    pub date: NaiveDate,
    /// Which processing configuration to use for this standard site job.
    /// If `None`, then use the default that a user request would for this
    /// date.
    pub processing_key: ProcCfgKey,
    /// The state of this job, distinct from the regular Job state
    pub state: StdSiteJobState,
    /// Whether this job is for a TCCON or EM27 site
    pub site_type: SiteType,
    /// The ID in the Jobs table of the job that will/has produced the
    /// output for this site/date. Will be `None` if no job has been submitted
    /// yet or if the tarball for this site/date was imported from an existing
    /// source.
    pub job: Option<i32>,
    /// Path to the tarfile for this site/date. Will be `None` if it has not yet
    /// been generated.
    pub tarfile: Option<PathBuf>,
    /// How the .mod and .vmr files should be organized within the tarball.
    pub output_structure: StdOutputStructure,
}

impl From<QStdSiteJob> for StdSiteJob {
    fn from(query_job: QStdSiteJob) -> Self {
        let site_id = query_job.site_id.unwrap_or_else(|| "??".to_owned());
        let site_type = query_job
            .site_type
            .map(|s| SiteType::try_from(s).unwrap())
            .unwrap_or(SiteType::Unknown);
        let output_structure = query_job
            .output_structure
            .map(|s| StdOutputStructure::from_str(&s).ok())
            .flatten()
            .unwrap_or_default();

        return Self {
            id: query_job.id,
            site_id: site_id,
            site_type: site_type,
            date: query_job.date,
            processing_key: ProcCfgKey(query_job.processing_key),
            state: query_job.state.into(),
            job: query_job.job,
            tarfile: query_job.tarfile.map(|s| PathBuf::from(s)),
            output_structure,
        };
    }
}

// New workflow:
//   Missing days get added to the table with state JobNeeded
//   Days flagged for regeneration have any output deleted and their state changed to JobNeeded
//   Entries with state == JobNeeded are grouped by date and jobs submitted for them
//   Jobs from lines with state == InProgress are checked for when they are done, files are moved into std site tarballs, and the state changed to Complete

/// Fill in any missing rows in the site job table. Missing rows are those representing a
/// combination of site, date, and processing configuration expected but not found.
/// If `not_before` is given, then only rows for that date and on will be checked if
/// missing.
///
/// # See also:
/// - [`Self::fill_missing_dates_for_site_all_proc_configs`] if you need to fill in rows for only one site.
/// - [`Self::fill_missing_dates_for_site`] if you need to fill in rows for only one site for a single
///   processing configuration.
impl StdSiteJob {
    pub async fn update_std_site_job_table(
        conn: &mut MySqlConn,
        config: &Config,
        not_before: Option<NaiveDate>,
    ) -> anyhow::Result<()> {
        for proc_key in config.get_auto_proc_cfgs() {
            let (first_date, last_met_date) = if let Some(dates) =
                Self::get_date_range_for_proc_cfg(conn, config, proc_key).await?
            {
                let start = not_before.unwrap_or(dates.0).max(dates.0);
                (start, dates.1)
            } else {
                warn!("No days have all the met required for processing configuration '{proc_key}', will not update standard sites jobs table");
                continue;
            };

            Self::fill_missing_dates_for_all_sites(
                conn,
                config,
                first_date,
                last_met_date,
                proc_key,
            )
            .await
            .map_err(|e| {
                let n = e.len();
                let msg = e
                    .into_iter()
                    .map(|(sid, err)| format!("  - {sid}, {err}"))
                    .join("\n");
                anyhow::anyhow!("{n} sites had an error while filling in missing dates:\n{msg}")
            })
            .context(
                "Error occurred while filling in missing days in the standard site jobs table",
            )?;
            Self::reset_rows_for_regen(conn).await
            .context("Error occurred while resetting rows flagged for regeneration in the standard site jobs table")?;
            Self::try_reset_days_missing_met(conn, config).await
            .context("Error occurred while checking for days previous missing meteorology in the standard site job table")?;
        }
        Ok(())
    }

    /// Helper functino that returns the range of dates that a given processing
    /// configuration can have priors generated for. The returned value will be
    /// `Some(_)` as long as there is at least one day of complete met data
    /// required, otherwise it will be `None`.
    async fn get_date_range_for_proc_cfg(
        conn: &mut MySqlConn,
        config: &Config,
        proc_key: &ProcCfgKey,
    ) -> anyhow::Result<Option<(NaiveDate, NaiveDate)>> {
        let proc_cfg = config
            .processing_configuration
            .get(proc_key)
            .ok_or_else(|| anyhow!("Processing configuration '{proc_key}' not found"))?;

        let met_cfgs = proc_cfg.get_met_configs(config)?;
        let first_avail_date =
            met::MetFile::get_first_or_last_complete_date_for_config_set(conn, &met_cfgs, true)
                .await
                .context("Error occurred while trying to identify the first complete day for default meteorologies")?;
        let last_avail_date =
            met::MetFile::get_first_or_last_complete_date_for_config_set(conn, &met_cfgs, false)
                .await
                .context("Error occurred while trying to identify the last complete day for default meteorologies")?;

        if let (Some(start), Some(end)) = (first_avail_date, last_avail_date) {
            Ok(Some((start, end)))
        } else {
            Ok(None)
        }
    }

    /// Add a new row to the standard site jobs table.
    /// Note that this does not submit the ginput job; if you
    /// have that job ID, it must be passed as the `job` argument.
    pub async fn add_std_site_job_row_from_args(
        conn: &mut MySqlConn,
        site_id: &str,
        date: NaiveDate,
        processing_key: &ProcCfgKey,
        state: StdSiteJobState,
        job: Option<i32>,
    ) -> anyhow::Result<i32> {
        let site_prim_key = siteinfo::StdSite::site_id_to_primary_key(conn, site_id).await?;
        let res = sqlx::query!(
            "INSERT INTO StdSiteJobs (site, date, processing_key, state, job) VALUES (?, ?, ?, ?, ?)",
            site_prim_key,
            date,
            processing_key,
            state,
            job
        )
        .execute(conn)
        .await?;

        let rid: i32 = res.last_insert_id().try_into()?;

        return Ok(rid);
    }

    /// Update the table of standard site jobs by adding any missing
    /// rows for a single site for all the automatic processing configurations.
    /// `first_date` and `last_date` can be used to limit the date range
    /// that rows are added. If either is `None`, it will be taken from
    /// the dates for which met data is available.
    pub async fn fill_missing_dates_for_site_all_proc_configs(
        conn: &mut MySqlConn,
        config: &Config,
        site_id: &str,
        first_date: Option<NaiveDate>,
        last_date: Option<NaiveDate>,
    ) -> anyhow::Result<()> {
        for proc_key in config.get_auto_proc_cfgs() {
            let opt_dates = Self::get_date_range_for_proc_cfg(conn, config, proc_key).await?;
            let (first_met_date, last_met_date) = if let Some(dates) = opt_dates {
                dates
            } else {
                warn!(
                    "No available met data for processing key '{proc_key}', nothing to be done to update the standard sites table"
                );
                continue;
            };

            // Ensure that, if the input requested a specific date range, we limit to the range of actually available
            // met data.
            let first_add_date = first_date.unwrap_or(first_met_date).max(first_met_date);
            let last_add_date = last_date.unwrap_or(last_met_date).min(last_met_date);

            StdSiteJob::fill_missing_dates_for_site(
                conn,
                config,
                site_id,
                first_add_date,
                last_add_date,
                proc_key,
            )
            .await?;
        }

        Ok(())
    }

    /// Fill in missing rows for a given site for a given processing configuration.
    /// Note that if you are looking for the equivalent of [`Self::update_std_site_job_table`]
    /// for a single site, use [`Self::fill_missing_dates_for_site_all_proc_configs`].
    async fn fill_missing_dates_for_site(
        conn: &mut MySqlConn,
        config: &Config,
        site_id: &str,
        first_date: NaiveDate,
        last_met_date: NaiveDate,
        processing_key: &ProcCfgKey,
    ) -> anyhow::Result<()> {
        // First we get dates for which there is already an entry (in any state) for this site in the table
        let extant_site_dates_and_procs =
            Self::get_extant_dates_and_proc(conn, site_id, processing_key).await?;

        // Now we get all the expected date for this site
        let locations = siteinfo::SiteInfo::get_site_locations(conn, site_id).await
            .with_context(|| format!("Failed to get site locations for site {site_id} while filling missing dates for standard site jobs"))?;

        if locations.is_empty() {
            log::warn!("No locations defined for site {site_id}");
        }

        let nopen = locations.iter().fold(
            0,
            |acc, loc| {
                if loc.end_date.is_none() {
                    acc + 1
                } else {
                    acc
                }
            },
        );

        if nopen > 1 {
            anyhow::bail!("Multiple open-ended locations for site {site_id}, this is an invalid configuration for a site");
        }

        let date_ranges: Vec<_> = locations
            .iter()
            .map(|loc| {
                (
                    loc.start_date,
                    loc.end_date
                        .unwrap_or_else(|| last_met_date + Duration::days(1)),
                )
            })
            .collect();

        // Last, we insert an "JobNeeded" entry for this site & date for each date missing.
        // We could use a transaction here, but it's not actually critical that we revert if only some
        // adds fail.  We don't worry about missing met here, we'll do that when we submit jobs
        let date_iter = DateIterator::new_with_bounds(
            date_ranges,
            Some(first_date),
            Some(last_met_date + Duration::days(1)),
        );

        let mut ndates = 0;
        for date in date_iter {
            if !extant_site_dates_and_procs.contains(&date) {
                let rid = Self::add_std_site_job_row_from_args(
                    conn,
                    site_id,
                    date,
                    processing_key,
                    StdSiteJobState::JobNeeded,
                    None,
                )
                .await?;
                ndates += 1;
                if ndates % 100 == 0 {
                    // TODO: add a length hint method to DateIterator and use that to update this message with how many
                    // there are to go.
                    log::info!("Adding 'JobNeeded' entries ({ndates} complete so far)")
                }

                if !crate::met::MetFile::is_date_complete_for_default_processing(conn, config, date)
                    .await?
                    .is_complete()
                {
                    warn!("Missing met data for date {}, setting standard site job table row for site {site_id} to 'MissingMet'", date);
                    Self::set_state_by_id(conn, StdSiteJobState::MissingMet, rid).await
                            .with_context(|| format!("Error occurred while setting row {rid} in the standard site jobs table to state 'MissingMet'"))?;
                }
            }
        }

        info!("{ndates} row in StdSiteJobs added for site {site_id}");
        Ok(())
    }

    /// Helper function that returns a set of dates that are already
    /// in the standard site jobs table for a specific site and processing
    /// configuration.
    async fn get_extant_dates_and_proc(
        conn: &mut MySqlConn,
        site_id: &str,
        proc_key: &ProcCfgKey,
    ) -> anyhow::Result<HashSet<NaiveDate>> {
        let row_iter = sqlx::query!(
            "SELECT date FROM v_StdSiteJobs WHERE site_id = ? AND processing_key = ?",
            site_id,
            proc_key
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|row| row.date);

        Ok(HashSet::from_iter(row_iter))
    }

    /// Fill in date & site combinations missing from the standard site jobs table.
    ///
    /// This takes all the sites defined in the standard sites table, and for each one checks that all the dates defined by
    /// its locations are present in the standard site jobs table. Any missing dates will have a row added in the state
    /// "JobNeeded" to later have a job submitted to fulfill it.
    ///
    /// Note that this will try all sites, even if an error occurs while processing one. If any site errors, the returned `Err`
    /// will contain the individual error messages from each site.
    ///
    /// Currently, this is an inner function for the main table update driver only.
    async fn fill_missing_dates_for_all_sites(
        conn: &mut MySqlConn,
        config: &Config,
        first_date: NaiveDate,
        last_met_date: NaiveDate,
        processing_key: &ProcCfgKey,
    ) -> Result<(), Vec<(String, anyhow::Error)>> {
        let site_ids = siteinfo::StdSite::get_site_ids(conn, None)
            .await
            .map_err(|e| vec![("all".to_string(), e)])?;
        let mut errors = Vec::new();
        for site_id in site_ids {
            if let Err(e) = Self::fill_missing_dates_for_site(
                conn,
                config,
                &site_id,
                first_date,
                last_met_date,
                processing_key,
            )
            .await
            {
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
    /// This will run [`Self::clear_output_for_regen`] on each row in the standard site jobs table
    /// with state equal to "RegenNeeded". That will delete the output tarfile associated with
    /// that job (if it exists), set the state to "JobNeeded", and clear the job ID associated
    /// with this row.
    pub async fn reset_rows_for_regen(conn: &mut MySqlConn) -> anyhow::Result<()> {
        let stdjobs: Vec<_> = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE state = ?",
            StdSiteJobState::RegenNeeded
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|q| StdSiteJob::try_from(q))
        .try_collect()?;

        let clearjobs: Vec<_> = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE state = ?",
            StdSiteJobState::NonopNeeded
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|q| StdSiteJob::try_from(q))
        .try_collect()?;

        let mut njobs = 0;
        for mut job in stdjobs {
            log::debug!(
                "Clearing outputs for site job {} and marking it as ready for a new job",
                job.id
            );
            job.clear_output_for_regen(conn, true).await?;
            njobs += 1;
        }

        for mut job in clearjobs {
            log::debug!(
                "Clearing outputs for site job {} but marking it as NONOPERATIONAL",
                job.id
            );
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
    pub async fn try_reset_days_missing_met(
        conn: &mut MySqlConn,
        config: &Config,
    ) -> anyhow::Result<()> {
        // First identify all the unique date/processing combinations flagged as missing meteorology
        let dates_missing_met = sqlx::query!(
            "SELECT DISTINCT date as udate, processing_key as upk FROM StdSiteJobs WHERE state = ?",
            StdSiteJobState::MissingMet
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|r| (r.udate, ProcCfgKey(r.upk)))
        .collect_vec();

        for (date, proc_key) in dates_missing_met {
            let day_state =
                met::MetFile::is_date_complete_for_processing_config(conn, config, date, &proc_key)
                    .await?;
            if day_state.is_complete() {
                // Assumes that if a date was missing met, it can't have an output file, so we only need to set the state
                let res = sqlx::query!(
                    "UPDATE StdSiteJobs SET state = ? WHERE date = ? AND processing_key = ? AND state = ?",
                    StdSiteJobState::JobNeeded,
                    proc_key,
                    date,
                    StdSiteJobState::MissingMet
                )
                .execute(&mut *conn)
                .await?;
                info!(
                    "{} rows in StdSiteJobs for {date} changed from 'MissingMet' to 'JobNeeded'",
                    res.rows_affected()
                );
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
    pub async fn add_jobs_for_pending_rows(
        conn: &mut MySqlConn,
        config: &Config,
    ) -> anyhow::Result<()> {
        // This will give us a series of records, one per date, that lists the site IDs and std. site job
        // table row IDs that need jobs for each data. We want to submit one job per date because ginput
        // can then calculate the EqL interpolators once for all the sites, rather than repeating that
        // work for every site.
        let query = sqlx::query!(
            r#"SELECT date,processing_key,GROUP_CONCAT(DISTINCT site_id SEPARATOR ",") AS site_ids,GROUP_CONCAT(DISTINCT id SEPARATOR ",") AS ids FROM v_StdSiteJobs WHERE state = ? GROUP BY date, processing_key;"#,
            StdSiteJobState::JobNeeded
        ).fetch_all(&mut *conn)
        .await?;

        // Determine a date after which jobs should get higher priority to ensure that "current"
        // jobs don't get delayed by backfill jobs. If this isn't configured, use a date in the future
        // to effectively disable this.
        let high_priority_date = if let Some(ndays) = config.execution.std_site_priority_days {
            debug!("execution.std_site_priority_days = {ndays}, will allocate higher priority to current standard site jobs");
            let today = chrono::Utc::now().date_naive();
            today - chrono::Duration::days(ndays)
        } else {
            debug!("execution.std_site_priority_days unset, will NOT allocate higher priority to current standard site jobs");
            let today = chrono::Utc::now().date_naive();
            today + chrono::Duration::days(1000)
        };

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

            let date = rec.date;
            let proc_key = ProcCfgKey(rec.processing_key);

            let day_state = crate::met::MetFile::is_date_complete_for_processing_config(
                conn, config, date, &proc_key,
            )
            .await?;

            if !day_state.is_complete() {
                warn!("Default meteorology not available for {}, setting these rows' states to MissingMet", rec.date);
                for rid in rids {
                    Self::set_state_by_id(conn, StdSiteJobState::MissingMet, rid).await
                        .with_context(|| format!("Error occurred while setting row {rid} in the standard site jobs table to state 'MissingMet'"))?;
                }
            } else {
                // For all the jobs, we'll just make all the possible output files, then put only the ones we want
                // into the tarballs later. This way we can still use one job per date, rather than splitting that
                // job up by what the different sites want.
                let priority = if rec.date >= high_priority_date {
                    debug!("Standard site job for {} receiving higher priority since it covers a recent period (on or after {})", rec.date, high_priority_date);
                    Some(100)
                } else {
                    None
                };
                let mut trans = conn.begin().await?;

                let job_id = jobs::Job::add_job_from_args_with_options(
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
                    priority,
                    None,
                    Some(jobs::TarChoice::No),
                    Some(&proc_key),
                )
                .await
                .with_context(|| {
                    format!(
                        "Error occurred while adding standard sites job for date {}",
                        rec.date
                    )
                })?;

                for rid in rids {
                    Self::set_job_by_id(&mut trans, rid, job_id).await
                    .with_context(|| format!("Error occurred while trying to update standard site job table row {rid} with job ID {job_id}"))?;
                }

                trans.commit().await?;
                let s_sids = rec.site_ids.as_deref().unwrap_or("?");
                info!(
                    "Created job #{job_id} for standard sites {s_sids} on date {}",
                    rec.date
                );
            }
        }

        Ok(())
    }

    pub async fn make_standard_site_tarballs(
        conn: &mut MySqlConn,
        config: &Config,
    ) -> anyhow::Result<()> {
        // Some things to keep in mind: (1) we should skip rows where the state != InProgress, because those
        // were probably updated while the jobs were running. (2) Different sites will have different output
        // structures, and we need to handle each one.
        let std_job_rows: Vec<StdSiteJob> = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE state = ? AND job IS NOT NULL;",
            StdSiteJobState::InProgress
        )
        .fetch_all(&mut *conn)
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
                }
                jobs::JobState::Errored => {
                    anyhow::bail!("Job {jid} required for standard sites had an error");
                }
                jobs::JobState::Cleaned => {
                    anyhow::bail!(
                        "Job {jid} required for standard sites was previously cleaned up"
                    );
                }
            }

            // Usually want to clean up finished jobs, as long as there isn't an error
            // and there aren't any lingering rows waiting for this job.
            job_ids.insert(jid);

            if row.state != StdSiteJobState::InProgress {
                warn!("Not making tarball for standard site job table row {}, state is not 'InProgress' suggesting a flag for regeneration or other change while the job was processing", row.id);
                continue;
            }

            info!(
                "Making standard site tarball ({} format) for {} on {}",
                row.output_structure, row.site_id, row.date
            );

            // Consistent with how we treat unexpected job states above, we'll error if the configuration
            // no longer defines the processing configuration used by this row. Perhaps this should be
            // more forgiving in the future, or perhaps this is more evidence that the configuration should
            // be partly moved into SQL tables.
            let proc_cfg = config.processing_configuration.get(&row.processing_key)
                .ok_or_else(|| anyhow!("Standard site job row {} uses a processing configuration missing from the config ('{}')", row.id, row.processing_key))?;
            // This should be handled by the config validation, but we check to be sure.
            let tar_root_dir = proc_cfg.auto_tarball_dir.as_deref().ok_or_else(|| {
                anyhow!(
                    "Processing configuration '{}' does not define a tarball output directory",
                    row.processing_key
                )
            })?;

            let output_tarballs = row.output_structure.make_std_site_tarball(
                &tar_root_dir,
                &row.site_id,
                &job,
                config,
            )?;
            if output_tarballs.len() > 1 {
                warn!("Multiple tarballs were created for standard site job {}, only the last one will be listed in the job's output file field", job.job_id);
            }

            let mut output_tarballs = output_tarballs.into_iter();
            if let Some(output_tarball) = output_tarballs.next() {
                row.set_complete(conn, output_tarball, None).await?;
            } else {
                anyhow::bail!(
                    "No output tarball path was returned while tarring job {}",
                    job.job_id
                )
            }
        }

        for jid in job_ids {
            // There is an occasional race condition that happens if a job completes while this function
            // is partway through making tarballs. A concrete example: a job had priors for ny, we, and zs.
            // This function started running before the job finished and got past all the ny rows in the StdSiteJobs
            // table. Then the job finished, so this function got the we and zs rows and cleaned up the job. That left
            // ny stuck because its job had already been cleaned up. So we do one last check that all rows referencing this
            // job are done.
            let count = sqlx::query!(
                "SELECT COUNT(*) AS n FROM v_StdSiteJobs WHERE state = ? AND job = ?",
                StdSiteJobState::InProgress,
                jid
            )
            .fetch_one(&mut *conn)
            .await?;

            if count.n == 0 {
                let mut job = jobs::Job::get_job_with_id(&mut *conn, jid).await?;
                job.set_cleaned(conn).await?;
                info!("Standard site job {jid} cleaned up");
            } else {
                warn!("Not cleaning up standard site job {jid} yet, some sites still pending (this should be handle the next time making tarballs runs)");
            }
        }

        Ok(())
    }

    /// Get a single row from the standard with job table by its row ID.
    pub async fn get_std_job_by_id(
        conn: &mut MySqlConn,
        ss_id: i32,
    ) -> anyhow::Result<Option<Self>> {
        let ssjob = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE id = ?",
            ss_id
        )
        .fetch_optional(conn)
        .await?
        .map(|q| StdSiteJob::from(q));

        Ok(ssjob)
    }

    /// Get the standard site job entry for the given site on the given date and
    /// for a given processing key.
    ///
    /// If no entry exists, will return `Ok(None)`. Will return an error if (a) the
    /// database query fails or (b) there is >1 entry that matches the site ID and date.
    pub async fn get_std_job_for_site_on_date(
        conn: &mut MySqlConn,
        site_id: &str,
        date: NaiveDate,
        proc_key: &ProcCfgKey,
    ) -> anyhow::Result<Option<Self>> {
        let mut ssjob = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE site_id = ? AND date = ? AND processing_key = ?",
            site_id,
            date,
            proc_key
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|q| StdSiteJob::from(q))
        .collect_vec();

        if ssjob.len() == 0 {
            Ok(None)
        } else if ssjob.len() == 1 {
            Ok(ssjob.pop())
        } else {
            anyhow::bail!(
                "Multiple standard site jobs matched '{site_id}' on {date} with processing key '{proc_key}'; databased inconsistent"
            )
        }
    }

    /// Flag rows in the standard site jobs table for a given site and a date range for priors regeneration.
    ///
    /// Unless you know whether this site and date range should be set as non-operational or not, you should prefer
    /// [`Self::set_regen_flag_by_site_info`] over this function.
    ///
    /// If `end_date` is `None`, then all rows from `start_date` on are flagged. Otherwise, rows up to but not
    /// including `end_date` are flagged. Returns the number of rows affected if successful. Returns an `Err`
    /// if the database query fails.
    pub async fn set_regen_flag(
        conn: &mut MySqlConn,
        site_id: &str,
        start_date: NaiveDate,
        end_date: Option<NaiveDate>,
        proc_key: Option<&ProcCfgKey>,
        set_inop: bool,
    ) -> anyhow::Result<u64> {
        let new_state = if set_inop {
            StdSiteJobState::NonopNeeded
        } else {
            StdSiteJobState::RegenNeeded
        };

        // Since standard site jobs cannot be in the future, setting the end date to
        // some time in the future is the same as having no end date.
        let end_date = end_date.unwrap_or_else(|| Utc::now().date_naive() + chrono::Days::new(30));

        let q = if let Some(pkey) = proc_key {
            sqlx::query!(
                "UPDATE v_StdSiteJobs SET state = ? WHERE site_id = ? AND date >= ? AND date < ? AND processing_key = ?",
                new_state,
                site_id,
                start_date,
                end_date,
                pkey
            )
            .execute(conn)
            .await?
        } else {
            sqlx::query!(
                "UPDATE v_StdSiteJobs SET state = ? WHERE site_id = ? AND date >= ? AND date < ?",
                new_state,
                site_id,
                start_date,
                end_date
            )
            .execute(conn)
            .await?
        };

        Ok(q.rows_affected())
    }

    /// Flag rows in the standard site for regeneration, intelligently deciding whether it requires regeneration or to be converted to non-operational status.
    ///
    /// If `end_date` is `None`, then all rows from `start_date` on are flagged. Otherwise, rows up to but not
    /// including `end_date` are flagged. Likewise, if `proc_key` is `None`, all rows for the enclosed dates
    /// are flagged, but if it is specified, only rows for that key are flagged.
    ///
    /// Returns an `Err` if the database query fails.
    ///
    /// If you want all the rows for this site and date range to be flagged as non-operational or not regardless
    /// of the site specifications, see [`Self::set_regen_flag`].
    pub async fn set_regen_flag_by_site_info(
        conn: &mut MySqlConn,
        site_id: &str,
        start_date: NaiveDate,
        end_date: Option<NaiveDate>,
        proc_key: Option<&ProcCfgKey>,
    ) -> anyhow::Result<()> {
        let mut trans = conn.begin().await?;
        let locations = SiteInfo::get_site_locations(&mut trans, site_id).await?;

        Self::set_regen_flag(&mut trans, site_id, start_date, end_date, proc_key, true).await?;
        for loc in locations {
            Self::set_regen_flag(
                &mut trans,
                site_id,
                loc.start_date,
                loc.end_date,
                proc_key,
                false,
            )
            .await?;
        }
        trans.commit().await?;
        Ok(())
    }

    /// Set the ginput job ID for a given standard site row.
    /// Used when the job ID is determined after the row is
    /// added.
    pub async fn set_job_by_id(
        conn: &mut MySqlConn,
        row_id: i32,
        job_id: i32,
    ) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteJobs SET state = ?, job = ? WHERE id = ?",
            StdSiteJobState::InProgress,
            job_id,
            row_id
        )
        .execute(conn)
        .await?;

        Ok(())
    }

    /// Set the row state to a given value. If you have a [`StdSiteJob`]
    /// instance, use [`Self::set_state`] instead.
    pub async fn set_state_by_id(
        conn: &mut MySqlConn,
        state: StdSiteJobState,
        row_id: i32,
    ) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteJobs SET state = ? WHERE id = ?",
            state,
            row_id
        )
        .execute(conn)
        .await?;

        Ok(())
    }

    /// Update the state of this instance and the corresponding row in the database.
    pub async fn set_state(
        &mut self,
        state: StdSiteJobState,
        conn: &mut MySqlConn,
    ) -> anyhow::Result<()> {
        Self::set_state_by_id(conn, state, self.id).await?;
        self.state = state;
        Ok(())
    }

    /// Set this entry to complete, including the path to the `ggg_inputs` tarball.
    ///
    /// Optionally, if the job ID has not yet been set, provide that as well.
    pub async fn set_complete(
        &mut self,
        conn: &mut MySqlConn,
        tarball: PathBuf,
        job_id: Option<i32>,
    ) -> anyhow::Result<()> {
        if let Some(jid) = job_id {
            sqlx::query!(
                "UPDATE StdSiteJobs SET state = ?, tarfile = ?, job = ? WHERE id = ?",
                StdSiteJobState::Complete,
                tarball.to_str().ok_or_else(|| anyhow::anyhow!(
                    "Could not convert tarball path to valid unicode"
                ))?,
                jid,
                self.id
            )
            .execute(conn)
            .await?;
        } else {
            sqlx::query!(
                "UPDATE StdSiteJobs SET state = ?, tarfile = ? WHERE id = ?",
                StdSiteJobState::Complete,
                tarball.to_str().ok_or_else(|| anyhow::anyhow!(
                    "Could not convert tarball path to valid unicode"
                ))?,
                self.id
            )
            .execute(conn)
            .await?;
        }

        self.state = StdSiteJobState::Complete;
        self.tarfile = Some(tarball);
        Ok(())
    }

    /// For this row in the standard site table, reset its state to "JobNeeded" and delete the output file, if present
    ///
    /// If an error occurs while deleting the output file, the database and this instance will not be updated. If the
    /// reset is successful, both the database and this instance will have the state set to "JobNeeded" and the output
    /// `tarfile` set to `None`.
    async fn clear_output_for_regen(
        &mut self,
        conn: &mut MySqlConn,
        needs_new_job: bool,
    ) -> anyhow::Result<()> {
        let mut trans = conn.begin().await?;

        if needs_new_job {
            sqlx::query!(
                "UPDATE StdSiteJobs SET state = ?, job = ?, tarfile = ? WHERE id = ?",
                StdSiteJobState::JobNeeded,
                None::<i32>,
                None::<String>,
                self.id
            )
            .execute(&mut *trans)
            .await
            .with_context(|| {
                format!(
                    "Error occurred trying to set state for row {} in StdSiteJobs table to {:?}",
                    self.id,
                    StdSiteJobState::JobNeeded
                )
            })?;
        } else {
            sqlx::query!("DELETE FROM StdSiteJobs WHERE id = ?", self.id)
                .execute(&mut *trans)
                .await
                .with_context(|| {
                    format!(
                        "Error occurred trying to remove row {} from StdSiteJobs table",
                        self.id
                    )
                })?;
        }

        if let Some(output) = &self.tarfile {
            std::fs::remove_file(output).with_context(|| {
                format!(
                    "Failed removing output file ({}) for standard site job entry {}",
                    output.display(),
                    self.id
                )
            })?;
        }

        // Using the transaction should ensure that if we can't remove the output file, the database doesn't think the file is missing when it
        // actually isn't
        trans.commit().await?;

        self.state = StdSiteJobState::JobNeeded;
        self.tarfile = None;

        Ok(())
    }

    /// Add/update entries in the standard site jobs table for the given tarballs.
    ///
    /// This will only add entries for tarballs that do not have a corresponding entry or update entries
    /// with a state of "job needed". Entries with other states will not be updated either because (a) the
    /// required inputs don't exist so the tarball isn't reproducible or (b) a job is already in the queue
    /// to generate this entry.
    pub async fn add_extant_files_to_std_site_records<P: AsRef<Path>>(
        conn: &mut MySqlConn,
        config: &Config,
        std_site_tarballs: &[P],
    ) -> anyhow::Result<()> {
        let all_jobs = jobs::Job::get_jobs_list(conn, false)
            .await
            .context("Failed occurred while getting list of all jobs")?;

        for tarball in std_site_tarballs {
            // For each tarball, check if we have an entry in the database for this site/date yet
            let tarball = tarball.as_ref();
            let (site_id, proc_key, date) = info_from_std_tarball_name(config, tarball)?;

            let existing_ssjob =
                Self::get_std_job_for_site_on_date(conn, &site_id, date, proc_key).await?;
            if let Some(mut existing_ssjob) = existing_ssjob {
                // If the standard site job already exists in the system and is waiting for a job to be created,
                // we can set it to complete right now.
                if let StdSiteJobState::JobNeeded = existing_ssjob.state {
                    if let Some(job) = Self::find_existing_job_for_standard_site(
                        &config.execution.std_site_job_queue,
                        site_id,
                        date,
                        &proc_key,
                        &all_jobs,
                    ) {
                        existing_ssjob
                            .set_complete(conn, tarball.to_path_buf(), Some(job.job_id))
                            .await?;
                        info!(
                            "Updated standard site job {} to use tarball {} and job {}",
                            existing_ssjob.id,
                            tarball.display(),
                            job.job_id
                        );
                    } else {
                        warn!("Could not use tarball {}, no job in the standard sites queue matches its site ID and date", tarball.display());
                    }
                } else {
                    info!(
                        "Did not update standard site job for {} because its state was '{}'.",
                        tarball.display(),
                        existing_ssjob.state
                    );
                }
            } else {
                // If there is no existing standard site job, then create it with no foreign key job ID
                let ss_id = Self::add_std_site_job_row_from_args(
                    conn,
                    &site_id,
                    date,
                    &proc_key,
                    StdSiteJobState::InProgress,
                    None,
                )
                .await?;
                let mut ss = Self::get_std_job_by_id(conn, ss_id).await?
                    .ok_or_else(|| anyhow::anyhow!("Failed to get the standard site job just created with ID = {ss_id} (this should not happen)"))?;
                ss.set_complete(conn, tarball.to_path_buf(), None).await?;
                info!(
                    "Created standard site job to use tarball {}",
                    tarball.display()
                );
            }
        }

        Ok(())
    }

    /// Given a list of all jobs in the database, find the most recently completed job in the standard site queue that
    /// generated the given site ID and date for the given processing configuration. Will return `None` if no jobs match
    /// those criteria, otherwise returns the job with the most recent completion date.
    fn find_existing_job_for_standard_site<'a>(
        std_site_queue: &str,
        site_id: String,
        date: NaiveDate,
        processing_key: &ProcCfgKey,
        all_jobs: &'a [jobs::Job],
    ) -> Option<&'a jobs::Job> {
        let mut matching_jobs = all_jobs
            .iter()
            .filter(|&j| {
                let job_proc_key = if let JobProcKey::Specified(k) = &j.processing_key {
                    k
                } else {
                    // In principle, from v3 on, only ad hoc jobs should use the default processing key, and those
                    // won't be the jobs we want. There may be some issues in the v2 -> v3 transition where standard
                    // site jobs were submitted before and so do have a null/default processing config, but since this
                    // function is currently only called when trying to add existing tarball to the database, that
                    // shouldn't cause a problem.
                    return false;
                };
                j.site_id.contains(&site_id)
                    && date >= j.start_date
                    && date < j.end_date
                    && j.complete_time.is_some()
                    && job_proc_key == processing_key
                    && j.queue.as_str() == std_site_queue
            })
            .collect_vec();

        if matching_jobs.is_empty() {
            return None;
        }

        if matching_jobs.len() == 1 {
            return matching_jobs.pop();
        }

        // Multiple jobs match, so find the one with the most recent completion time.
        // We already filtered for jobs with completion times above, so it's safe to unwrap
        // that field here. Since there's at least two elements in this iterator (given the
        // two if statements above), we know this will return Some(_).
        warn!("Multiple jobs could have produced the standard site {site_id} output for {date}, will use the most recently completed job.");
        matching_jobs.into_iter().reduce(|curr, new| {
            if new.complete_time.unwrap() > curr.complete_time.unwrap() {
                new
            } else {
                curr
            }
        })
    }

    /// Provide a summary the sites generated/pending/etc. for a given date
    pub async fn summarize_date_and_proc_cfg(
        conn: &mut MySqlConn,
        date: NaiveDate,
        proc_key: &ProcCfgKey,
    ) -> anyhow::Result<StdJobDateSummary> {
        let stdsites = siteinfo::SiteInfo::get_site_info_for_date(conn, date, false).await?;
        let all_site_ids = stdsites
            .iter()
            .map(|s| s.site_id.as_deref().unwrap_or("??"))
            .collect_vec();

        let rows = sqlx::query_as!(
            QStdSiteJob,
            "SELECT * FROM v_StdSiteJobs WHERE date = ? AND processing_key = ?",
            date,
            proc_key
        )
        .fetch_all(conn)
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
                StdSiteJobState::Unknown | StdSiteJobState::MissingMet => {
                    summary.other_sites.insert(row.site_id.clone())
                }
                StdSiteJobState::RegenNeeded | StdSiteJobState::NonopNeeded => {
                    summary.regen_sites.insert(row.site_id.clone())
                }
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

/// Given a path to a standard site tarball, extract the site ID and date.
///
/// Will error if:
/// - The filename does not follow the pattern "XX_ggg_inputs_YYYYMMDD.tgz"
/// - Could not get a filename from the given path.
pub fn info_from_std_tarball_name<'cfg>(
    config: &'cfg crate::config::Config,
    tarball: &Path,
) -> anyhow::Result<(String, &'cfg ProcCfgKey, NaiveDate)> {
    // First we need to find which processing configuration this is for.
    // We have to assume it's under the correct path for now; in the future,
    // we might add an override.
    let proc_key = proc_cfg_from_tarball_path(config, tarball)?;

    let filename = tarball
        .file_stem()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not get file name for standard site tarball to determine site ID and date"
            )
        })?
        .to_string_lossy();

    // Filename should have format ID_ggg_inputs_YYYYMMDD.tgz
    let mut parts = filename.split('_');
    let site_id = parts
        .next()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not get site ID from {filename} (failed to get first split on '_')"
            )
        })?
        .to_string();

    if parts.next() != Some("ggg") {
        anyhow::bail!("Standard site tarball {filename} does not match expected filename format (second split on '_' != 'ggg')");
    }

    if parts.next() != Some("inputs") {
        anyhow::bail!("Standard site tarball {filename} does not match expected filename format (third split on '_' != 'inputs')");
    }

    let datestr = parts.next().ok_or_else(|| {
        anyhow::anyhow!("Could not get date from {filename} (failed to get fourth split on '_')")
    })?;

    let date = NaiveDate::parse_from_str(datestr, "%Y%m%d")
        .with_context(|| format!("Date string ({datestr}) in {filename} could not be parsed."))?;

    Ok((site_id, proc_key, date))
}

fn proc_cfg_from_tarball_path<'cfg>(
    config: &'cfg crate::config::Config,
    p: &Path,
) -> anyhow::Result<&'cfg ProcCfgKey> {
    let canon_path = p.canonicalize().with_context(|| {
        anyhow!(
            "Error occurred getting canonical path for existing tarball, {}",
            p.display()
        )
    })?;

    for (key, cfg) in config.processing_configuration.iter() {
        if let Some(pc_path) = &cfg.auto_tarball_dir {
            let canon_pc_path = pc_path.canonicalize().with_context(|| {
                anyhow!("Error occurred getting canonical tarball path for processing config {key}")
            })?;
            if canon_path.starts_with(canon_pc_path) {
                return Ok(key);
            }
        }
    }

    Err(anyhow!("No processing configuration found with a tarball directory matching {} (original path = {})", canon_path.display(), p.display()))
}

pub struct AddStdJobSummary {
    pub job_id: i32,
    pub sites_included: Vec<String>,
}

#[derive(Debug, FromRow)]
struct QStdSiteJob {
    id: i32,
    #[allow(dead_code)]
    site: i32,
    site_id: Option<String>,
    site_type: Option<String>,
    date: NaiveDate,
    processing_key: String,
    state: i8,
    job: Option<i32>,
    tarfile: Option<String>,
    output_structure: Option<String>,
}

/// A version of [`QStdSiteJob`] to use when exporting database contents
/// (it omits fields that would be taken from the view).
#[derive(Debug, FromRow, Serialize, Deserialize)]
pub(crate) struct ExportStdSiteJob {
    pub(crate) id: i32,
    pub(crate) site: i32,
    pub(crate) date: NaiveDate,
    pub(crate) processing_key: Option<String>,
    pub(crate) state: i8,
    pub(crate) job: Option<i32>,
    pub(crate) tarfile: Option<String>,
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
        field.iter().sorted().join(",")
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
