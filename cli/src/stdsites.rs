use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use chrono::{Duration, NaiveDate};
use clap::{self, Args, Subcommand};
use itertools::Itertools;
use log::warn;
use orm::{config::ProcCfgKey, siteinfo::StdSite, stdsitejobs, utils::DateIterator, MySqlConn};
use sqlx::Connection;

/// Manage jobs for the standard sites
#[derive(Debug, Args)]
pub struct StdSiteJobCli {
    #[clap(subcommand)]
    pub command: StdSiteJobActions,
}

#[derive(Debug, Subcommand)]
pub enum StdSiteJobActions {
    /// Print a summary of standard site jobs
    Print(PrintStdJobsSummaryCli),

    /// Update the standard site jobs table: add rows for new site-days possible
    UpdateTable(UpdateTableCli),

    /// Add jobs to generate standard sites' priors for days in need of priors
    /// for which met data is available.
    AddJobs,

    /// Enter existing std. site tarballs into the database.
    UseExistingTars(UseExistingTarsCli),

    /// Collect completed standard site jobs outputs into the standard sites'
    /// tar files.
    TarFiles,

    /// Flag a range of dates for standard priors regeneration, either for
    /// all sites or a subset
    FlagForRegen(FlagForRegenCli),

    /// Submit jobs for a special run of standard sites, usually to test new priors
    SubmitSpecialRun(SpecialRunCli),

    /// Make tarballs of special run jobs submitted using submit-special-run
    TarSpecialRun(SpecialRunTarCli),
}

/// Update the standard site jobs table: add rows for new site-days possible
#[derive(Debug, Args)]
pub struct UpdateTableCli {
    #[clap(short = 'b', long)]
    not_before: Option<NaiveDate>,
}

#[derive(Debug, Args)]
pub struct FlagForRegenCli {
    /// Site ID to flag. Can provide this argument multiple times to flag multiple
    /// sites. Either this or --all-sites is required, but cannot have both.
    #[clap(short = 's', long)]
    site_id: Vec<String>,

    /// Flag all sites for regen. Either this or --site-id is required, but cannot have both.
    #[clap(long)]
    all_sites: bool,

    /// First date to flag
    start_date: NaiveDate,

    /// Date after the last date to flag; if not given, only START_DATE is flagged
    end_date: Option<NaiveDate>,
}

pub async fn update_std_site_job_table_cli(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
    args: UpdateTableCli,
) -> anyhow::Result<()> {
    update_std_site_job_table(conn, config, args.not_before).await
}

pub async fn update_std_site_job_table(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
    not_before: Option<NaiveDate>,
) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::update_std_site_job_table(conn, config, not_before).await?;
    Ok(())
}

pub async fn add_jobs_for_pending_rows(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::add_jobs_for_pending_rows(conn, config).await
}

pub async fn make_std_site_tarballs(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::make_standard_site_tarballs(conn, config).await
}

pub enum SitesToFlag {
    Some(Vec<String>),
    All,
}

pub async fn flag_for_regen_cli(conn: &mut MySqlConn, args: FlagForRegenCli) -> anyhow::Result<()> {
    let sites = if args.all_sites && !args.site_id.is_empty() {
        anyhow::bail!("--all-sites and --site-id are mutually exclusive");
    } else if args.all_sites {
        SitesToFlag::All
    } else {
        SitesToFlag::Some(args.site_id)
    };

    flag_for_regen(conn, args.start_date, args.end_date, sites).await
}

pub async fn flag_for_regen(
    conn: &mut MySqlConn,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    sites: SitesToFlag,
) -> anyhow::Result<()> {
    let end_date = end_date.unwrap_or_else(|| start_date + Duration::days(1));
    let site_ids = if let SitesToFlag::Some(sids) = sites {
        sids
    } else {
        StdSite::get_site_ids(conn, None).await?
    };

    for sid in site_ids {
        stdsitejobs::StdSiteJob::set_regen_flag_by_site_info(
            conn,
            &sid,
            start_date,
            Some(end_date),
        )
        .await?;
    }

    Ok(())
}

/// Print a table summarizing the progress of generating standard site priors
#[derive(Debug, Args)]
pub struct PrintStdJobsSummaryCli {
    /// First date to print in the table. If not given, defaults to 7 days ago.
    #[clap(short = 's', long)]
    start_date: Option<NaiveDate>,

    /// Date after the last one to print. If not given, defaults to today.
    #[clap(short = 'e', long)]
    end_date: Option<NaiveDate>,
}

pub async fn print_std_jobs_summary_cli(
    conn: &mut MySqlConn,
    args: PrintStdJobsSummaryCli,
) -> anyhow::Result<()> {
    let start_date = if let Some(sd) = args.start_date {
        sd
    } else {
        chrono::Utc::now().date_naive() - chrono::Duration::days(7)
    };

    print_std_jobs_summary(conn, start_date, args.end_date).await
}

pub async fn print_std_jobs_summary(
    conn: &mut MySqlConn,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<()> {
    let end_date = end_date.unwrap_or_else(|| chrono::Utc::now().date_naive());
    let mut summaries = vec![];
    for date in DateIterator::new_one_range(start_date, end_date) {
        let this_summary = stdsitejobs::StdSiteJob::summarize_date(conn, date).await?;
        summaries.push(this_summary);
    }

    println!("{}", orm::utils::to_std_table(summaries));

    Ok(())
}

// ------------ //
// SPECIAL RUNS //
// ------------ //

#[derive(Debug, Args)]
pub struct SpecialRunCli {
    /// A comma-separated list of site IDs for which to do the special run.
    /// If this is not given, then all sites active at any point during the
    /// requested time period will be run.
    #[clap(long)]
    site_ids: Option<String>,

    /// Key from the configuration to specify which processing configuration to use.
    /// If not given, the default for each date will be used.
    #[clap(long)]
    proc_key: Option<String>,

    /// Number of days per job submitted - if the number of days between the
    /// start and end dates exceeds this number, multiple jobs will be submitted.
    /// If this is omitted, then the entire date range will be one job.
    #[clap(long)]
    split_days: Option<u32>,

    /// Priority for these jobs (>0 will take precedence over regular user jobs)
    #[clap(long)]
    priority: Option<i32>,

    /// Email address to contact when the job is done. If not given, no
    /// email will be sent.
    #[clap(long)]
    email: Option<String>,

    /// Which queue to submit to. If not given, will use the submitted jobs queue.
    #[clap(long)]
    queue: Option<String>,

    /// First date to run for, in YYYY-MM-DD format
    start_date: NaiveDate,

    /// Day after the last date to run for, in YYYY-MM-DD format. Note that this
    /// is required, unlike many other commands where the end date is optional.
    end_date: NaiveDate,
}

pub async fn special_std_site_run_cli(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
    args: SpecialRunCli,
) -> anyhow::Result<()> {
    let site_ids = args
        .site_ids
        .map(|ids| ids.split(',').map(|s| s.to_owned()).collect_vec());
    let queue = args
        .queue
        .as_deref()
        .unwrap_or_else(|| &config.execution.submitted_job_queue);
    special_std_site_run(
        conn,
        config,
        site_ids,
        args.proc_key.map(ProcCfgKey::from),
        args.split_days,
        args.email,
        args.priority,
        args.start_date,
        args.end_date,
        queue,
    )
    .await
}

pub async fn special_std_site_run(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
    site_ids: Option<Vec<String>>,
    proc_cfg_key: Option<ProcCfgKey>,
    split_days: Option<u32>,
    email: Option<String>,
    priority: Option<i32>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    queue: &str,
) -> anyhow::Result<()> {
    // Check that our met and ginput keys are defined in the config and the met data we need is available
    if let Some(key) = &proc_cfg_key {
        let met_cfgs = config.get_mets_for_processing_config(key)?;
        for date in orm::utils::DateIterator::new_one_range(start_date, end_date) {
            let state =
                orm::met::MetFile::is_date_complete_for_config_set(conn, date, &met_cfgs).await?;
            if !state.is_complete() {
                anyhow::bail!("Required met not available for {date}");
            }
        }

        let ginput_key = &config
            .processing_configuration
            .get(&key)
            .expect(
                "A missing processing configuration key should have errored earlier in the code",
            )
            .ginput;
        config
            .execution
            .ginput
            .get(ginput_key)
            .ok_or_else(|| anyhow::anyhow!("No ginput key name '{ginput_key}"))?;
    } else {
        for date in orm::utils::DateIterator::new_one_range(start_date, end_date) {
            let state =
                orm::met::MetFile::is_date_complete_for_default_processing(conn, config, date)
                    .await?;
            if !state.is_complete() {
                anyhow::bail!("Required met not available for {date}");
            }
        }
    }

    // Now submit the job or jobs if we're breaking it up into multiple jobs to speed things up.
    let job_dates = special_job_date_ranges(start_date, end_date, split_days.map(|d| d as i64));
    let (site_ids, site_lats, site_lons) =
        special_job_locations(conn, start_date, end_date, site_ids).await?;
    for (start, end) in job_dates {
        let mut transaction = conn.begin().await?;
        let new_job_id = orm::jobs::Job::add_job_from_args_with_options(
            &mut transaction,
            site_ids.clone(),
            start,
            end,
            config.execution.output_path.clone(),
            email.clone(),
            site_lats.clone(),
            site_lons.clone(),
            queue,
            Some(orm::jobs::ModFmt::Text),
            Some(orm::jobs::VmrFmt::Text),
            Some(orm::jobs::MapFmt::TextAndNetCDF),
            priority,
            None,
            Some(orm::jobs::TarChoice::No),
            proc_cfg_key.as_ref(),
        )
        .await?;
        transaction.commit().await?;

        println!("Submitted job {new_job_id} for dates {start} to {end}");
    }
    Ok(())
}

fn special_job_date_ranges(
    start_date: NaiveDate,
    end_date: NaiveDate,
    split_days: Option<i64>,
) -> Vec<(NaiveDate, NaiveDate)> {
    if let Some(num_days) = split_days {
        orm::utils::split_date_range_by_days(start_date, end_date, num_days)
    } else {
        vec![(start_date, end_date)]
    }
}

async fn special_job_locations(
    conn: &mut MySqlConn,
    start_date: NaiveDate,
    end_date: NaiveDate,
    site_ids: Option<Vec<String>>,
) -> anyhow::Result<(Vec<String>, Vec<Option<f32>>, Vec<Option<f32>>)> {
    let site_ids = if let Some(sids) = site_ids {
        sids
    } else {
        let mut sids = HashSet::new();
        for (idate, date) in
            orm::utils::DateIterator::new_one_range(start_date, end_date).enumerate()
        {
            let active_site_info =
                orm::siteinfo::SiteInfo::get_site_info_for_date(conn, date, true).await?;

            // Because we rely on the default location defined for the sites, if a site does not have an entry for some dates
            // in the current range, that will cause a crash when the job goes to start. So we need to take only the subset of
            // sites that have a location defined for all dates in this range.
            if idate == 0 {
                for info in active_site_info {
                    sids.insert(
                        info.site_id
                            .expect("Foreign key for standard site ID should match"),
                    );
                }
            } else {
                let this_sid_set = HashSet::from_iter(active_site_info.into_iter().map(|info| {
                    info.site_id
                        .expect("Foreign key for standard site ID should match")
                }));

                let common = Vec::from_iter(sids.intersection(&this_sid_set).cloned());
                let to_remove = sids.clone().into_iter().filter(|sid| !common.contains(sid));
                for sid in to_remove {
                    warn!("{sid} does not have a location defined for all dates in {start_date} to {end_date}, it will be skipped in the corresponding job.");
                    sids.remove(&sid);
                }
            }
        }
        Vec::from_iter(sids.into_iter())
    };

    // Create vecs of `None` to indicate that we just use the default lat/lon for these sites.
    let lat = Vec::from_iter(site_ids.iter().map(|_| None));
    let lon = lat.clone();
    Ok((site_ids, lat, lon))
}

/// Make tarballs of special standard site runs
#[derive(Debug, Args)]
pub struct SpecialRunTarCli {
    /// Root directory to put the tarballs under. Note that this is required.
    #[clap(short = 'o', long)]
    tar_root_dir: PathBuf,

    /// Pass this argument to keep the original job output directory, rather than
    /// cleaning it up.
    #[clap(short = 'k', long)]
    keep_output: bool,

    /// The job ID numbers of jobs that we want to tar up.
    job_ids: Vec<i32>,
}

pub async fn tar_special_jobs_cli(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
    args: SpecialRunTarCli,
) -> anyhow::Result<()> {
    tar_special_jobs(
        conn,
        config,
        &args.job_ids,
        &args.tar_root_dir,
        args.keep_output,
    )
    .await
}

pub async fn tar_special_jobs(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
    job_ids: &[i32],
    tar_root_dir: &Path,
    keep_output: bool,
) -> anyhow::Result<()> {
    for jid in job_ids {
        let mut job = orm::jobs::Job::get_job_with_id(conn, *jid).await?;

        for site_id in job.site_id.iter() {
            // For each site, get its desired tarball format
            let site_info = if let Some(sid) =
                orm::siteinfo::StdSite::get_by_site_id(conn, site_id).await?
            {
                sid
            } else {
                log::warn!("Site {site_id} is not in the standard sites table; cannot make a tarball for it");
                continue;
            };

            site_info.output_structure.make_std_site_tarball(
                tar_root_dir,
                site_id,
                &job,
                config,
            )?;
        }

        if !keep_output {
            job.set_cleaned(conn).await?;
        }
    }

    Ok(())
}

/// Add existing standard site tarballs to the database.
///
/// If standard site tarballs exist without a corresponding entry in the StdSiteJobs
/// table, use this command to add them. It takes one or more paths to existing
/// standard site tarballs, then checks to see if the corresponding site/date pairs
/// have entries in the StdSiteJobs table. If not, and if it can find a job in the
/// standard sites queue that could have produced this file (i.e. includes that site
/// and date), then that tarball will be entered into the StdSiteJobs table.
///
/// Note that if there is already an entry in the StdSiteJobs table for the site/date
/// of a given tarball that is in any state other than "job needed", that tarball will
/// NOT be added. This protects against messing up the database by "completing" a standard
/// site job entry before the actual job runs.
#[derive(Debug, Args)]
pub struct UseExistingTarsCli {
    /// The existing ??_ggg_inputs_????????.tgz tarballs to put into the database.
    tarballs: Vec<PathBuf>,
}

pub async fn use_existing_tars_cli(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
    args: UseExistingTarsCli,
) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::add_extant_files_to_std_site_records(conn, config, &args.tarballs)
        .await
}
