use std::{collections::HashSet, path::{Path, PathBuf}};

use chrono::{NaiveDate, Duration};
use clap::{self, Subcommand, Args};
use itertools::Itertools;
use orm::{stdsitejobs,MySqlConn, siteinfo::StdSite, utils::DateIterator};

/// Manage jobs for the standard sites
#[derive(Debug, Args)]
pub struct StdSiteJobCli {
    #[clap(subcommand)]
    pub command: StdSiteJobActions
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
    #[clap(short='b', long)]
    not_before: Option<NaiveDate>
}

#[derive(Debug, Args)]
pub struct FlagForRegenCli {
    /// Site ID to flag. Can provide this argument multiple times to flag multiple
    /// sites. Either this or --all-sites is required, but cannot have both.
    #[clap(short='s', long)]
    site_id: Vec<String>,

    /// Flag all sites for regen. Either this or --site-id is required, but cannot have both.
    #[clap(long)]
    all_sites: bool,

    /// First date to flag
    start_date: NaiveDate,

    /// Date after the last date to flag; if not given, only START_DATE is flagged
    end_date: Option<NaiveDate>
}

pub async fn update_std_site_job_table_cli(conn: &mut MySqlConn, config: &orm::config::Config, args: UpdateTableCli) -> anyhow::Result<()> {
    update_std_site_job_table(conn, config, args.not_before).await
}

pub async fn update_std_site_job_table(conn: &mut MySqlConn, config: &orm::config::Config, not_before: Option<NaiveDate>) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::update_std_site_job_table(
        conn, 
        config,
        not_before
    ).await?;
    Ok(())
}

pub async fn add_jobs_for_pending_rows(conn: &mut MySqlConn, config: &orm::config::Config) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::add_jobs_for_pending_rows(conn, config).await
}

pub async fn make_std_site_tarballs(conn: &mut MySqlConn, config: &orm::config::Config) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::make_standard_site_tarballs(conn, config).await
}


pub enum SitesToFlag {
    Some(Vec<String>),
    All
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

pub async fn flag_for_regen(conn: &mut MySqlConn, start_date: NaiveDate, end_date: Option<NaiveDate>, sites: SitesToFlag) -> anyhow::Result<()> {
    let end_date = end_date.unwrap_or_else(|| start_date + Duration::days(1));
    let site_ids = if let SitesToFlag::Some(sids) = sites {
        sids
    } else {
        StdSite::get_site_ids(conn, None).await?
    };

    for sid in site_ids {
        stdsitejobs::StdSiteJob::set_regen_flag(conn, &sid, start_date, Some(end_date), true).await?;
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

pub async fn print_std_jobs_summary_cli(conn: &mut MySqlConn, args: PrintStdJobsSummaryCli) -> anyhow::Result<()> {
    let start_date = if let Some(sd) = args.start_date {
        sd
    } else {
        chrono::Utc::now().date_naive() - chrono::Duration::days(7)
    };

    print_std_jobs_summary(conn, start_date, args.end_date).await
}


pub async fn print_std_jobs_summary(conn: &mut MySqlConn, start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<()> {
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

    /// Key from the configuration to specify which ginput version to use.
    /// If not given, the default for each date will be used.
    #[clap(long)]
    ginput_key: Option<String>,

    /// Key from the configuration to specify which met data to use.
    /// If not given, the default for each date will be used.
    #[clap(long)]
    met_key: Option<String>,

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

    /// First date to run for, in YYYY-MM-DD format
    start_date: NaiveDate,

    /// Day after the last date to run for, in YYYY-MM-DD format. Note that this
    /// is required, unlike many other commands where the end date is optional.
    end_date: NaiveDate,
}


pub async fn special_std_site_run_cli(conn: &mut MySqlConn, config: &orm::config::Config, args: SpecialRunCli) -> anyhow::Result<()> {
    let site_ids = args.site_ids.map(|ids| ids.split(',').map(|s| s.to_owned()).collect_vec());
    special_std_site_run(
        conn,
        config,
        site_ids,
        args.ginput_key.as_deref(),
        args.met_key.as_deref(),
        args.split_days,
        args.email,
        args.priority,
        args.start_date,
        args.end_date
    ).await
}

pub async fn special_std_site_run(
    conn: &mut MySqlConn,
    config: &orm::config::Config,
    site_ids: Option<Vec<String>>,
    ginput_key: Option<&str>,
    met_key: Option<&str>,
    split_days: Option<u32>,
    email: Option<String>,
    priority: Option<i32>,
    start_date: NaiveDate,
    end_date: NaiveDate
) -> anyhow::Result<()> {

    // Check that our met and ginput keys are defined in the config and the met data we need is available
    if let Some(met) = met_key {
        let met_cfgs = config.get_met_configs(met)?;
        for date in orm::utils::DateIterator::new_one_range(start_date, end_date) {
            let state = orm::met::MetFile::is_date_complete_for_config_set(conn, date, met_cfgs).await?;
            if !state.is_complete() {
                anyhow::bail!("Required met not available for {date}");
            }
        }
    } else {
        for date in orm::utils::DateIterator::new_one_range(start_date, end_date) {
            let state = orm::met::MetFile::is_date_complete_for_default_mets(conn, config, date).await?;
            if !state.is_complete() {
                anyhow::bail!("Required met not available for {date}");
            }
        }
    }

    if let Some(ginput) = ginput_key {
        config.execution.ginput.get(ginput)
        .ok_or_else(|| anyhow::anyhow!("No ginput key name '{ginput}"))?;
    }

    
    // Now submit the job or jobs if we're breaking it up into multiple jobs to speed things up.
    let job_dates = special_job_date_ranges(start_date, end_date, split_days.map(|d| d as i64));
    let (site_ids, site_lats, site_lons) = special_job_locations(conn, start_date, end_date, site_ids).await?;
    for (start, end) in job_dates {
        let new_job_id = orm::jobs::Job::add_job_from_args_with_options(
            conn, 
            site_ids.clone(),
            start,
            end,
            config.execution.output_path.clone(),
            email.clone(),
            site_lats.clone(),
            site_lons.clone(),
            &config.execution.submitted_job_queue,
            Some(orm::jobs::ModFmt::Text),
            Some(orm::jobs::VmrFmt::Text),
            Some(orm::jobs::MapFmt::TextAndNetCDF),
            priority,
            None,
            Some(orm::jobs::TarChoice::No),
            met_key,
            ginput_key
        ).await?;

        println!("Submitted job {new_job_id} for dates {start} to {end}");
    }
    Ok(())
}

fn special_job_date_ranges(start_date: NaiveDate, end_date: NaiveDate, split_days: Option<i64>) -> Vec<(NaiveDate, NaiveDate)> {
    if let Some(num_days) = split_days {
        let mut ranges = vec![];
        let mut curr_start = start_date;
        while curr_start < end_date {
            let mut curr_end = curr_start + chrono::Duration::days(num_days);
            dbg!(curr_end);
            if curr_end > end_date {
                curr_end = end_date;
            }
            dbg!(curr_end);
            ranges.push((curr_start, curr_end));
            curr_start += chrono::Duration::days(num_days);
        }
        ranges
    } else {
        vec![(start_date, end_date)]
    }
}

async fn special_job_locations(conn: &mut MySqlConn, start_date: NaiveDate, end_date: NaiveDate, site_ids: Option<Vec<String>>) -> anyhow::Result<(Vec<String>, Vec<Option<f32>>, Vec<Option<f32>>)> {
    let site_ids = if let Some(sids) = site_ids {
        sids
    } else {
        let mut sids = HashSet::new();
        for date in orm::utils::DateIterator::new_one_range(start_date, end_date) {
            let active_site_info = orm::siteinfo::SiteInfo::get_site_info_for_date(conn, date, true).await?;
            for info in active_site_info {
                sids.insert(info.site_id.expect("Foreign key for standard site ID should match"));
            }
        }
        Vec::from_iter(sids.into_iter())
    };

    let lat = Vec::from_iter(site_ids.iter().map(|_| None));
    let lon = lat.clone();
    Ok((site_ids, lat, lon))
}


/// Make tarballs of special standard site runs
#[derive(Debug, Args)]
pub struct SpecialRunTarCli {
    /// Root directory to put the tarballs under. Note that this is required.
    #[clap(short='o', long)]
    tar_root_dir: PathBuf,

    /// Pass this argument to keep the original job output directory, rather than
    /// cleaning it up.
    #[clap(short='k', long)]
    keep_output: bool,

    /// The job ID numbers of jobs that we want to tar up.
    job_ids: Vec<i32>,
}

pub async fn tar_special_jobs_cli(conn: &mut MySqlConn, config: &orm::config::Config, args: SpecialRunTarCli) -> anyhow::Result<()> {
    tar_special_jobs(conn, config, &args.job_ids, &args.tar_root_dir, args.keep_output).await
}

pub async fn tar_special_jobs(conn: &mut MySqlConn, config: &orm::config::Config, job_ids: &[i32], tar_root_dir: &Path, keep_output: bool) -> anyhow::Result<()> {
    for jid in job_ids {
        let mut job = orm::jobs::Job::get_job_with_id(conn, *jid).await?;

        for site_id in job.site_id.iter() {
            // For each site, get its desired tarball format
            let site_info = if let Some(sid) = orm::siteinfo::StdSite::get_by_site_id(conn, site_id).await? {
                sid
            } else {
                log::warn!("Site {site_id} is not in the standard sites table; cannot make a tarball for it");
                continue;
            };
            
            site_info.output_structure.make_std_site_tarball(tar_root_dir, site_id, &job, config)?;
        }

        if !keep_output {
            job.set_cleaned(conn).await?;
        }
    }
    
    Ok(())
}