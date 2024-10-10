use std::{fmt::Display, path::PathBuf, str::FromStr};

use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime};
use clap::{self, Args, Subcommand};
use itertools::Itertools;
use log::{debug, info};
use orm::{config::Config, error::JobError, jobs::{Job, JobState, MapFmt, ModFmt, TarChoice, VmrFmt}, MySqlConn};


/// Manage ginput jobs
#[derive(Debug, Args)]
pub struct JobCli {
    #[clap(subcommand)]
    pub commands: JobActions
}

#[derive(Debug, Subcommand)]
pub enum JobActions {
    /// Add a new job to the database
    Add(AddJobCli),

    /// Reset a job to pending, clearing any output
    Reset(ResetJobCli),

    /// Delete a job, clearing any output
    Delete(DeleteJobCli),

    /// Delete output and update status of expired jobs
    CleanExpired(CleanExpiredCli),

    /// Delete output from jobs that errored
    CleanErrored(CleanErroredCli),

    /// Update deletion times for one or more jobs
    ChangeDeleteTime(ChangeDeleteTimeCli),

    /// Change the priority of a job
    SetPriority(SetPriorityCli),

    /// Print jobs in the database
    Print(PrintJobsCli),

    /// Describe the current state of the job
    Status(JobStatusCli),
}

#[derive(Debug, Args)]
#[clap(setting = clap::AppSettings::DeriveDisplayOrder)]
/// Add a job manually from the command line
pub struct AddJobCli {
    
    #[clap(long="mod-fmt")]
    /// What format to output the .mod files in ("none" or "text"). 
    /// Default is "text".
    mod_fmt: Option<ModFmt>,
    #[clap(long="vmr-fmt")]
    /// What format to output the .vmr files in ("none" or "text"). 
    /// Default is "text".
    vmr_fmt: Option<VmrFmt>,
    #[clap(long="map-fmt")]
    /// What format to output the .map files in ("none", "text", or "netcdf"). 
    /// Default is "text".
    map_fmt: Option<MapFmt>,
    #[clap(short='p', long="priority")]
    /// Priority to give this job; higher will be run before jobs with lower values.
    priority: Option<i32>,
    #[clap(long="no-delete")]
    /// Never delete the output files from this job.
    no_delete: bool,
    #[clap(short='t', long="to-tarball")]
    /// Pack the output files from this job into a single tarball.
    to_tarball: bool,

    /// Pack the output files into an EGI-naming convention tarball.
    /// Mututally exclusive with --to-tarball
    #[clap(long)]
    egi_tarball: bool,

    /// Which queue to add the job to, if not given, then will use the submitted
    /// job queue defined in the config.
    #[clap(long)]
    queue: Option<String>,

    /// The two-letter site IDs used to identify the output in this job. 
    /// Pass multiple site IDs as a comma-separated list. If multiple lat/lons
    /// are given, the number of site IDs must be 1 or equal to the number of
    /// lat/lons. If lat/lons are not given, then these site IDs must be recognized
    /// as standard sites.
    site_id: String,

    /// The first date to generate priors for (inclusive), in YYYY-MM-DD format.
    start_date: NaiveDate,

    /// The last date to generate priors for (exclusive), in YYYY-MM-DD format.
    end_date: NaiveDate,

    /// The email address to contact when the priors are ready
    email: String,


    #[clap(allow_hyphen_values = true)]
    /// The latitudes to generate priors for. May be omitted if all SITE_ID values are standard sites.
    /// Note that if a latitude is provided for any locations, it must be provided for ALL locations;
    /// there is no way to use the default standard site location for only some sites in a single submission.
    /// See help text for SITE_ID for information on the interaction between the number of site IDs and
    /// lat/lon coordinates.
    lat: Option<String>,

    #[clap(allow_hyphen_values = true)]
    /// The longitudes to generate priors for. Same caveats as latitudes apply, must have the same number of latitudes as longitudes.
    lon: Option<String>
}

#[derive(Debug)]
pub struct AddJobArgs {
    site_id: Vec<String>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    email: String,
    lat: Vec<Option<f32>>,
    lon: Vec<Option<f32>>,
    queue: Option<String>,
    mod_fmt: Option<ModFmt>,
    vmr_fmt: Option<VmrFmt>,
    map_fmt: Option<MapFmt>,
    priority: Option<i32>,
    delete_time: Option<NaiveDateTime>,
    save_dir: PathBuf,
    save_tarball: TarChoice
}

/// Reset a job, deleting a run directory or output, and setting its status to 'pending'
#[derive(Debug, Args)]
pub struct ResetJobCli {
    /// The job ID to reset
    id: i32,
}

pub async fn reset_job(db: &mut orm::MySqlConn, clargs: ResetJobCli) -> anyhow::Result<()> {
    orm::jobs::Job::reset_job_with_id(db, clargs.id).await?;
    println!("Reset job #{}", clargs.id);
    Ok(())
}

#[derive(Debug, Args)]
/// Delete a job from the command line
pub struct DeleteJobCli {
    /// The job ID to delete.
    id: i32,
}

pub async fn delete_job(db: &mut orm::MySqlConn, clargs: DeleteJobCli) -> anyhow::Result<()> {
    let n_deleted = orm::jobs::Job::delete_job_with_id(db, clargs.id).await?;
    println!("Deleted {n_deleted} job(s)");
    Ok(())
}

/// Change the priority of a job
#[derive(Debug, Args)]
pub struct SetPriorityCli {
    /// The job ID to update
    id: i32,

    /// The new priority to give this job
    #[clap(allow_hyphen_values = true)]
    new_priority: i32,

    /// By default, only jobs that are pending can have their priority updated
    /// (because it will only affect them). Use this flag to allow changing any
    /// job's priority.
    #[clap(short='a', long)]
    allow_any_state: bool,
}

pub async fn set_job_priority_cli(conn: &mut MySqlConn, args: SetPriorityCli) -> anyhow::Result<()> {
    let res = Job::set_priority_by_id(args.id, conn, args.new_priority, args.allow_any_state).await;
    match res {
        Ok(_) => Ok(()),
        Err(orm::error::JobPriorityError::StateNotPending) => Err(anyhow::anyhow!("{}. To allow changing this job's priority, use --allow-any-state.", orm::error::JobPriorityError::StateNotPending)),
        Err(e) => Err(e.into())
    }
}

impl AddJobArgs {
    fn convert_from_clargs(clargs: AddJobCli, config: &Config) -> Result<Self, anyhow::Error> {
        if clargs.lat.is_none() != clargs.lon.is_none() {
            anyhow::bail!("lat and lon must be both given or neither given");
        }
        let site_ids = orm::jobs::Job::parse_site_id_str(&clargs.site_id)?;

        let lat = orm::jobs::Job::parse_lat_str(&clargs.lat.unwrap_or("".to_owned())).context("Problem with given latitude.")?;
        let lon = orm::jobs::Job::parse_lon_str(&clargs.lon.unwrap_or("".to_owned())).context("Problem with given longitude.")?;
        let (site_ids, lat, lon) = orm::jobs::Job::expand_site_lat_lon(site_ids, lat, lon)?;

        let delete_time = if clargs.no_delete {
            None
        }else{
            let now = chrono::Local::now().naive_local();
            Some(now + chrono::Duration::hours(config.execution.hours_to_keep as i64))
        };

        let save_tarball = if clargs.to_tarball && clargs.egi_tarball {
            anyhow::bail!("Cannot have both --to-tarball and --egi-tarball");
        } else if clargs.to_tarball {
            TarChoice::Yes
        }else if clargs.egi_tarball {
            TarChoice::Egi
        } else {
            TarChoice::No
        };

        let save_dir = config.execution.output_path.clone();
        
        Ok(Self { 
            site_id: site_ids,
            start_date: clargs.start_date,
            end_date: clargs.end_date,
            email: clargs.email,
            lat: lat,
            lon: lon,
            queue: clargs.queue,
            mod_fmt: clargs.mod_fmt,
            vmr_fmt: clargs.vmr_fmt,
            map_fmt: clargs.map_fmt,
            priority: clargs.priority,
            delete_time: delete_time,
            save_dir: save_dir,
            save_tarball: save_tarball
        })
    }
}

pub async fn add_job(db: &mut orm::MySqlConn, clargs: AddJobCli, config: &Config) -> anyhow::Result<()> {
    let args = AddJobArgs::convert_from_clargs(clargs, config)?;
    let id = Job::add_job_from_args(db, 
        args.site_id,
        args.start_date, 
        args.end_date, 
        args.save_dir, 
        Some(args.email), 
        args.lat,
        args.lon,
        &args.queue.unwrap_or_else(|| config.execution.submitted_job_queue.clone()),
        args.mod_fmt, 
        args.vmr_fmt, 
        args.map_fmt, 
        args.priority, 
        args.delete_time, 
        Some(args.save_tarball))
    .await?;
    println!("Added new job, ID = {id}");
    Ok(())
}

/// Delete output for jobs whose delete time has passed and set their
/// status to "cleaned".
#[derive(Debug, Args)]
pub struct CleanExpiredCli {
    /// Do not actually delete output or change status, just print what would occur.
    #[clap(short='d', long)]
    dry_run: bool,
}

pub async fn clean_expired_jobs_cli(conn: &mut MySqlConn, args: CleanExpiredCli) -> anyhow::Result<()> {
    Job::clean_up_expired_jobs(conn, args.dry_run).await
}

/// Delete any existing output for jobs that errored. The job remains in the
/// database
#[derive(Debug, Args)]
pub struct CleanErroredCli {
    /// The earliest submission date to delete, only jobs with a submission
    /// time of or after midnight of this date will have their output deleted
    #[clap(short = 's', long)]
    not_before: Option<NaiveDate>,

    /// The last (exclusive) date to delete, only jobs with a submission time
    /// before midnight on this date will have their output deleted.
    #[clap(short = 'e', long)]
    not_after: Option<NaiveDate>,

    /// Do not actually delete output, only print which jobs' output will be
    /// deleted
    #[clap(short = 'd', long)]
    dry_run: bool,
}

pub async fn clean_errored_jobs_cli(conn: &mut MySqlConn, args: CleanErroredCli) -> anyhow::Result<()> {
    clean_errored_jobs(conn, args.not_before, args.not_after, args.dry_run).await
}

pub async fn clean_errored_jobs(
    conn: &mut MySqlConn,
    not_before: Option<NaiveDate>,
    not_after: Option<NaiveDate>,
    dry_run: bool
) -> anyhow::Result<()> 
{
    let not_before = not_before.map(|d| d.and_hms_opt(0, 0, 0).unwrap());
    let not_after = not_after.map(|d| d.and_hms_opt(0, 0, 0).unwrap());
    let errored_jobs = Job::get_jobs_in_state(conn, orm::jobs::JobState::Errored).await
        .context("Error occured getting the list of errors jobs")?;

    for job in errored_jobs {
        if let Some(start) = not_before {
            if job.submit_time < start { continue; }
        }

        if let Some(end) = not_after {
            if job.submit_time >= end { continue; }
        }

        if dry_run {
            println!("Would clean up output for job #{}", job.job_id);
        } else {
            job.delete_output_and_run_dir()
                .with_context(|| format!("Error occured cleaning up output for job #{}", job.job_id))?;
            println!("Output for job #{} cleaned up", job.job_id);
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum JobStateFilter {
    Any,
    PendingAndRunning,
    Only(Vec<JobState>)
}

impl JobStateFilter {
    /// Return `true` if only searching for pending and running jobs. If so, we can do a
    /// more optimized search for jobs, since the Job query function has a special case
    /// for that subset which only queries those jobs in the database.
    pub fn pending_running_only(&self) -> bool {
        match self {
            JobStateFilter::Any => false,
            JobStateFilter::PendingAndRunning => true,
            JobStateFilter::Only(states) => {
                states.len() == 2 && states.contains(&JobState::Pending) && states.contains(&JobState::Running)
            },
        }
    }
}

impl Display for JobStateFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStateFilter::Any => write!(f, "any"),
            JobStateFilter::PendingAndRunning => write!(f, "default"),
            JobStateFilter::Only(states) => {
                for (i, s) in states.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{s}")?;
                }
                Ok(())
            },
        }
    }
}

impl FromStr for JobStateFilter {
    type Err = JobError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.to_lowercase();
        if s_lower == "all" {
            return Ok(Self::Any);
        }

        if s_lower == "default" {
            return Ok(Self::PendingAndRunning);
        }

        let states: Vec<_> = s.split(',')
            .map(|x| JobState::from_str(x))
            .try_collect()?;

        Ok(Self::Only(states))
    }
}

impl Default for JobStateFilter {
    fn default() -> Self {
        Self::PendingAndRunning
    }
}

/// Print out jobs, either in a table or with more details
#[derive(Debug, Args)]
pub struct PrintJobsCli {
    /// Print out details descriptions of all matching jobs, rather than a table.
    /// Note that this is the only way to get all the information about jobs; many
    /// fields are omitted from the table to keep its width reasonable.
    #[clap(short = 'd', long)]
    details: bool,

    /// List jobs in certain states. The default is pending and running. Other valid
    /// strings are "all" (all jobs), "pending" or "p", "running" or "r", "complete" or "d",
    /// "errored" or "e", and "cleaned" or "x". This has no effect if --job-id is specified.
    #[clap(short='s', long, default_value_t = JobStateFilter::default())]
    states: JobStateFilter,

    /// Shorthand for --states=all. Mutually exclusive with --states.
    #[clap(long, conflicts_with = "states")]
    all: bool,

    /// Limit to certain job IDs, repeat this argument to specify multiple
    /// job IDs. If given, these jobs will be displayed regardless of their
    /// state (i.e. --states will have no effect). 
    #[clap(short = 'j', long)]
    job_id: Vec<i32>,

    /// Limit jobs to those submitted on or after this date
    #[clap(short = 'a', long)]
    submitted_after: Option<NaiveDate>,
        
    /// Limit jobs to those submitted before this date
    #[clap(short = 'b', long)]
    submitted_before: Option<NaiveDate>,

    /// Limit jobs to those submitted under this email. Use "NONE" to filter for jobs submitted without an email.
    #[clap(short = 'e', long)]
    submitter_email: Option<String>
}

pub async fn print_jobs_table_cli(conn: &mut MySqlConn, args: PrintJobsCli) -> anyhow::Result<()> {
    let states = if args.all {
        JobStateFilter::Any
    } else {
        args.states
    };

    print_jobs_table(
        conn, 
        args.details,
        states,
        args.job_id.as_slice(),
        (args.submitted_after, args.submitted_before),
        args.submitter_email.as_deref()
    ).await
}

pub async fn print_jobs_table(
    conn: &mut MySqlConn,
    detailed: bool,
    state_filter: JobStateFilter,
    job_ids: &[i32],
    submit_date_range: (Option<NaiveDate>, Option<NaiveDate>),
    submit_email: Option<&str>
) -> anyhow::Result<()> {
    let state_filter = if !job_ids.is_empty() {
        JobStateFilter::Any
    } else {
        state_filter
    };

    let jobs = Job::get_jobs_list(conn, state_filter.pending_running_only()).await?;

    // Because the filtering is kind of specific to this function, we'll do it in Rust here
    // rather than as specific SQL queries.
    let submit_start = submit_date_range.0.map(|d| d.and_hms_opt(0, 0, 0).unwrap());
    let submit_end = submit_date_range.1.map(|d| d.and_hms_opt(0, 0, 0).unwrap());

    let jobs = jobs.into_iter()
        .filter(|j| {
            if let Some(start) = submit_start {
                if j.submit_time < start { return false; }
            }

            if let Some(end) = submit_end {
                if j.submit_time >= end { return false; }
            }

            if !job_ids.is_empty() {
                if !job_ids.contains(&j.job_id) {
                    return false;
                }
            }

            if let Some(filter_email) = submit_email {
                return j.email.as_deref().unwrap_or("NONE") == filter_email;
            }

            if let JobStateFilter::Only(states) = &state_filter {
                return states.contains(&j.state);
            }

            return true;
        }).map(|mut j| {
            // Email should be the only field where a used string is
            // stored. Carriage returns mess up the table, so escape them.
            j.email = j.email.map(|s| s.replace('\r', "\\r"));
            j
        });

    if detailed {
        let mut at_least_one = false;
        for job in jobs {
            println!("{}", job.verbose_display());
            at_least_one = true;
        }
        if !at_least_one {
            println!("No jobs matching given criteria.");
        }
    } else {
        let table = orm::utils::to_std_table(jobs);
        println!("{table}");
    }

    Ok(())
}


#[derive(Debug, Args)]
pub struct JobStatusCli {
    /// ID number of the job to check the status of
    job_id: i32,
}

pub async fn print_job_status_cli(conn: &mut MySqlConn, args: JobStatusCli) -> anyhow::Result<()> {
    print_job_status(conn, args.job_id).await
}


pub async fn print_job_status(conn: &mut MySqlConn, job_id: i32) -> anyhow::Result<()> {
    let job = orm::jobs::Job::get_job_with_id(conn, job_id).await
        .context("Error occurred while retrieving the job from the database")?;

    let run_dir = job.run_dir(false);
    println!("Job {}:", job.job_id);
    // Loop through the ginput run args JSON files; they will be created for each day as it is run,
    // so we can use the first one's creation/modification time to get when the job started as well
    // as their presence to count how many days are done or in progress.
    let (start_time, ndays) = orm::utils::DateIterator::new_one_range(job.start_date, job.end_date)
        .fold((None, 0), |(time, n), date| {
            let run_args_file = orm::jobs::run_arg_file(&run_dir, date);
            // Creation time would be better, but I found the ccycle filesystem
            // doesn't provide that
            match orm::utils::get_file_modification_time(&run_args_file) {
                Ok(ctime) => {
                    let new_time = time
                        .map(|t| if t < ctime { t } else { ctime })
                        .unwrap_or(ctime);
                    (Some(new_time), n+1)
                },
                Err(e) => {
                    debug!("While querying run arguments for {date}: {e}");
                    (time, n)
                }
            }
        });

    if let Some(start_time) = start_time {
        let run_dur = chrono::Local::now() - start_time;
        println!("  Started at {}, running for {}", start_time.format("%Y-%m-%d %H:%M:%S"), orm::utils::duration_string(run_dur));
        let total_ndays = (job.end_date - job.start_date).num_days();
        println!("  Generating day {ndays} of {total_ndays}");
    } else if job.state.is_over() {
        println!("  Job is finished.");
        if let Some(p) = job.output_file {
            println!("  Output to {}", p.display());
        } else {
            println!("  No output.");
        }
        return Ok(());
    } else {
        println!("  Job not yet executing (no run args files under {}).", run_dir.display());
        return Ok(());
    }

    // Also count the .mod, .vmr, and .map files output; if the output directory is cleaned up,
    // we'll have already returned.
    let (nmod, nvmr, nmap) = walkdir::WalkDir::new(&run_dir)
        .into_iter()
        .filter_entry(|entry| entry.file_name().to_str().map(|s| !s.starts_with(".")).unwrap_or(true))
        .fold((0, 0, 0), |(mods, vmrs, maps), entry| {
            if let Ok(entry) = entry {
                let name = entry.file_name().to_str().unwrap_or("");
                if name.ends_with(".mod") {
                    (mods +1, vmrs, maps)
                } else if name.ends_with(".vmr") {
                    (mods, vmrs + 1, maps)
                } else if name.ends_with(".map") || name.ends_with(".map.nc") {
                    (mods, vmrs, maps + 1)
                } else {
                    (mods, vmrs, maps)
                }
            } else {
                (mods, vmrs, maps)
            }
        });

    println!("  Output files so far: {nmod} .mod / {nvmr} .vmr / {nmap} .map or .map.nc");
    println!("  under {}", run_dir.display());

    Ok(())
}


#[derive(Debug, Args)]
pub struct ChangeDeleteTimeCli {
    /// Time to update jobs to. If not given, then will be computed 
    /// as now plus the configured delay
    #[clap(short='t', long)]
    delete_time: Option<NaiveDateTime>,
    
    /// Job IDs to update.
    #[clap(short='j', long)]
    job_id: Vec<i32>,

    /// The name of a queue; if given, jobs with a NULL deletion time in
    /// that queue will have their deletion time updated. This works as 
    /// an intersection with --job-id
    #[clap(short='n', long)]
    null_in_queue: Option<String>,

    /// Set this to only print what will happen.
    #[clap(short='d', long)]
    dry_run: bool,
}

pub async fn change_jobs_delete_time_cli(conn: &mut MySqlConn, config: &Config, args: ChangeDeleteTimeCli) -> anyhow::Result<()> {
    change_jobs_delete_time(conn, config, args.delete_time, &args.job_id, args.null_in_queue.as_deref(), args.dry_run).await
}

pub async fn change_jobs_delete_time(conn: &mut MySqlConn, config: &Config, delete_time: Option<NaiveDateTime>, job_ids: &[i32], null_in_queue: Option<&str>, dry_run: bool) -> anyhow::Result<()> {
    let jobs = if let Some(queue) = null_in_queue {
        Job::get_jobs_in_queue(conn, queue).await?
    } else {
        Job::get_jobs_list(conn, false).await?
    };

    let delete_time = Some(delete_time.unwrap_or_else(|| {
        let now = chrono::Local::now().naive_local();
        now + chrono::Duration::hours(config.execution.hours_to_keep.into())
    }));

    let mut nchanged = 0;
    for job in jobs {
        if null_in_queue.is_some() && job.delete_time.is_some() {
            continue;
        }

        if !job_ids.is_empty() && !job_ids.contains(&job.job_id) {
            continue;
        }

        if dry_run {
            println!("Would change delete time on job {} from {:?} to {:?}", job.job_id, job.delete_time, delete_time);
        } else {
            Job::set_delete_time_by_id(job.job_id, conn, delete_time).await?;
            debug!("Updated deletion time for job {}", job.job_id);
        }
        nchanged += 1;
    }
    info!("{nchanged} jobs deletion times updated to {}", delete_time.unwrap());
    Ok(())
}