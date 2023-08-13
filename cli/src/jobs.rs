use std::path::PathBuf;

use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime};
use clap::{self, Args};
// use clap::builder::AppSettings;
use orm::{jobs::{Job, ModFmt, VmrFmt, MapFmt, TarChoice}, MySqlConn, config::Config};
use tabled::Table;


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

#[derive(Debug, Args)]
/// Delete a pending job from the command line
pub struct DeleteJobCli {
    /// The job ID to delete. Will have no effect if the job has already run.
    id: i32,
}

impl AddJobArgs {
    fn convert_from_clargs(clargs: AddJobCli, config: &Config) -> Result<Self, anyhow::Error> {
        if clargs.lat.is_none() != clargs.lon.is_none() {
            anyhow::bail!("lat and lon must be both given or neither given");
        }
        let site_ids = orm::jobs::Job::parse_site_id_str(&clargs.site_id);

        let lat = orm::jobs::Job::parse_lat_str(&clargs.lat.unwrap_or("".to_owned())).context("Problem with given latitude.")?;
        let lon = orm::jobs::Job::parse_lon_str(&clargs.lon.unwrap_or("".to_owned())).context("Problem with given longitude.")?;
        let (site_ids, lat, lon) = orm::jobs::Job::expand_site_lat_lon(site_ids, lat, lon)?;

        let delete_time = if clargs.no_delete {
            None
        }else{
            let now = chrono::Local::now().naive_local();
            Some(now + chrono::Duration::hours(config.execution.hours_to_keep as i64))
        };

        let save_tarball = if clargs.to_tarball {
            TarChoice::Yes
        }else{
            TarChoice::No
        };

        let save_dir = config.execution.output_path.clone();
        dbg!(&save_dir);
        
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

pub async fn delete_job(db: &mut orm::MySqlConn, clargs: DeleteJobCli) -> anyhow::Result<()> {
    let n_deleted = orm::jobs::Job::delete_job_with_id(db, clargs.id).await?;
    println!("Deleted {n_deleted} job(s)");
    Ok(())
}

/// Print out jobs, either in a table or with more details
#[derive(Debug, Args)]
pub struct PrintJobsCli {
    /// Print out details descriptions of all matching jobs, rather than a table.
    /// Note that this is the only way to get all the information about jobs; many
    /// fields are omitted from the table to keep its width reasonable.
    #[clap(short = 'd', long)]
    details: bool,

    /// List all jobs meeting the other criteria, not just pending jobs
    #[clap(long)]
    all: bool,

    /// Limit to certain job IDs, repeat this argument to specify multiple
    /// job IDs.
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
    print_jobs_table(
        conn, 
        args.details,
        !args.all,
        args.job_id.as_slice(),
        (args.submitted_after, args.submitted_before),
        args.submitter_email.as_deref()
    ).await
}

pub async fn print_jobs_table(
    conn: &mut MySqlConn,
    detailed: bool,
    pending_only: bool,
    job_ids: &[i32],
    submit_date_range: (Option<NaiveDate>, Option<NaiveDate>),
    submit_email: Option<&str>
) -> anyhow::Result<()> {
    let jobs = Job::get_jobs_list(conn, pending_only).await?;

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

            return true;
        });

    if detailed {
        let mut at_least_one = false;
        for job in jobs {
            println!("{job}");
            at_least_one = true;
        }
        if !at_least_one {
            println!("No jobs matching given criteria.");
        }
    } else {
        let table_config = tabled::settings::Settings::default()
            .with(tabled::settings::Style::markdown());
        let table = Table::new(jobs)
            .with(table_config)
           .to_string();
        println!("{table}");
    }

    Ok(())
}