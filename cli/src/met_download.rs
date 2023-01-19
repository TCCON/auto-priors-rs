use std::collections::HashMap;
use std::io::{self, Write};
use std::process::{Command, Termination};

use anyhow::Context;
use clap::{self, Args};
use chrono::{NaiveDate, Duration};
use log::{warn, info, debug};
use orm::{self, geos::GeosDayState};

use crate::utils;


/// Check that a user-provided end date is after the start date OR set the default end date
fn check_start_end_date(start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<NaiveDate> {
    if let Some(ed) = end_date {
        if ed <= start_date {
            return Err(anyhow::Error::msg("end_date must be at least one day after the start_date"))
        }else{
            return Ok(ed)
        }

    }else{
        return Ok(start_date + Duration::days(1))
    };
}

/// Get start and end dates with defaults if values not provided.
/// 
/// Defaults follow:
/// 
/// 1. If no start date provided, first try to find the last day which has a complete set of
///    met data and try downloading the next day
/// 2. If no full days of existing data found, use the "earliest_date" keys in the met section
///    of the config file (specifically returning the earliest across all the types of met data)
/// 3. If no end date provided, use today as the end date.
/// 
/// Note that unlike [`check_start_end_date`] this does NOT verify that the end date is after the 
/// start date; it assumes that if that's the case, whatever iteration over days you have will just
/// do nothing.
/// 
/// # Returns
/// The start and end dates, either from the input or using the defaults described above. Returns an 
/// `Err` if:
/// 
/// * the `met_key` is not a valid section in the config, or
/// * any of the database queries fail, or
/// * no start date is provided and there are (somehow) no met sections to get the default start date from.
async fn get_start_end_dates(
    conn: &mut orm::MySqlConn,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    met_key: &str) -> anyhow::Result<(NaiveDate, NaiveDate)>
{
    let cfgs = config.get_met_configs(met_key)?;

    let start_date = if let Some(start) = start_date {
        start
    }else{
        // If no start date, use the first day we don't have met data for. If no data previously downloaded, use the
        // start date defined for the meteorology. If no meteorology datasets defined, return an error.
        let x = orm::geos::GeosFile::get_last_complete_date_for_config_set(conn, cfgs).await?;
        let y = if let Some(d) = x {
            Some(d + Duration::days(1))
        }else{
            info!("Found no complete days, starting from the beginning of the meteorology");
            config.get_met_start_date(met_key)?
        };

        y.ok_or_else(|| anyhow::Error::msg(format!(
            "Could not infer start date: no existing met data for key '{met_key}' and no configured met dataset to determine the default start from"
        )))?
    };

    let end_date = if let Some(end) = end_date {
        end
    }else {
        // Assume we use today
        chrono::offset::Utc::now().naive_utc().date()
    };

    Ok((start_date, end_date))
}

/// Check whether the required model files are present for a range of dates.
/// 
/// This will print one line per date to stdout. Each line will have the date
/// (in YYYY-MM-DD format), a colon, then the status "missing" (no model files
/// available for that day), "incomplete" (some model files available for that 
/// day, but at least one is missing), "complete" (the expected number of model
/// files is available for that day), or "UNKNOWN" (an error occurred while 
/// querying for that day).
#[derive(Debug, Args)]
pub struct CheckDatesCli {
    /// The first date to check, in YYYY-MM-DD format
    pub start_date: NaiveDate,
    /// The day AFTER the last date to check, if omitted, only START_DATE is checked
    pub end_date: Option<NaiveDate>,
    /// Which GEOS product stream to look for.
    #[clap(short = 'p', long, default_value = "fpit")]
    pub geos_product: orm::geos::GeosProduct,
    /// Which set of vertical levels to look for the 3D met fields on - "eta" = hybrid model levels, "pres" = fixed pressure levels
    #[clap(short = 'l', long, default_value = "eta")]
    pub met_levels: orm::geos::GeosLevels,
    /// Pass this to only require the 2D and 3D met files be present for a day to be complete. By default, the chemistry files must also be present.
    #[clap(short = 'c', long)]
    pub no_req_chm: bool
}

pub enum CheckFileStatus {
    Ok,
    Missing,
    Error,
    ErrorAndMissing
}

impl CheckFileStatus {
    fn new(any_missing: bool, error_occurred: bool) -> Self {
        if any_missing && error_occurred {
            Self::ErrorAndMissing
        }else if error_occurred {
            Self::Error
        }else if any_missing {
            Self::Missing
        }else{
            Self::Ok
        }
    }
}

impl Termination for CheckFileStatus {
    fn report(self) -> std::process::ExitCode {
        // avoid exit code of 1 because that usually means a crash error in the program
        match self {
            CheckFileStatus::Ok => std::process::ExitCode::from(0),
            CheckFileStatus::Missing => std::process::ExitCode::from(2),
            CheckFileStatus::Error => std::process::ExitCode::from(4),
            CheckFileStatus::ErrorAndMissing => std::process::ExitCode::from(6),
        }
    }
}

/// Command-line interface to check whether met files are already downloaded for a range of dates
/// 
/// This will print out to the command line the state (missing, incomplete, complete, or unknown/error) 
/// for each date in the range.
/// 
/// See [`check_files_for_dates`] for a function to use within the Rust code.
pub async fn check_files_for_dates_cli(conn: &mut orm::MySqlConn, clargs: CheckDatesCli) -> anyhow::Result<CheckFileStatus> {
    
    let files_found = check_files_for_dates(
        conn,
        clargs.start_date,
        clargs.end_date,
        clargs.geos_product,
        clargs.met_levels,
        !clargs.no_req_chm).await?;

    // Print the results out in chronological order
    let mut any_missing = false;
    let mut error_occurred = false;
    let mut dates: Vec<&NaiveDate> = files_found.keys().collect();
    dates.sort_unstable();
    for date in dates {
        if let Some(v) = files_found.get(date) {
            // Since we're iterating over the keys of the map, we should always be inside here
            let s = if let Some(state) = v {
                match state {
                    GeosDayState::Incomplete | GeosDayState::Missing => any_missing = true,
                    GeosDayState::Complete => {}
                }

                state.as_ref()
            }else{
                error_occurred = true;
                "UNKNOWN (errored during check)"
            };

            println!("{date}: {s}");
        }
    }

    Ok(CheckFileStatus::new(any_missing, error_occurred))
}

/// Get a map of which dates in a range have all their met data
/// 
/// # Inputs
/// * `conn` - connection to the SQL database
/// * `start_date` - first date to check
/// * `end_date` - last date (exclusive) to check, `None` will default to one day after `start_date`
/// * `geos_product` - which met product to check
/// * `met_levels` - which set of vertical levels to check
/// * `req_chm` - whether to require the chemistry files for the day to be complete.
/// 
/// # Returns
/// A HashMap with the dates as keys and a day status as values. A `None` indicates that an error occurred
/// while trying to check that date. Returns an `Err` if the end date is not after the start date.
pub async fn check_files_for_dates(
    conn: &mut orm::MySqlConn,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    geos_product: orm::geos::GeosProduct,
    met_levels: orm::geos::GeosLevels,
    req_chm: bool) -> anyhow::Result<HashMap<NaiveDate, Option<orm::geos::GeosDayState>>> 
{
    // Verify input dates are valid
    let end_date = check_start_end_date(start_date, end_date)?;

    // For each date, try to check if the necessary files are present. If we get an error, log it,
    // but keep going.
    let mut files_map = HashMap::new();
    let mut curr_date = start_date;
    while curr_date < end_date {
        let files_found = match orm::geos::GeosFile::is_date_complete(conn, curr_date, met_levels, geos_product, req_chm).await {
            Ok(state) => Some(state),
            Err(e) => {
                warn!("Error checking met files for date {curr_date}: {e}");
                None
            }
        };
        files_map.insert(curr_date, files_found);
        curr_date += Duration::days(1);
    }
    
    Ok(files_map)
}

/// Download meteorological reanalysis files for a range of dates [alias: drbd]
#[derive(Debug, Args)]
pub struct DownloadDatesCli {
    /// The key used in your TOML configuration file to declare a meteorology type.
    /// If you have [[data.download.geosit]] for example, then the key would be "geosit".
    pub met_key: String,
    /// The first date to download data for, in yyyy-mm-dd format.
    pub start_date: NaiveDate,
    /// The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given
    /// the default is one day after start_date (i.e. just download for start_date).
    pub end_date: Option<NaiveDate>,
    /// Print what would be downloaded but do not download anything.
    #[clap(short='d', long="dry-run")]
    pub dry_run: bool
}


pub async fn download_files_for_dates_cli(
    conn: &mut orm::MySqlConn, 
    clargs: DownloadDatesCli, 
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
) -> Result<(), anyhow::Error> 
{
    download_files_for_dates(
        conn,
        &clargs.met_key, 
        clargs.start_date, 
        clargs.end_date, 
        config,
        downloader,
        clargs.dry_run
    ).await
}

/// Download missing files for a given meteorological reanalysis [alias: dmr]
#[derive(Debug, Args)]
pub struct DownloadMissingCli {
    /// The key used in your TOML configuration file to declare a meteorology type.
    /// If you have [[data.download.geosit]] for example, then the key would be "geosit".
    pub met_key: String,

    /// The first date to download data for, in yyyy-mm-dd format. If not given, it will default
    /// to the most recent day that has all the expected met data for the given met_key. If no 
    /// complete days are present, it will use the earliest "earliest_date" value in the TOML 
    /// download sections for this met_key.
    #[clap(short = 's', long="start-date")]
    pub start_date: Option<NaiveDate>,

    /// The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given, it
    /// defaults to today (and so will try to download met data through yesterday).
    #[clap(short = 'e', long="end-date")]
    pub end_date: Option<NaiveDate>,

    /// Set this flag to print what would be downloaded, but not actually download anything.
    #[clap(short = 'd', long="dry-run")]
    pub dry_run: bool
}

pub async fn download_missing_files_cli(
    conn: &mut orm::MySqlConn, 
    clargs: DownloadMissingCli,
    config: &orm::config::Config, 
    downloader: impl utils::Downloader + Clone
) -> Result<(), anyhow::Error> {
    download_missing_files(
        conn,
        &clargs.met_key,
        clargs.start_date,
        clargs.end_date,
        config,
        downloader,
        clargs.dry_run
    ).await
}

pub async fn download_missing_files(
    conn: &mut orm::MySqlConn,
    met_key: &str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
    dry_run: bool) -> Result<(), anyhow::Error> 
{
    let cfgs = config.get_met_configs(met_key)?;
    let (start_date, end_date) = get_start_end_dates(conn, start_date, end_date, config, met_key).await?;

    // Now the main function: loop through each date and met type, download that met type if needed
    let mut curr_date = start_date;
    while curr_date < end_date {
        for cfg in cfgs {
            match orm::geos::GeosFile::is_date_complete_for_config(conn, curr_date, cfg).await? {
                GeosDayState::Complete => {
                    info!("{curr_date} already downloaded for {cfg}, not redownloading")
                },
                GeosDayState::Incomplete | GeosDayState::Missing => {
                    info!("{curr_date} must be downloaded for {cfg}");
                    download_one_file_one_date(conn, curr_date, cfg, &config.data, downloader.clone(), dry_run).await?;
                }
            }
        }

        curr_date += Duration::days(1);
    }

    Ok(())
}

/// Rescan the directories with met files and add any new files found to the database
#[derive(Debug, Args)]
pub struct RescanMetCli {
    /// The key used in your TOML configuration file to declare a meteorology type.
    /// If you have [[data.download.geosit]] for example, then the key would be "geosit".
    pub met_key: String,

    /// The first date to download data for, in yyyy-mm-dd format. If not given, it will default
    /// to the most recent day that has all the expected met data for the given met_key. If no 
    /// complete days are present, it will use the earliest "earliest_date" value in the TOML 
    /// download sections for this met_key.
    #[clap(short = 's', long="start-date")]
    pub start_date: Option<NaiveDate>,

    /// The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given, it 
    /// defaults to today (and so will try to download met data through yesterday).
    #[clap(short = 'e', long="end-date")]
    pub end_date: Option<NaiveDate>,

    /// Set this flag to print what would be downloaded, but not actually download anything.
    #[clap(short = 'd', long="dry-run")]
    pub dry_run: bool
}

pub async fn rescan_met_files_cli(conn: &mut orm::MySqlConn, clargs: RescanMetCli, config: &orm::config::Config) -> anyhow::Result<()> {
    let n = rescan_met_files(
        conn,
        clargs.start_date,
        clargs.end_date,
        config,
        &clargs.met_key,
        clargs.dry_run
    ).await?;

    info!("{n} new met files added to the database.");
    Ok(())
}

pub async fn rescan_met_files(
    conn: &mut orm::MySqlConn, 
    start_date: Option<NaiveDate>, 
    end_date: Option<NaiveDate>, 
    config: &orm::config::Config, 
    met_key: &str,
    dry_run: bool) -> anyhow::Result<u64>
{
    let (start_date, end_date) = get_start_end_dates(conn, start_date, end_date, config, met_key).await?;
    let mut curr_date = start_date;
    let download_cfgs = config.get_met_configs(met_key)?;

    let mut n_added = 0;

    while curr_date < end_date {
        info!("Scanning for new met files on {curr_date}");

        for cfg in download_cfgs {
            for file in cfg.expected_files_on_day(curr_date, &config.data)? {
                match orm::geos::GeosFile::file_exists_by_type(conn, &file, cfg).await {
                    Ok(true) => {
                        debug!("{} [{}] already in database", file.display(), cfg);
                    },
                    Ok(false) => {
                        if !dry_run {
                            n_added += orm::geos::GeosFile::add_geos_file_infer_date(conn, &file, cfg)
                            .await
                            .and(Ok(1))
                            .unwrap_or_else(|e| {warn!("Error adding {} to the database: {}", file.display(), e); 0});
                        } else {
                            println!("Would add {} [{}]", file.display(), cfg);
                            n_added += 1;
                        }
                    },
                    Err(e) => {
                        warn!("Error checking if {} is in the database: {}", file.display(), e);
                    }
                }
            }
        }
        curr_date += Duration::days(1);
    }
    Ok(n_added)
}

pub async fn download_files_for_dates(
    conn: &mut orm::MySqlConn,
    met_key: &str, 
    start_date: NaiveDate, 
    end_date: Option<NaiveDate>, 
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
    dry_run: bool) -> Result<(), anyhow::Error> 
{
    // First check that the dates are valid
    let end_date = check_start_end_date(start_date, end_date)?;

    // Then check that the requested met was defined in the configuration
    let met_cfg = config.get_met_configs(met_key)?;

    let mut curr_date = start_date;
    while curr_date < end_date {
        for file_cfg in met_cfg {
            download_one_file_one_date(
                conn,
                curr_date, 
                file_cfg, 
                &config.data,
                downloader.clone(),
                dry_run
            ).await?;
        }

        curr_date += Duration::days(1);
    }

    Ok(())
}


async fn download_one_file_one_date(
    conn: &mut orm::MySqlConn,
    date: NaiveDate, 
    file_cfg: &orm::config::DownloadConfig, 
    data_cfg: &orm::config::DataConfig, 
    downloader: impl utils::Downloader,
    dry_run: bool) -> Result<(), anyhow::Error>
    
{
    let save_dir = file_cfg.get_save_dir(data_cfg)?;
    
    let mut out: Box<dyn Write> = if dry_run {
        Box::new(io::stdout())
    }else{
        let wget_list = save_dir.join("wget_list.txt");
        Box::new(std::fs::File::create(wget_list)?)
    };

    if dry_run {
        println!("Would download the following URLs for {date} to {}", save_dir.display());
    }

    let mut expected_met_files = vec![];
    let basename_pat = file_cfg.get_basename_pattern()?;
    for file_time in file_cfg.times_on_day(date) {
        // `out` will be stdout in the case of a dry run; this allows us to print the exact URLs that
        // would be downloaded
        writeln!(out, "{}", file_time.format(&file_cfg.url_pattern))
            .with_context(|| "Unable to write out download URL")?;

        let base_name = file_time.format(basename_pat).to_string();
        expected_met_files.push((file_time, save_dir.join(base_name)));
    }

    if !dry_run {
        Command::new("wget")
            .args(["-i", "wget_list.txt"])
            .current_dir(&save_dir)
            .spawn()
            .with_context(|| format!("wget command to download {} in {} failed", date, save_dir.display()))?;

        for (file_time, file_path) in expected_met_files {
            if file_path.exists() {
                orm::geos::GeosFile::add_geos_file(conn, &file_path, file_time, file_cfg).await?;
            }
        }
    }else{
        println!("");
    }

    Ok(())
}

pub fn test_me() -> i32 {
    42
}