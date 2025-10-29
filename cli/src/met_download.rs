use std::collections::HashMap;
use std::process::Termination;

use anyhow::Context;
use chrono::{Duration, NaiveDate};
use clap::{self, Args, Subcommand};
use itertools::Itertools;
use log::{debug, error, info, warn};
use orm::{
    self,
    met::{AddMetFileError, MetDayState, MetFile},
    utils::DateIterator,
};
use sqlx::Connection;

use crate::utils::{self, DownloadError};

/// Manage meteorology downloads and database
#[derive(Debug, Args)]
pub struct MetCli {
    #[clap(subcommand)]
    pub command: MetActions,
}

#[derive(Debug, Subcommand)]
pub enum MetActions {
    /// Check whether the required model files are listed in the database for
    /// a range of dates
    Check(CheckDatesCli),

    /// Download model files for a range of dates
    DownloadDates(DownloadDatesCli),

    /// Download missing model files
    DownloadMissing(DownloadMissingCli),

    /// Delete (and possibly redownload) met files
    RemoveDates(RemoveDatesCli),

    /// Report on the currently downloaded default met files
    Report(ReportMetCli),

    /// Print a summary table of available met data for a given date range
    Table(MetTableCli),

    /// Rescan model download directories and add new files to the database
    Rescan(RescanMetCli),
}

/// Check that a user-provided end date is after the start date OR set the default end date
fn check_start_end_date(
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<NaiveDate> {
    if let Some(ed) = end_date {
        if ed <= start_date {
            return Err(anyhow::Error::msg(
                "end_date must be at least one day after the start_date",
            ));
        } else {
            return Ok(ed);
        }
    } else {
        return Ok(start_date + Duration::days(1));
    };
}

/// Returns a [`utils::DateIterator`] that iterates over days to try downloading met data for
///
/// If `met_key` is `None`, then this calls [`get_date_iter_for_defaults`]. This is the normal
/// use; that figures out what dates we need to download to have all the met files for the default
/// prior generation up to today. If `met_key` is `Some(_)`, then this calls [`get_date_iter_for_specified_met`].
/// That is intended for special cases where a single met type needs downloaded, potentially even
/// for periods when it would not normally be downloaded based on the defaults.
///
/// For information on how the inputs map to the output iterator, see the documentation for
/// [`get_date_iter_for_defaults`] and [`get_date_iter_for_specified_met`].
pub async fn get_date_iter(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    met_key: Option<&str>,
    ignore_defaults: bool,
) -> anyhow::Result<orm::utils::DateIterator> {
    if let Some(key) = met_key {
        debug!("Setting up date iteration for met {key} as specified");
        get_date_iter_for_specified_met(conn, start_date, end_date, config, key, ignore_defaults)
            .await
    } else {
        debug!("Setting up date iteration for default mets in config");
        get_date_iter_for_defaults(conn, start_date, end_date, config).await
    }
}

/// Get a date iterator describing the period to try to download met data for.
///
/// This function should usually be called through [`get_date_iter`]. This should be more
/// commonly used than [`get_date_iter_for_defaults`], as this one accounts for how the
/// default meteorlogy can change for different time periods. The time period it returns
/// depends on the inputs:
///
/// 1. If no start date provided, it tries to find the last day which has a complete set of
///    met data (based on the defaults defined in the config) and starts with the next day
/// 2. If no full days of existing data found, it takes the list of default option sets from
///    the config, gets the chronologically first set of defaults, and looks at the met type
///    used in that set. It takes the latest of the "earliest available" dates across all the
///    files for that met type as the start date.
/// 3. If no end date provided, use today as the end date.
///
/// Note that unlike [`check_start_end_date`] this does NOT verify that the end date is after the
/// start date; it assumes that if that's the case, whatever iteration over days you have will just
/// do nothing.
///
/// # Returns
/// An iterator over all the dates to check for met, either from the input or using the defaults described above.
/// Returns an `Err` if:
///
/// * the met key from the first defaults set is not a valid met section in the config,
/// * any of the database queries fail,
/// * no start date is provided and there are (somehow) no met sections or default option sets to get
///   the default start date from, or
/// * one of more of the default option sets overlap in time
async fn get_date_iter_for_defaults(
    conn: &mut orm::MySqlConn,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
) -> anyhow::Result<orm::utils::DateIterator> {
    let start_date = if let Some(start) = start_date {
        start
    } else if let Some(d) =
        orm::met::MetFile::get_last_complete_date_for_default_processing(conn, config).await?
    {
        // If no start date, use the first day we don't have met data for. If no data previously downloaded, use the
        // start date defined for the meteorology. If no meteorology datasets defined, return an error.
        d + Duration::days(1)
    } else {
        info!("Found no complete days, starting from the beginning of the meteorology");
        let defaults = config.get_all_defaults_check_overlap()?;
        let first_defaults = defaults
            .first()
            .ok_or_else(|| anyhow::Error::msg("No default option sets configured"))?;

        // Take the later of the earliest date for which this meteorology is available and the start of the
        // default set - assuming that if the default set starts after the met, then we don't need the full
        // met record.
        let first_met_configs =
            config.get_mets_for_processing_config(&first_defaults.processing_configuration)?;
        let met_start_date = first_met_configs
            .iter()
            .map(|c| c.cfg.earliest_date)
            .max()
            .expect("Should have been at least one file configured for the first meteorology"); // get_met_configs should have errored if 0 met files defined

        let defaults_start_date = first_defaults.start_date.unwrap_or(met_start_date);
        if defaults_start_date > met_start_date {
            defaults_start_date
        } else {
            met_start_date
        }
    };

    let end_date = if let Some(end) = end_date {
        end
    } else {
        // Assume we use today
        let end = chrono::offset::Utc::now().naive_utc().date();
        debug!("No end date specified, using {end}");
        end
    };

    Ok(orm::utils::DateIterator::new(vec![(start_date, end_date)]))
}

/// This function provides a date iterator for days relevant to a single met type.
///
/// This function is usually only called through [`get_date_iter`]. The uses of this
/// function should be less common than [`get_date_iter_for_defaults`], this function
/// is useful in two cases:
///
/// 1. we need to fill in a single (perhaps new) set of met data, or
/// 2. we want to fill in met data over a period which it isn't typically used (according to the defaults)
///
/// What dates the iterator contains is very flexible:
///
/// * If both `start_date` and `end_date` are `Some(_)`, then the returned iterator will only contain
///   dates between them (with `end_date` being exclusive as normal).
/// * If `end_date` is `None`, then today is used.
/// * If `start_date` is None, then this function first looks in the database to see if there is already
///   some data for this met type. If so, the day after the last complete day is treated as the start date.
///   If there are no files for this met type in the database, then it checks the configuration to see if the
///   requested met type defines its earliest available date on any of the files needed. If so, the latest of
///   those dates is treated as the start date. If even the configuration does not define a start date, then it
///   checks the chronologically first default option set that uses the given met type. If that does not define
///   a start date, this returns an `Err`.
/// * When `ignore_defaults` is `false`, the iterator will only include dates for which the specified met type
///   is the default as given in the configuration.
/// * When `ignore_defaults` is `true`, the iterator will include all dates between the provided/calculated
///   start and end dates.
///
/// # Returns
/// A [`utils::DateIterator`] for the calculated dates. This returns as `Err` if:
///
/// * `met_key` is not found in the configuration,
/// * the configuration's default option sets overlap in time,
/// * the database query to check for the latest complete day fails, or
/// * the start date could not be determined, as described above.
async fn get_date_iter_for_specified_met(
    conn: &mut orm::MySqlConn,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    met_key: &str,
    ignore_defaults: bool,
) -> anyhow::Result<orm::utils::DateIterator> {
    let dl_cfgs = config.get_met_configs(met_key)?;
    let default_options = config.get_all_defaults_check_overlap()?;

    let start_date = if let Some(d) = start_date {
        // the user provided start date takes precedence
        debug!("Setting start date to {d} from command line");
        Some(d)
    } else if let Some(d) =
        orm::met::MetFile::get_first_or_last_complete_date_for_config_set(conn, dl_cfgs, false)
            .await?
    {
        // if that's not available, assume we want to start with the day after the last date for which we have this met
        // data for
        let start = d + Duration::days(1);
        debug!("Setting start date to {start} given the last complete date for {met_key} was {d}");
        Some(start)
    } else if let Some(d) = dl_cfgs.iter().map(|c| c.earliest_date).max() {
        // if there is no existing met data, take the latest date after which all the files needed for this met are available
        // this should never really *not* have a max value, because if there's an entry for the met in the download HashMap,
        // it really should have at least one entry in the TOML file. But it's possible someone could write the file like
        // `data.download.geosfpit = []`, so we'll be careful here.
        debug!("Setting start date to {d} based on the configured earliest available dates for {met_key}");
        Some(d)
    } else {
        warn!("Could not determine starting date for {met_key}");
        None
    };

    let end_date = if let Some(end) = end_date {
        // the user provided end date takes precedence
        debug!("Setting end date to {end} from the command line");
        end
    } else {
        // otherwise we use today
        debug!("Setting end date to today as no end date specified");
        chrono::offset::Utc::now().naive_utc().date()
    };

    // This branch is if we want to get the date range between the start and end date provided as a single
    // contiguous range. As long as there was a clear starting date, this is simple. If not, then we will
    // try to guess the start date from the default options. If those don't define one, then we can't deduce the starting date.
    if ignore_defaults {
        if let Some(d) = start_date {
            debug!("Will cover dates {d} to {end_date}");
            return Ok(orm::utils::DateIterator::new(vec![(d, end_date)]));
        } else {
            // This case should be EXTREMELY rare for the reasons in the last else if branch for start_date above
            let defaults_start = default_options.iter()
                .filter(|opts| opts.met == met_key)
                .map(|opts| opts.start_date)
                .next()
                .flatten()
                .ok_or_else(|| anyhow::Error::msg(
                    format!("No earliest date defined for met = {met_key} and either no default option sets reference this met or the first one to do so has no start date defined.")
                ))?;

            debug!("Will cover dates {defaults_start} to {end_date} based on configured earliest dates for {met_key}");
            return Ok(orm::utils::DateIterator::new(vec![(
                defaults_start,
                end_date,
            )]));
        }
    }

    // If we're here, then we interpret the request to be for all dates, potentially limited to between given or
    // inferred start and end dates, for which the default option sets say we should use this met.
    let mut date_ranges: Vec<(NaiveDate, NaiveDate)> = vec![];
    for opts in default_options {
        if opts.met != met_key {
            continue;
        }

        let this_start = opts.start_date.or(start_date)
            .ok_or_else(|| anyhow::Error::msg(format!(
                "A default option set for met = {met_key} has no start date defined and that met has no earliest date defined."
            )))?;
        let this_end = opts.end_date.unwrap_or(end_date);
        debug!("Adding date range {this_start} to {this_end} to iterate over");
        date_ranges.push((this_start, this_end))
    }

    if date_ranges.is_empty() {
        warn!("Will not iterate over any dates; if trying to operate on a date range for which {met_key} is not the default, you may need to pass an extra flag to ignore the configured default met.");
    }

    // Still use bounds on the iterator even though we do use the defined start and end dates elsewhere. This
    // is needed in case all the default option sets define start and/or end dates; we still need to cut down
    // the iterator to the desired limits.

    Ok(orm::utils::DateIterator::new_with_bounds(
        date_ranges,
        start_date,
        Some(end_date),
    ))
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
    /// The key identifying the section in the configuration file to use
    /// for the set of met files required. In a configuration file with
    /// sections "[[data.download.geosfpit]]", the key would be "geosfpit".
    /// If not given, the default met(s) are checked.
    #[clap(short = 'm', long = "met")]
    pub met_key: Option<String>,
    /// The first date to check, in YYYY-MM-DD format
    pub start_date: NaiveDate,
    /// The day AFTER the last date to check, if omitted, only START_DATE is checked
    pub end_date: Option<NaiveDate>,
}

pub enum CheckFileStatus {
    Ok,
    Missing,
    Error,
    ErrorAndMissing,
}

impl CheckFileStatus {
    fn new(any_missing: bool, error_occurred: bool) -> Self {
        if any_missing && error_occurred {
            Self::ErrorAndMissing
        } else if error_occurred {
            Self::Error
        } else if any_missing {
            Self::Missing
        } else {
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
pub async fn check_files_for_dates_cli(
    conn: &mut orm::MySqlConn,
    clargs: CheckDatesCli,
    cfg: &orm::config::Config,
) -> anyhow::Result<CheckFileStatus> {
    let files_found = if let Some(met_key) = &clargs.met_key {
        check_one_config_set_files_for_dates(
            conn,
            cfg,
            &met_key,
            clargs.start_date,
            clargs.end_date,
        )
        .await?
    } else {
        check_default_files_for_dates(conn, cfg, clargs.start_date, clargs.end_date).await?
    };

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
                    MetDayState::Incomplete(_, _) | MetDayState::Missing => any_missing = true,
                    MetDayState::Complete => {}
                }

                state.as_ref()
            } else {
                error_occurred = true;
                "UNKNOWN (errored during check)"
            };

            println!("{date}: {s}");
        }
    }

    Ok(CheckFileStatus::new(any_missing, error_occurred))
}

/// Print out a report of the downloaded default met files
#[derive(Debug, Args)]
pub struct ReportMetCli {
    /// List each date missing, incomplete, or that errored in addition to the summary
    #[clap(short = 'd', long)]
    detailed: bool,
}

pub async fn report_default_met_status_cli(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    args: ReportMetCli,
) -> anyhow::Result<()> {
    report_default_met_status(conn, config, args.detailed).await
}

pub async fn report_default_met_status(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    detailed: bool,
) -> anyhow::Result<()> {
    let (start_date, end_date) = config.get_default_met_date_range();
    let start_date = if let Some(sd) = start_date {
        sd
    } else {
        if let Some(sd) =
            orm::met::MetFile::get_first_complete_day_for_default_processing(conn, config).await?
        {
            println!("No start date defined for default mets in config; will report on met data starting from first complete date.");
            sd
        } else {
            println!("No met default met data recorded in the database yet.");
            return Ok(());
        }
    };

    // Summarize the expected met types for each date range
    println!("Default mets:");
    for default_set in config.default_options.iter() {
        let start = default_set
            .start_date
            .map(|d| d.to_string())
            .unwrap_or_else(|| "(no start date)".to_string());
        let end = default_set
            .end_date
            .map(|d| d.to_string())
            .unwrap_or_else(|| "(no end date)".to_string());
        let met_key = &default_set.met;
        println!(" - {start} to {end}: {met_key}");
    }

    let files_found = check_default_files_for_dates(conn, config, start_date, end_date).await?;
    // ensure the dates are ordered
    let mut dates: Vec<_> = files_found.keys().copied().collect();
    dates.sort_unstable();
    let mut nmissing = 0;
    let mut nincomplete = 0;
    let mut ncomplete = 0;
    let mut nerrored = 0;
    let ntotal = files_found.len();

    for date in dates {
        // we know this is a key in the map, so safe to unwrap
        let state = *files_found.get(&date).unwrap();
        match state {
            Some(MetDayState::Complete) => ncomplete += 1,
            Some(MetDayState::Incomplete(_, _)) => {
                nincomplete += 1;
                if detailed {
                    println!("{date} = INCOMPLETE");
                }
            }
            Some(MetDayState::Missing) => {
                nmissing += 1;
                if detailed {
                    println!("{date} = MISSING");
                }
            }
            None => {
                nerrored += 1;
                if detailed {
                    println!("{date} = ERROR GETTING STATE");
                }
            }
        }
    }

    if detailed {
        println!("");
    }
    println!("{ncomplete}/{ntotal} days default met COMPLETE");
    println!("{nincomplete}/{ntotal} days default met INCOMPLETE");
    println!("{nmissing}/{ntotal} days default met MISSING");
    println!("{nerrored}/{ntotal} days default met ERRORED GETTING STATE");

    Ok(())
}

/// Print a summary table of available met data for a time range
#[derive(Debug, Args)]
pub struct MetTableCli {
    /// Pass this flag to print information about all met data for the given date range, not just the defaults.
    #[clap(short = 'a', long)]
    all_mets: bool,

    /// The first date to show in the table. If not given, defaults to 7 days ago.
    #[clap(short = 's', long)]
    start_date: Option<NaiveDate>,

    /// The date after the last one to show in the table. If not given, defaults to today.
    #[clap(short = 'e', long)]
    end_date: Option<NaiveDate>,
}

pub async fn print_met_availability_table_cli(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    args: MetTableCli,
) -> anyhow::Result<()> {
    let start_date = if let Some(sd) = args.start_date {
        sd
    } else {
        chrono::Utc::now().date_naive() - chrono::Duration::days(7)
    };

    if args.all_mets {
        print_met_availability_table_all_mets(conn, config, start_date, args.end_date).await
    } else {
        print_met_availability_table(conn, config, start_date, args.end_date).await
    }
}

pub async fn print_met_availability_table(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<()> {
    let end_date = end_date.unwrap_or_else(|| chrono::Utc::now().date_naive());

    // First get all the unique met file types and the values for each row
    let mut met_types = vec![];
    let mut rows = vec![];
    let mut dates = vec![];
    for date in orm::utils::DateIterator::new_one_range(start_date, end_date) {
        let defaults = config.get_defaults_for_date(date)?;
        let dl_cfgs = config.get_met_configs(&defaults.met)?;
        let mut row = HashMap::new();
        for dl_cfg in dl_cfgs {
            let cfg_string = dl_cfg.to_short_string();
            if !met_types.contains(&cfg_string) {
                met_types.push(cfg_string.clone());
            }
            let state = orm::met::MetFile::is_date_complete_for_config(conn, date, dl_cfg).await?;
            row.insert(cfg_string, state);
        }
        rows.push(row);
        dates.push(date);
    }

    // Now we can build the table
    print_met_table_inner(rows, dates, met_types, "N/D")
}

pub async fn print_met_availability_table_all_mets(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<()> {
    let end_date = end_date.unwrap_or_else(|| chrono::Utc::now().date_naive());

    // First get the list of all mets defined in the config
    let met_keys = config
        .data
        .met_download
        .keys()
        .map(|k| k.to_string())
        .collect_vec();

    // Now we can loop through the dates and mets to check if complete
    let mut rows = vec![];
    let mut dates = vec![];
    let mut met_types = vec![];

    let mut first_date = true;
    for date in orm::utils::DateIterator::new_one_range(start_date, end_date) {
        let mut row = HashMap::new();
        for key in met_keys.iter() {
            let dl_cfgs = config.get_met_configs(&key)?;
            for dl_cfg in dl_cfgs {
                let state =
                    orm::met::MetFile::is_date_complete_for_config(conn, date, dl_cfg).await?;
                row.insert(dl_cfg.to_short_string(), state);

                if first_date {
                    met_types.push(dl_cfg.to_short_string());
                }
            }
        }
        rows.push(row);
        dates.push(date);
        first_date = false;
    }

    print_met_table_inner(rows, dates, met_types, "ERROR")
}

fn print_met_table_inner(
    rows: Vec<HashMap<String, MetDayState>>,
    dates: Vec<NaiveDate>,
    met_types: Vec<String>,
    fill: &str,
) -> anyhow::Result<()> {
    let row_iter = rows.iter().zip(dates.iter()).map(|(r, d)| {
        let mut states = met_types
            .iter()
            .map(|k| {
                r.get(k)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| fill.to_string())
            })
            .collect::<Vec<_>>();
        states.insert(0, d.to_string());
        states
    });
    let mut builder = tabled::builder::Builder::from_iter(row_iter);
    let date_header = vec!["Date".to_string()];
    builder.set_header(date_header.into_iter().chain(met_types.into_iter()));

    let table = builder.build();
    println!("{}", orm::utils::table_to_std_string(table));

    Ok(())
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
pub async fn check_one_config_set_files_for_dates(
    conn: &mut orm::MySqlConn,
    cfg: &orm::config::Config,
    met_key: &str,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<HashMap<NaiveDate, Option<orm::met::MetDayState>>> {
    // Verify input dates are valid
    let end_date = check_start_end_date(start_date, end_date)?;

    // Go ahead and get the set of files expected to be downloaded according to the config
    let dl_configs = cfg.get_met_configs(met_key)?;

    // For each date, try to check if the necessary files are present. If we get an error, log it,
    // but keep going.
    let mut files_map = HashMap::new();
    let mut curr_date = start_date;
    while curr_date < end_date {
        let files_found =
            match orm::met::MetFile::is_date_complete_for_config_set(conn, curr_date, dl_configs)
                .await
            {
                Ok(state) => Some(state),
                Err(e) => {
                    warn!("Error checking met files for date {curr_date}: {e:?}");
                    None
                }
            };
        files_map.insert(curr_date, files_found);
        curr_date += Duration::days(1);
    }

    Ok(files_map)
}

pub async fn check_default_files_for_dates(
    conn: &mut orm::MySqlConn,
    cfg: &orm::config::Config,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<HashMap<NaiveDate, Option<orm::met::MetDayState>>> {
    let end_date = check_start_end_date(start_date, end_date)?;
    let mut files_map = HashMap::new();

    // TODO: add an indicatif progress bar if running in a tty
    for date in DateIterator::new_one_range(start_date, end_date) {
        let default_config = cfg.get_defaults_for_date(date)?;
        let dl_configs = cfg.get_met_configs(&default_config.met)?;
        let files_found = match orm::met::MetFile::is_date_complete_for_config_set(
            conn, date, dl_configs,
        )
        .await
        {
            Ok(state) => Some(state),
            Err(e) => {
                warn!("Error checking met files for date {date}: {e:?}");
                None
            }
        };
        files_map.insert(date, files_found);
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
    #[clap(short = 'd', long = "dry-run")]
    pub dry_run: bool,
}

pub async fn download_files_for_dates_cli(
    conn: &mut orm::MySqlConn,
    clargs: DownloadDatesCli,
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
) -> Result<(), anyhow::Error> {
    download_files_for_dates(
        conn,
        &clargs.met_key,
        clargs.start_date,
        clargs.end_date,
        config,
        downloader,
        clargs.dry_run,
    )
    .await
}

/// Download missing files for a given meteorological reanalysis [alias: dmr]
#[derive(Debug, Args)]
pub struct DownloadMissingCli {
    /// The first date to download data for, in yyyy-mm-dd format. If not given, it will default
    /// to the most recent day that has all the expected met data for the given met_key. If no
    /// complete days are present, it will use the earliest "earliest_date" value in the TOML
    /// download sections for this met_key.
    #[clap(short = 's', long = "start-date")]
    pub start_date: Option<NaiveDate>,

    /// The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given, it
    /// defaults to today (and so will try to download met data through yesterday).
    #[clap(short = 'e', long = "end-date")]
    pub end_date: Option<NaiveDate>,

    #[clap(short = 'm', long = "met")]
    pub met_key: Option<String>,

    #[clap(short = 'i', long)]
    pub ignore_defaults: bool,

    /// Set this flag to print what would be downloaded, but not actually download anything.
    #[clap(short = 'd', long = "dry-run")]
    pub dry_run: bool,
}

pub async fn download_missing_files_cli(
    conn: &mut orm::MySqlConn,
    clargs: DownloadMissingCli,
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
) -> Result<(), anyhow::Error> {
    download_missing_files(
        conn,
        clargs.start_date,
        clargs.end_date,
        clargs.met_key.as_deref(),
        clargs.ignore_defaults,
        config,
        downloader,
        clargs.dry_run,
    )
    .await
}

pub async fn download_missing_files(
    conn: &mut orm::MySqlConn,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    met_key: Option<&str>,
    ignore_defaults: bool,
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    let date_iter =
        get_date_iter(conn, config, start_date, end_date, met_key, ignore_defaults).await?;
    let mut missed_dates = vec![];

    // Now the main function: loop through each date and met type, download that met type if needed
    for curr_date in date_iter {
        debug!("Checking met files for {curr_date}");
        let dl_cfgs = if let Some(key) = met_key {
            config.get_met_configs(key)?
        } else {
            let defaults = config.get_defaults_for_date(curr_date)?;
            config.get_met_configs(&defaults.met)?
        };

        for dl_cfg in dl_cfgs {
            let res = match orm::met::MetFile::is_date_complete_for_config(conn, curr_date, dl_cfg)
                .await?
            {
                MetDayState::Complete => {
                    info!("{curr_date} already downloaded for {dl_cfg}, not redownloading");
                    Ok(())
                }
                MetDayState::Incomplete(_, _) | MetDayState::Missing => {
                    info!("{curr_date} must be downloaded for {dl_cfg}");
                    download_one_file_set_one_date(
                        conn,
                        curr_date,
                        dl_cfg,
                        downloader.clone(),
                        dry_run,
                    )
                    .await
                }
            };

            if let Err(DownloadError::FilesNotAvailable) = res {
                // If we didn't successfully download all the files, it's only an error if the given date is long enough ago
                // that the files should have been available.
                let first_optional_date = chrono::Utc::now().date_naive()
                    - chrono::Duration::days(dl_cfg.days_latency as i64);
                if curr_date >= first_optional_date {
                    warn!("Could not download {dl_cfg} for {curr_date}, but this may be due to latency (not expecting files from {first_optional_date} on");
                } else {
                    missed_dates.push(curr_date);
                }
            } else if let Err(e) = res {
                return Err(e.into());
            }
        }
    }

    if missed_dates.is_empty() {
        Ok(())
    } else {
        let n = missed_dates.len();
        let missed_dates = missed_dates.into_iter().map(|d| d.to_string()).join(", ");
        anyhow::bail!("Not all dates downloaded successfully, missed {n}: {missed_dates}")
    }
}

/// Rescan the directories with met files and add any new files found to the database.
///
/// Which dates are scanned depends on a number of things. If you specify --met, then the rules for start dates are:
/// - A start date given by --start-date takes precedence.
/// - If --start-date not given, fall back on the day after the last complete day for that met.
/// - If no complete days exist, fall back on the earliest date we expect this met product to be available
///   (based on the config).
/// - If the earliest date cannot be inferred from the met configuration, then the last fall back is to
///   look at the defaults configured and use the start date for the first default that references this met.
///
/// For end dates:
/// - An end date given by --end-date is preferred.
/// - If not given, then the end date is set to today.
#[derive(Debug, Args)]
pub struct RescanMetCli {
    /// The first date to check for data, in yyyy-mm-dd format. If not given, it will default
    /// to a sensible value, depending on the value of --met-key.
    #[clap(short = 's', long = "start-date")]
    pub start_date: Option<NaiveDate>,

    /// The last date (exclusive) to check for data, in yyyy-mm-dd format. If not given, it
    /// will default to a sensible value, depending on the value of --met-key.
    #[clap(short = 'e', long = "end-date")]
    pub end_date: Option<NaiveDate>,

    /// The key used in your TOML configuration file to declare a meteorology type.
    /// If you have [[data.download.geosit]] for example, then the key would be "geosit".
    #[clap(short = 'm', long = "met")]
    pub met_key: Option<String>,

    /// By default when you specify --met, this will rescan any dates given by the date range rules,
    /// regardless of whether that met type is configured as the default for those dates. Pass this
    /// flag to ignore dates when that met type is not configured as the default.
    #[clap(short = 'o', long)]
    pub obey_defaults: bool,

    /// Set this flag to print what would be added to the database, but not actually modify the database.
    #[clap(short = 'd', long = "dry-run")]
    pub dry_run: bool,
}

pub async fn rescan_met_files_cli(
    conn: &mut orm::MySqlConn,
    clargs: RescanMetCli,
    config: &orm::config::Config,
) -> anyhow::Result<()> {
    let n = rescan_met_files(
        conn,
        clargs.start_date,
        clargs.end_date,
        config,
        clargs.met_key.as_deref(),
        clargs.obey_defaults,
        clargs.dry_run,
    )
    .await?;

    info!("{n} new met files added to the database.");
    if n == 0 {
        info!("")
    }
    Ok(())
}

pub async fn rescan_met_files(
    conn: &mut orm::MySqlConn,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    met_key: Option<&str>,
    obey_defaults: bool,
    dry_run: bool,
) -> anyhow::Result<u64> {
    let date_iter =
        get_date_iter(conn, config, start_date, end_date, met_key, !obey_defaults).await?;

    let mut n_added = 0;
    let mut transaction = conn.begin().await?;

    for curr_date in date_iter {
        info!("Scanning for new met files on {curr_date}");
        let download_cfgs = if let Some(key) = met_key {
            config.get_met_configs(key)?
        } else {
            let defaults = match config.get_defaults_for_date(curr_date) {
                Ok(dl_cflgs) => dl_cflgs,
                Err(e) => {
                    warn!("{e}");
                    continue;
                }
            };
            config.get_met_configs(&defaults.met)?
        };

        for dl_cfg in download_cfgs {
            for file in dl_cfg.expected_files_on_day(curr_date)? {
                if !file.exists() {
                    continue;
                }

                match orm::met::MetFile::file_exists_by_type(&mut transaction, &file, dl_cfg).await
                {
                    Ok(true) => {
                        debug!("{} [{}] already in database", file.display(), dl_cfg);
                    }
                    Ok(false) => {
                        if !dry_run {
                            n_added += orm::met::MetFile::add_met_file_infer_date(
                                &mut transaction,
                                &file,
                                dl_cfg,
                            )
                            .await
                            .and(Ok(1))
                            .unwrap_or_else(|e| {
                                warn!("Error adding {} to the database: {}", file.display(), e);
                                0
                            });
                        } else {
                            println!("Would add {} [{}]", file.display(), dl_cfg);
                            n_added += 1;
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Error checking if {} is in the database: {e:?}",
                            file.display()
                        );
                    }
                }
            }
        }
    }

    transaction.commit().await?;
    Ok(n_added)
}

/// Delete already downloaded met files between two dates.
///
/// By default, all met files between START_DATE (inclusive) and END_DATE
/// (exclusive) will be deleted from the database and the file system. To only
/// delete a specific type of met data, use the --met-product option.
#[derive(Debug, Args)]
pub struct RemoveDatesCli {
    /// The first date to delete, in YYYY-MM-DD format.
    start_date: NaiveDate,
    /// The day after the last date to delete, in YYYY-MM-DD format.
    end_date: NaiveDate,
    /// If given, only delete met files for this product.
    #[clap(short = 'm', long)]
    met_product: Option<orm::met::MetProduct>,
    /// If given, print what would be done instead of actually deleting files.
    #[clap(short = 'd', long)]
    dry_run: bool,
    /// If given, this code will try to redownload the deleted files. This must be the met key
    /// from the configuration file to use to download.
    #[clap(short = 'r', long)]
    redownload: Option<String>,
}

pub async fn remove_dates_cli(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    args: RemoveDatesCli,
    downloader: impl utils::Downloader + Clone,
) -> anyhow::Result<()> {
    remove_dates(
        conn,
        config,
        downloader,
        args.start_date,
        args.end_date,
        args.met_product,
        args.dry_run,
        args.redownload.as_deref(),
    )
    .await
}

pub async fn remove_dates(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
    start_date: NaiveDate,
    end_date: NaiveDate,
    met_product: Option<orm::met::MetProduct>,
    dry_run: bool,
    redownload: Option<&str>,
) -> anyhow::Result<()> {
    // If we were given redownload, check that it is a valid met key before we proceed.
    if let Some(met_key) = redownload {
        config.get_met_configs(met_key).context(
            "The met key provided as the redownload argument was not valid for the given config.",
        )?;
    }

    let met_files = MetFile::get_files_by_dates(conn, start_date, end_date, met_product.as_ref())
        .await
        .context("Error occurred while listing met files to remove")?;

    let mut n_error_occurred = 0;
    let n_files = met_files.len();

    for file in met_files {
        if dry_run {
            println!(
                "Would delete MetFile ID = {}, {}",
                file.file_id,
                file.file_path.display()
            );
        } else {
            // This method has some info! calls so we don't need to print anything in the successful case.
            // We don't immediately return if an error occurs because we want to try the other files.
            file.delete_me(conn).await.unwrap_or_else(|e| {
                error!(
                    "Failed to delete met file ID = {} ({}), reason was {e:?}",
                    file.file_id,
                    file.file_path.display()
                );
                n_error_occurred += 1;
            });
        }
    }

    if n_error_occurred > 0 {
        let dlmsg = if redownload.is_some() {
            " (Redownloading will not be attempted if it was requested.)"
        } else {
            ""
        };
        anyhow::bail!("Failed to delete {n_error_occurred} of {n_files} met files.{dlmsg}");
    }

    if let Some(met_key) = redownload {
        download_files_for_dates(
            conn,
            met_key,
            start_date,
            Some(end_date),
            config,
            downloader,
            dry_run,
        )
        .await
        .context("Error occurred while redownloaded deleted met files")?;
    }

    Ok(())
}

pub async fn download_files_for_dates(
    conn: &mut orm::MySqlConn,
    met_key: &str,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    // First check that the dates are valid
    let end_date = check_start_end_date(start_date, end_date)?;

    // Then check that the requested met was defined in the configuration
    let met_cfg = config.get_met_configs(met_key)?;

    let mut curr_date = start_date;
    while curr_date < end_date {
        for file_cfg in met_cfg {
            download_one_file_set_one_date(conn, curr_date, file_cfg, downloader.clone(), dry_run)
                .await?;
        }

        curr_date += Duration::days(1);
    }

    Ok(())
}

async fn download_one_file_set_one_date(
    conn: &mut orm::MySqlConn,
    date: NaiveDate,
    file_cfg: &orm::config::MetDownloadConfig,
    mut downloader: impl utils::Downloader,
    dry_run: bool,
) -> Result<(), DownloadError> {
    let mut transaction = conn
        .begin()
        .await
        .context("Error occurred while obtaining the transaction ")?;
    let save_dir = &file_cfg.download_dir;

    if dry_run {
        println!(
            "Would download the following URLs for {date} to {}",
            save_dir.display()
        );
    }

    let mut expected_met_files = vec![];
    let basename_pat = file_cfg.get_basename_pattern()?;
    for file_time in file_cfg.times_on_day(date) {
        let file_url = file_time.format(&file_cfg.url_pattern).to_string();
        downloader
            .add_file_to_download(file_url)
            .with_context(|| "Unable to add URL to list of files to download")?;

        let base_name = file_time.format(basename_pat).to_string();
        expected_met_files.push((file_time, save_dir.join(base_name)));
    }

    if !dry_run {
        // It's possible that some of the files were available, so we want to see
        // if the returned error is one for files not available. If so, we check
        // if some of the files showed up anyway.
        let some_missing = match downloader.download_files(&save_dir) {
            Ok(_) => false,
            Err(DownloadError::FilesNotAvailable) => true,
            Err(e) => return Err(e),
        };

        let mut all_added_to_db = true;
        for (file_time, file_path) in expected_met_files {
            match orm::met::MetFile::add_met_file(&mut transaction, &file_path, file_time, file_cfg)
                .await
            {
                Ok(_) => (),
                Err(AddMetFileError::FileAlreadyInDb(p)) => {
                    info!("Met file {} already present in database", p.display())
                }
                Err(AddMetFileError::FileCharacteristicMismatch(p)) => {
                    return Err(anyhow::anyhow!(
                        "{}",
                        AddMetFileError::FileCharacteristicMismatch(p)
                    )
                    .into());
                }
                Err(AddMetFileError::FileDoesNotExist(p)) => {
                    if !some_missing {
                        return Err(anyhow::anyhow!("At least one of the expected met files ({}) was not present on disk, but the downloader reported success", p.display()).into());
                    } else {
                        all_added_to_db = false;
                    }
                }
                Err(AddMetFileError::Other(e)) => return Err(DownloadError::Other(e)),
            }
        }

        transaction
            .commit()
            .await
            .context("Error occurred while committing the transaction")?;

        if all_added_to_db && some_missing {
            warn!("The met downloader returned an error indicating that some files were not available, but all the expected files existed.");
            Ok(())
        } else if some_missing {
            Err(DownloadError::FilesNotAvailable)
        } else {
            Ok(())
        }
    } else {
        for file in downloader.iter_files() {
            println!("Would download {file}");
        }
        println!("");
        Ok(())
    }
}
