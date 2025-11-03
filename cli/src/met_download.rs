use std::collections::{HashMap, HashSet};
use std::process::Termination;

use anyhow::{anyhow, Context};
use chrono::{Duration, NaiveDate};
use clap::{self, Args, Subcommand};
use itertools::Itertools;
use log::{debug, error, info, warn};
use orm::utils::get_date_range_intersection;
use orm::{
    self,
    config::{KeyedMetDownloadConfig, MetCfgKey, ProcCfgKey},
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
///
/// # See also
/// - [`get_date_iter_for_specified_met`] if you need an iterator that starts
///   and ends at the correct dates for all mets needed by a specific met file type.
/// - [`get_date_iter_for_specified_proc_config`] if you need an iterator that starts
///   and ends at the correct dates for all mets needed by a processing config.
pub async fn get_date_iter_for_defaults(
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
/// This function is useful in two cases:
///
/// 1. we need to fill in a single (perhaps new) set of met data needed for a processing configuration, or
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
///
/// # See also
/// - [`get_date_iter_for_specified_met`] if you need an iterator that starts
///   and ends at the correct dates for all mets needed by a single met file type.
/// - [`get_date_iter_for_defaults`] if you need an iterator that starts and ends at
///   the correct dates for the default processing configs.
#[deprecated = "no longer used"]
async fn get_date_iter_for_specified_proc_config(
    conn: &mut orm::MySqlConn,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    proc_key: &ProcCfgKey,
    ignore_defaults: bool,
) -> anyhow::Result<orm::utils::DateIterator> {
    let dl_cfgs = config.get_mets_for_processing_config(proc_key)?;
    let default_options = config.get_all_defaults_check_overlap()?;

    let start_date = if let Some(d) = start_date {
        // the user provided start date takes precedence
        debug!("Setting start date to {d} from command line");
        Some(d)
    } else if let Some(d) =
        orm::met::MetFile::get_first_or_last_complete_date_for_config_set(conn, &dl_cfgs, false)
            .await?
    {
        // if that's not available, assume we want to start with the day after the last date for which we have this met
        // data for
        let start = d + Duration::days(1);
        debug!("Setting start date to {start} given the last complete date for processing config '{proc_key}' was {d}");
        Some(start)
    } else if let Some(d) = dl_cfgs.iter().map(|c| c.cfg.earliest_date).max() {
        // if there is no existing met data, take the latest date after which all the files needed for this met are available
        // this should never really *not* have a max value, because if there's an entry for the met in the download HashMap,
        // it really should have at least one entry in the TOML file. But it's possible someone could write the file like
        // `data.download.geosfpit = []`, so we'll be careful here.
        debug!("Setting start date to {d} based on the configured earliest available dates for processing config '{proc_key}'");
        Some(d)
    } else {
        warn!("Could not determine starting date for processing config '{proc_key}'");
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
                .filter(|opts| &opts.processing_configuration == proc_key)
                .map(|opts| opts.start_date)
                .next()
                .flatten()
                .ok_or_else(|| anyhow::Error::msg(
                    format!("No earliest date defined for processing config '{proc_key}' and either no default option sets reference this met or the first one to do so has no start date defined.")
                ))?;

            debug!("Will cover dates {defaults_start} to {end_date} based on configured earliest dates for processing config '{proc_key}'");
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
        if &opts.processing_configuration != proc_key {
            continue;
        }

        let this_start = opts.start_date.or(start_date)
            .ok_or_else(|| anyhow::Error::msg(format!(
                "A default option set for processing config '{proc_key}' has no start date defined and that met has no earliest date defined."
            )))?;
        let this_end = opts.end_date.unwrap_or(end_date);
        debug!("Adding date range {this_start} to {this_end} to iterate over");
        date_ranges.push((this_start, this_end))
    }

    if date_ranges.is_empty() {
        warn!("Will not iterate over any dates; if trying to operate on a date range for which processing config '{proc_key}' is not the default, you may need to pass an extra flag to ignore the configured default met.");
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

/// Return an iterator of dates to download/recheck/etc. for a single type of met file.
///
/// The start and end of the date iterator will depend on:
///
/// - If `start_date` or `end_date` are `Some(_)`, the argument value is used.
/// - If `start_date` is `None`, then it first checks if any files for the given
///   meteorology were previously downloaded and, if so, is set to the next day.
///   If no files exist yet, it starts from the earliest available date defined
///   for that met.
/// - If `end_date` is `None`, it defaults to today.
///
/// # Returns
/// A [`utils::DateIterator`] for the calculated dates. This returns as `Err` if:
///
/// - the given `met_key` is not in the config, or
/// - there is an error query the database for the last file downloaded.
///
/// # See also
/// - [`get_date_iter_for_specified_proc_config`] if you need an iterator that starts
///   and ends at the correct dates for all mets needed by a processing config.
/// - [`get_date_iter_for_defaults`] if you need an iterator that starts and ends at
///   the correct dates for the default processing configs.
pub async fn get_date_iter_for_specified_met(
    conn: &mut orm::MySqlConn,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    met_key: &MetCfgKey,
) -> anyhow::Result<DateIterator> {
    // While we only need these if start and end date aren't specified, it makes the logic
    // for start date cleaner if we have them ready.
    let met_cfg = config.data.met_download.get(met_key).ok_or_else(|| {
        anyhow!("Met key '{met_key}' is not defined in the met download config section")
    })?;
    let keyed_met_cfg = KeyedMetDownloadConfig {
        product_key: met_key,
        cfg: &met_cfg,
    };

    let start_date = if let Some(start) = start_date {
        debug!("Setting start date to {start} from the input argument");
        start
    } else if let Some(d) =
        orm::met::MetFile::get_last_complete_date_for_config(conn, keyed_met_cfg).await?
    {
        let start = d + Duration::days(1);
        debug!("Setting start date to {start} given the last complete date for met '{met_key}' was {d}");
        start
    } else {
        debug!("Setting start date to {} as the earliest available date for met '{met_key}' according to the config", met_cfg.earliest_date);
        met_cfg.earliest_date
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

    Ok(DateIterator::new_one_range(start_date, end_date))
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
    /// for the processing configuration to check.
    /// If not given, the default met(s) are checked.
    #[clap(short = 'p', long = "proc-cfg")]
    pub proc_key: Option<ProcCfgKey>,
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
    let files_found = if let Some(proc_key) = &clargs.proc_key {
        check_one_config_set_files_for_dates(
            conn,
            cfg,
            &proc_key,
            clargs.start_date,
            clargs.end_date,
        )
        .await?
    } else {
        check_default_files_for_dates(conn, cfg, clargs.start_date, clargs.end_date).await?
    };

    // Print the results out in chronological order
    let mut any_missing = false;
    let mut dates: Vec<&NaiveDate> = files_found.keys().collect();
    dates.sort_unstable();
    for date in dates {
        if let Some(day_state) = files_found.get(date) {
            // Since we're iterating over the keys of the map, we should always be inside here
            if !day_state.is_complete() {
                any_missing = true;
            }
            println!("{date}: {day_state}");
        }
    }

    Ok(CheckFileStatus::new(any_missing, false))
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
        let met_keys =
            config.get_mets_for_processing_config(&default_set.processing_configuration)?;
        let met_key_string = met_keys.iter().map(|k| k.product_key).join(", ");
        println!(" - {start} to {end}: {met_key_string}");
    }

    let files_found = check_default_files_for_dates(conn, config, start_date, end_date).await?;
    // ensure the dates are ordered
    let mut dates: Vec<_> = files_found.keys().copied().collect();
    dates.sort_unstable();
    let mut nmissing = 0;
    let mut nincomplete = 0;
    let mut ncomplete = 0;
    let mut nother = 0;
    let ntotal = files_found.len();

    for date in dates {
        // we know this is a key in the map, so safe to unwrap
        let state = *files_found.get(&date).unwrap();
        if state.is_complete() {
            ncomplete += 1;
        } else if state.is_incomplete() {
            nincomplete += 1;
            if detailed {
                println!("{date} = INCOMPLETE");
            }
        } else if state.is_missing() {
            nmissing += 1;
            if detailed {
                println!("{date} = MISSING");
            }
        } else {
            nother += 1;
            if detailed {
                println!("{date} = OTHER ({state})");
            }
        }
    }

    if detailed {
        println!("");
    }
    println!("{ncomplete}/{ntotal} days default met COMPLETE");
    println!("{nincomplete}/{ntotal} days default met INCOMPLETE");
    println!("{nmissing}/{ntotal} days default met MISSING");
    println!("{nother}/{ntotal} days default met ERRORED GETTING STATE");

    Ok(())
}

/// Print a summary table of available met data for a time range
#[derive(Debug, Args)]
pub struct MetTableCli {
    /// Pass this flag to print information about all met data for the given date range, not just the defaults.
    #[clap(short = 'a', long, group = "met")]
    all_mets: bool,

    /// Give this option
    #[clap(short = 'm', long, group = "met")]
    mets: Option<Vec<MetCfgKey>>,

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

    let table_mets = TableMetSelection::from_cl_args(args.all_mets, args.mets);
    print_met_availability_table(conn, config, table_mets, start_date, args.end_date).await
}

pub enum TableMetSelection {
    Defaults,
    All,
    Specific(Vec<MetCfgKey>),
}

impl TableMetSelection {
    fn as_key_and_mets(
        self,
        cfg: &orm::config::Config,
    ) -> anyhow::Result<Vec<KeyedMetDownloadConfig<'_>>> {
        match self {
            TableMetSelection::Defaults => cfg.get_mets_for_defaults(),
            TableMetSelection::All => Ok(cfg.get_all_mets()),
            TableMetSelection::Specific(items) => Ok(cfg
                .get_all_mets()
                .into_iter()
                .filter(|kc| items.contains(kc.product_key))
                .collect_vec()),
        }
    }

    fn from_cl_args(all_mets: bool, specific_mets: Option<Vec<MetCfgKey>>) -> Self {
        if let Some(mets) = specific_mets {
            return Self::Specific(mets);
        }

        if all_mets {
            return Self::All;
        }

        return Self::Defaults;
    }
}

pub async fn print_met_availability_table(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    table_mets: TableMetSelection,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<()> {
    fn make_def_miss(kc: KeyedMetDownloadConfig<'_>) -> MetDayState {
        let n = orm::met::MetFile::num_expected_daily_files(kc.cfg).unwrap_or(999);
        MetDayState::new_missing_infallible(n)
    }

    let end_date = end_date.unwrap_or_else(|| chrono::Utc::now().date_naive());

    let met_types = table_mets.as_key_and_mets(config)?;
    let dates = DateIterator::new_one_range(start_date, end_date).collect_vec();
    let mut rows = dates.iter().map(|_| HashMap::new()).collect_vec();

    for key_and_met in met_types.iter() {
        let mut states = orm::met::MetFile::are_dates_complete_for_config(
            conn,
            start_date,
            Some(end_date),
            *key_and_met,
        )
        .await?;

        let def_miss = make_def_miss(*key_and_met);
        for (date, row) in dates.iter().zip(rows.iter_mut()) {
            let state = states.remove(date).unwrap_or_else(|| def_miss.clone());
            row.insert(key_and_met.product_key, state);
        }
    }

    let met_keys = met_types.iter().map(|kc| kc.product_key).collect_vec();
    print_met_table_inner(rows, dates, met_keys, "N/D")
}

fn print_met_table_inner(
    rows: Vec<HashMap<&MetCfgKey, MetDayState>>,
    dates: Vec<NaiveDate>,
    met_types: Vec<&MetCfgKey>,
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
    builder.set_header(
        date_header
            .into_iter()
            .chain(met_types.into_iter().map(|k| k.to_string())),
    );

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
/// A HashMap with the dates as keys and a day status as values. Returns an `Err` if the end date is not after the start date.
pub async fn check_one_config_set_files_for_dates(
    conn: &mut orm::MySqlConn,
    cfg: &orm::config::Config,
    proc_key: &ProcCfgKey,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<HashMap<NaiveDate, orm::met::MetDayState>> {
    // Verify input dates are valid
    let end_date = check_start_end_date(start_date, end_date)?;

    // Go ahead and get the set of files expected to be downloaded according to the config
    let dl_configs = cfg.get_mets_for_processing_config(proc_key)?;

    // For each date, try to check if the necessary files are present. If we get an error, log it,
    // but keep going.
    let files_map = orm::met::MetFile::are_dates_complete_for_config_set(
        conn,
        start_date,
        Some(end_date),
        &dl_configs,
    )
    .await?;

    Ok(files_map)
}

pub async fn check_default_files_for_dates(
    conn: &mut orm::MySqlConn,
    cfg: &orm::config::Config,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<HashMap<NaiveDate, MetDayState>> {
    // In order to minimize the number of DB calls, we need to:
    //  1. For each default, get the intersection of its date range and the requested date range
    //  2. For each met within that default, check if it is complete for that date range
    //  3. Insert or merge that completeness with previous mets relevant for that date

    let mut states = HashMap::new();

    for default in cfg.default_options.iter() {
        let res = get_date_range_intersection(
            Some(start_date),
            end_date,
            default.start_date,
            default.end_date,
        );
        let (check_start, check_end) = match res {
            // Since we pass in `Some(start_date)`, we know the start date will not be `None`
            Ok((s, e)) => (s.unwrap(), e),
            // An error means that the two date ranges do not overlap, which in this case means
            // we don't need to check anything.
            Err(_) => continue,
        };

        let met_cfgs = cfg.get_mets_for_processing_config(&default.processing_configuration)?;
        for key_and_cfg in met_cfgs {
            let met_states = orm::met::MetFile::are_dates_complete_for_config(
                conn,
                check_start,
                check_end,
                key_and_cfg,
            )
            .await?;

            for (date, state) in met_states.into_iter() {
                states
                    .entry(date)
                    .and_modify(|s: &mut MetDayState| *s = s.merge(&state))
                    .or_insert(state);
            }
        }
    }

    Ok(states)
}

/// Download meteorological reanalysis files for a range of dates [alias: drbd]
#[derive(Debug, Args)]
pub struct DownloadDatesCli {
    /// A comma-separated list of met or processing config keys for which to
    /// download met data, see --proc-keys. Met keys are those defined in the
    /// `[data.met_download]` section of the config, processing keys are those
    /// defined in `[processing_configurations]` section.
    pub target_keys: String,
    /// The first date to download data for, in yyyy-mm-dd format.
    pub start_date: NaiveDate,
    /// The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given
    /// the default is one day after start_date (i.e. just download for start_date).
    pub end_date: Option<NaiveDate>,
    /// Set this flag if the target keys are processing config keys, rather than
    /// met keys. With this set, all the mets required by the processing keys will
    /// be downloaded. Without this, only the mets specified in the list are downloaded.
    /// Note that this means TARGET_KEYS can be met keys OR processing keys, but not
    /// a mixture.
    pub proc_keys: bool,
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
    let met_keys = met_keys_from_target_keys(config, &clargs.target_keys, clargs.proc_keys)?;
    // needed to make the inner values references
    let met_keys = met_keys.iter().collect_vec();

    download_files_for_dates(
        conn,
        &met_keys,
        clargs.start_date,
        clargs.end_date,
        config,
        downloader,
        clargs.dry_run,
    )
    .await
}

fn met_keys_from_target_keys(
    config: &orm::config::Config,
    target_keys: &str,
    are_proc_keys: bool,
) -> anyhow::Result<Vec<MetCfgKey>> {
    let target_keys = target_keys.split(',');
    let met_keys = if are_proc_keys {
        let mut keys = HashSet::new();
        for k in target_keys {
            let k = ProcCfgKey::from(k.to_string());
            let proc_cfg = config
                .processing_configuration
                .get(&k)
                .ok_or_else(|| anyhow!("Unknown processing configuration, '{k}'"))?;
            keys.extend(proc_cfg.required_mets.iter());
        }
        Vec::from_iter(keys.into_iter().map(|k| k.to_owned()))
    } else {
        target_keys.map(|k| MetCfgKey(k.to_string())).collect_vec()
    };
    Ok(met_keys)
}

/// Download missing files for a given meteorological reanalysis [alias: dmr]
#[derive(Debug, Args)]
pub struct DownloadMissingCli {
    /// The first date to download data for, in yyyy-mm-dd format. If not given, it will default
    /// to the most recent day that has all the expected met data. If no complete days are present,
    /// it will use the earliest "earliest_date" value in the TOML
    /// download sections for this met_key.
    #[clap(short = 's', long = "start-date")]
    pub start_date: Option<NaiveDate>,

    /// The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given, it
    /// defaults to today (and so will try to download met data through yesterday).
    #[clap(short = 'e', long = "end-date")]
    pub end_date: Option<NaiveDate>,

    #[clap(short = 'm', long = "met")]
    pub proc_key: Option<ProcCfgKey>,

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
        clargs.proc_key.as_ref(),
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
    proc_key_requested: Option<&ProcCfgKey>,
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
    dry_run: bool,
) -> anyhow::Result<()> {
    // To minimize the effort, we will get a list of the unique mets across all of the processing configurations,
    // then download each met separately
    let proc_keys = if let Some(k) = proc_key_requested {
        vec![k]
    } else {
        config.get_proc_cfgs_with_auto_met_download()
    };
    let met_keys_and_dates = collect_required_date_ranges_for_proc_mets(config, &proc_keys)?;

    // Download each met for either the input date range or the date range it is needed by
    // the processing configurations. Don't stop for errors, try all files and summarize
    // errors at the end.
    let mut errors = vec![];
    for (met_key, (default_start, default_end)) in met_keys_and_dates {
        let res = download_missing_files_for_met(
            conn,
            config,
            start_date.or(Some(default_start)),
            end_date.or(default_end),
            met_key,
            downloader.clone(),
            dry_run,
        )
        .await;

        if let Err(e) = res {
            errors.push(format!("For {met_key}: {e}."));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        let s = errors.join(" ");
        let proc_str = proc_key_requested
            .map(|k| format!("processing '{k}'"))
            .unwrap_or_else(|| "processing defaults".to_string());
        Err(anyhow!("Some met downloads for {proc_str} failed. {s}"))
    }
}

/// Given a list of processing configurations, return a hash map that
/// maps the required met keys to the date ranges for which they are
/// needed by processing configurations.
fn collect_required_date_ranges_for_proc_mets<'cfg>(
    config: &'cfg orm::config::Config,
    proc_keys: &[&ProcCfgKey],
) -> anyhow::Result<HashMap<&'cfg MetCfgKey, (NaiveDate, Option<NaiveDate>)>> {
    let mut met_keys: HashMap<&MetCfgKey, (NaiveDate, Option<NaiveDate>)> = HashMap::new();
    for proc_key in proc_keys {
        let proc_cfg = config
            .processing_configuration
            .get(proc_key)
            .ok_or_else(|| anyhow!("Unknown processing config key, '{proc_key}'"))?;

        for k in proc_cfg.required_mets.iter() {
            if let Some((start, end)) = met_keys.get_mut(k) {
                *start = (*start).min(proc_cfg.start_date);
                *end = orm::utils::later_end_date(*end, proc_cfg.end_date);
            } else {
                met_keys.insert(k, (proc_cfg.start_date, proc_cfg.end_date));
            }
        }
    }
    Ok(met_keys)
}

async fn download_missing_files_for_met(
    conn: &mut orm::MySqlConn,
    config: &orm::config::Config,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    met_cfg_key: &MetCfgKey,
    downloader: impl utils::Downloader + Clone,
    dry_run: bool,
) -> anyhow::Result<()> {
    let date_iter =
        get_date_iter_for_specified_met(conn, start_date, end_date, config, met_cfg_key).await?;

    let met_cfg =
        config.data.met_download.get(met_cfg_key).ok_or_else(|| {
            anyhow!("Tried to download file for an unknown met key: '{met_cfg_key}'")
        })?;
    let key_and_cfg = KeyedMetDownloadConfig {
        product_key: met_cfg_key,
        cfg: &met_cfg,
    };

    let mut missed_dates = vec![];

    for curr_date in date_iter {
        let state =
            orm::met::MetFile::is_date_complete_for_config(conn, curr_date, key_and_cfg).await?;

        let res = if state.is_complete() {
            info!("{curr_date} already complete for met '{met_cfg_key}'");
            Ok(())
        } else {
            info!("{curr_date} must be downloaded for met '{met_cfg_key}'");
            download_one_file_set_one_date(
                conn,
                curr_date,
                key_and_cfg,
                downloader.clone(),
                dry_run,
            )
            .await
        };

        if let Err(DownloadError::FilesNotAvailable) = res {
            // If we didn't successfully download all the files, it's only an error if the given date is long enough ago
            // that the files should have been available.
            let first_optional_date = chrono::Utc::now().date_naive()
                - chrono::Duration::days(met_cfg.days_latency as i64);
            if curr_date >= first_optional_date {
                warn!("Could not download met, '{met_cfg_key}' for {curr_date}, but this may be due to latency (not expecting files from {first_optional_date} on");
            } else {
                missed_dates.push(curr_date);
            }
        } else if let Err(e) = res {
            return Err(e.into());
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
/// Which dates are scanned depends on a number of things. For each met checked, the rules for start dates are:
/// - A start date given by --start-date takes precedence.
/// - If --start-date not given, fall back on the day after the last complete day for that met.
/// - If no complete days exist, fall back on the earliest date we expect this met product to be available
///   (based on the config).
///
/// For end dates:
/// - An end date given by --end-date is preferred.
/// - If not given, then the end date is set to today.
#[derive(Debug, Args)]
pub struct RescanMetCli {
    /// The first date to check for data, in yyyy-mm-dd format. If not given, it will default
    /// to a sensible value, depending on the set of mets chosen to scan.
    #[clap(short = 's', long = "start-date")]
    pub start_date: Option<NaiveDate>,

    /// The last date (exclusive) to check for data, in yyyy-mm-dd format. If not given, it
    /// will default to a sensible value, depending on the set of mets chosen to scan.
    #[clap(short = 'e', long = "end-date")]
    pub end_date: Option<NaiveDate>,

    /// A comma-separated list of met or processing config keys for which to
    /// check for met data, see --proc-keys. Met keys are those defined in the
    /// `[data.met_download]` section of the config, processing keys are those
    /// defined in `[processing_configurations]` section. If omitted, all of the
    /// mets that need downloaded to support the enabled processing configs will
    /// be rescanned.
    #[clap(short = 'm', long = "met")]
    pub target_keys: Option<String>,

    /// Set this flag if the target keys are processing config keys, rather than
    /// met keys. With this set, all the mets required by the processing keys will
    /// be rescanned. Without this, only the mets specified in the list are rescanned.
    /// Note that this means TARGET_KEYS can be met keys OR processing keys, but not
    /// a mixture.
    pub proc_keys: bool,

    /// Set this flag to print what would be added to the database, but not actually modify the database.
    #[clap(short = 'd', long = "dry-run")]
    pub dry_run: bool,
}

pub async fn rescan_met_files_cli(
    conn: &mut orm::MySqlConn,
    clargs: RescanMetCli,
    config: &orm::config::Config,
) -> anyhow::Result<()> {
    let met_keys = if let Some(tgt) = &clargs.target_keys {
        Some(met_keys_from_target_keys(config, &tgt, clargs.proc_keys)?)
    } else {
        None
    };

    let n = rescan_met_files(
        conn,
        clargs.start_date,
        clargs.end_date,
        config,
        met_keys.as_deref(),
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
    met_keys: Option<&[MetCfgKey]>,
    dry_run: bool,
) -> anyhow::Result<u64> {
    // Tried to avoid allocated the vec by just returning the iter, but the lifetimes
    // don't work out. We want to collect the met keys first...
    let met_key_vec = if let Some(keys) = met_keys {
        // This makes it into a vector of references
        Vec::from_iter(keys.iter())
    } else {
        let mut v = HashSet::new();
        for proc_k in config.get_proc_cfgs_with_auto_met_download() {
            let proc = config
                .processing_configuration
                .get(proc_k)
                .ok_or_else(|| anyhow!("Unknown processing configuration '{proc_k}'"))?;
            v.extend(proc.required_mets.iter());
        }
        Vec::from_iter(v.into_iter())
    };

    // ...because this block is the same however we get the list of keys.
    let mut met_keys_and_cfgs = vec![];
    for met_k in met_key_vec {
        let met_cfg = config
            .data
            .met_download
            .get(met_k)
            .ok_or_else(|| anyhow!("Unknown met key '{met_k}'"))?;
        met_keys_and_cfgs.push(KeyedMetDownloadConfig {
            product_key: met_k,
            cfg: met_cfg,
        });
    }

    let mut n_added = 0;
    let mut transaction = conn.begin().await?;

    for met_key_cfg in met_keys_and_cfgs {
        let fallback_start_date =
            orm::met::MetFile::get_last_complete_date_for_config(&mut transaction, met_key_cfg)
                .await?
                .map(|d| d + Duration::days(1))
                .unwrap_or(met_key_cfg.cfg.earliest_date);
        let met_start_date = start_date.unwrap_or(fallback_start_date);
        let met_end_date = end_date.or(met_key_cfg.cfg.latest_date);
        let met_end_date =
            check_start_end_date(met_start_date, met_end_date).with_context(|| {
                anyhow!(
                    "Start/end dates for met '{}' were invalid",
                    met_key_cfg.product_key
                )
            })?;

        for curr_date in DateIterator::new_one_range(met_start_date, met_end_date) {
            info!("Scanning for new met files on {curr_date}");

            for file in met_key_cfg.cfg.expected_files_on_day(curr_date)? {
                if !file.exists() {
                    continue;
                }

                match orm::met::MetFile::file_exists_by_type(&mut transaction, &file, met_key_cfg)
                    .await
                {
                    Ok(true) => {
                        debug!(
                            "{} [{}] already in database",
                            file.display(),
                            met_key_cfg.product_key
                        );
                    }
                    Ok(false) => {
                        if !dry_run {
                            n_added += orm::met::MetFile::add_met_file_infer_date(
                                &mut transaction,
                                &file,
                                met_key_cfg,
                            )
                            .await
                            .and(Ok(1))
                            .unwrap_or_else(|e| {
                                warn!("Error adding {} to the database: {}", file.display(), e);
                                0
                            });
                        } else {
                            println!("Would add {} [{}]", file.display(), met_key_cfg.product_key);
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
///
/// To redownload files, use the `met download-dates` or `met download-missing`
/// subcommands.
#[derive(Debug, Args)]
pub struct RemoveDatesCli {
    /// The first date to delete, in YYYY-MM-DD format.
    start_date: NaiveDate,
    /// The day after the last date to delete, in YYYY-MM-DD format.
    end_date: NaiveDate,
    /// If given, only delete met files for this product.
    #[clap(short = 'm', long)]
    met_product: Option<MetCfgKey>,
    /// If given, print what would be done instead of actually deleting files.
    #[clap(short = 'd', long)]
    dry_run: bool,
}

pub async fn remove_dates_cli(
    conn: &mut orm::MySqlConn,
    args: RemoveDatesCli,
) -> anyhow::Result<()> {
    remove_dates(
        conn,
        args.start_date,
        args.end_date,
        args.met_product,
        args.dry_run,
    )
    .await
}

pub async fn remove_dates(
    conn: &mut orm::MySqlConn,
    start_date: NaiveDate,
    end_date: NaiveDate,
    met_product: Option<MetCfgKey>,
    dry_run: bool,
) -> anyhow::Result<()> {
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
        anyhow::bail!("Failed to delete {n_error_occurred} of {n_files} met files.");
    } else {
        Ok(())
    }
}

pub async fn download_files_for_dates(
    conn: &mut orm::MySqlConn,
    met_keys: &[&MetCfgKey],
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    downloader: impl utils::Downloader + Clone,
    dry_run: bool,
) -> anyhow::Result<()> {
    // First check that the dates are valid
    let end_date = check_start_end_date(start_date, end_date)?;

    // Then check that the requested met was defined in the configuration
    let met_cfgs: Vec<KeyedMetDownloadConfig<'_>> = met_keys
        .iter()
        .map(|k| {
            config
                .data
                .met_download
                .get(k)
                .map(|c| KeyedMetDownloadConfig {
                    product_key: k,
                    cfg: c,
                })
                .ok_or_else(|| anyhow!("Unknown met key: '{k}'"))
        })
        .try_collect()?;

    let mut curr_date = start_date;
    while curr_date < end_date {
        for file_cfg in met_cfgs.iter() {
            download_one_file_set_one_date(conn, curr_date, *file_cfg, downloader.clone(), dry_run)
                .await?;
        }

        curr_date += Duration::days(1);
    }

    Ok(())
}

async fn download_one_file_set_one_date(
    conn: &mut orm::MySqlConn,
    date: NaiveDate,
    file_cfg: KeyedMetDownloadConfig<'_>,
    mut downloader: impl utils::Downloader,
    dry_run: bool,
) -> Result<(), DownloadError> {
    let mut transaction = conn
        .begin()
        .await
        .context("Error occurred while obtaining the transaction ")?;
    let save_dir = &file_cfg.cfg.download_dir;

    if dry_run {
        println!(
            "Would download the following URLs for {date} to {}",
            save_dir.display()
        );
    }

    let mut expected_met_files = vec![];
    let basename_pat = file_cfg.cfg.get_basename_pattern()?;
    for file_time in file_cfg.cfg.times_on_day(date) {
        let file_url = file_time.format(&file_cfg.cfg.url_pattern).to_string();
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
