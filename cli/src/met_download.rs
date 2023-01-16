use std::collections::HashMap;
use std::io::{self, Write};
use std::process::Command;

use anyhow::Context;
use clap::{self, Args};
use chrono::{NaiveDate, Duration};
use log::{warn, info};
use orm::{self, geos::GeosDayState};


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


pub async fn check_files_for_dates_cli(conn: &mut orm::MySqlConn, clargs: CheckDatesCli) -> anyhow::Result<()> {
    
    let files_found = check_files_for_dates(
        conn,
        clargs.start_date,
        clargs.end_date,
        clargs.geos_product,
        clargs.met_levels,
        !clargs.no_req_chm).await?;

    // Print the results out in chronological order
    let mut dates: Vec<&NaiveDate> = files_found.keys().collect();
    dates.sort_unstable();
    for date in dates {
        if let Some(v) = files_found.get(date) {
            // Since we're iterating over the keys of the map, we should always be inside here
            let s = if let Some(state) = v {
                state.as_ref()
            }else{
                "UNKNOWN (errored during check)"
            };

            println!("{date}: {s}");
        }
    }

    Ok(())
}

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
    pub met_key: String,
    pub start_date: NaiveDate,
    pub end_date: Option<NaiveDate>,
    #[clap(short='d', long="dry-run")]
    pub dry_run: bool
}


pub fn download_files_for_dates_cli(clargs: DownloadDatesCli, config: &orm::config::Config) -> Result<(), anyhow::Error> {
    download_files_for_dates(
        &clargs.met_key, 
        clargs.start_date, 
        clargs.end_date, 
        config, 
        clargs.dry_run
    )
}

/// Download missing files for a given meteorological reanalysis [alias: dmr]
#[derive(Debug, Args)]
pub struct DownloadMissingCli {
    pub met_key: String,
    #[clap(short = 's', long="start-date")]
    pub start_date: Option<NaiveDate>,
    #[clap(short = 'e', long="end-date")]
    pub end_date: Option<NaiveDate>,
    #[clap(short = 'd', long="dry-run")]
    pub dry_run: bool
}

pub async fn download_missing_files_cli(conn: &mut orm::MySqlConn, clargs: DownloadMissingCli, config: &orm::config::Config) -> Result<(), anyhow::Error> {
    download_missing_files(
        conn,
        &clargs.met_key,
        clargs.start_date,
        clargs.end_date,
        config,
        clargs.dry_run
    ).await
}

pub async fn download_missing_files(
    conn: &mut orm::MySqlConn,
    met_key: &str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    config: &orm::config::Config,
    dry_run: bool) -> Result<(), anyhow::Error> 
{
    let cfgs = config.get_met_configs(met_key)?;
    let start_date = if let Some(start) = start_date {
        start
    }else{
        // If no start date, use the first day we don't have met data for. If no data previously downloaded, use the
        // start date defined for the meteorology. If no meteorology datasets defined, return an error.
        let x = orm::geos::GeosFile::get_last_complete_date_for_config_set(conn, cfgs).await?;
        let y = if let Some(d) = x {
            Some(d)
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
                    download_one_file_one_date(curr_date, cfg, &config.data, dry_run)?;
                }
            }
        }

        curr_date += Duration::days(1);
    }

    Ok(())
}

pub fn download_files_for_dates(
    met_key: &str, 
    start_date: NaiveDate, 
    end_date: Option<NaiveDate>, 
    config: &orm::config::Config,
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
                curr_date, 
                file_cfg, 
                &config.data, 
                dry_run
            )?;
        }

        curr_date += Duration::days(1);
    }

    Ok(())
}


fn download_one_file_one_date(
    date: NaiveDate, 
    file_cfg: &orm::config::DownloadConfig, 
    data_cfg: &orm::config::DataConfig, 
    dry_run: bool) -> Result<(), anyhow::Error>
    
{
    let end = date.and_hms(0, 0, 0) + Duration::days(1);
    let mut file_time = date.and_hms(0, 0, 0);
    let root_save_dir = match file_cfg.data_type {
        orm::geos::GeosDataType::Met => data_cfg.geos_path.as_path(),
        orm::geos::GeosDataType::Chm => data_cfg.chem_path.as_path(),
    };
    
    let subdir = if let Some(sd) = &file_cfg.subdir {
        sd.clone()
    }else{
        file_cfg.levels.standard_subdir()
    };
    
    let save_dir = root_save_dir.join(subdir)
        .canonicalize()
        .with_context(|| format!("Failed to canonicalized the root reanalysis save directory path '{}'", root_save_dir.display()))?;
    
    let mut out: Box<dyn Write> = if dry_run {
        Box::new(io::stdout())
    }else{
        let wget_list = save_dir.join("wget_list.txt");
        Box::new(std::fs::File::create(wget_list)?)
    };

    let file_time_del = Duration::minutes(file_cfg.file_freq_min);

    if dry_run {
        println!("Would download the following URLs for {date} to {}", save_dir.display());
    }

    while file_time < end {
        writeln!(out, "{}", file_time.format(&file_cfg.url_pattern))
            .with_context(|| "Unable to write out download URL")?;
        file_time += file_time_del;
    }

    if !dry_run {
        Command::new("wget")
            .args(["-i", "wget_list.txt"])
            .current_dir(&save_dir)
            .spawn()
            .with_context(|| format!("wget command to download {} in {} failed", file_time, save_dir.display()))?;
    }else{
        println!("");
    }

    Ok(())
}