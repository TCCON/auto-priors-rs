use std::io::{self, Write};
use std::process::Command;

use anyhow::Context;
use clap::{self, Args};
use chrono::{NaiveDate, Duration};
use orm;


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

pub fn download_files_for_dates(
    met_key: &str, 
    start_date: NaiveDate, 
    end_date: Option<NaiveDate>, 
    config: &orm::config::Config,
    dry_run: bool) -> Result<(), anyhow::Error> 
{
    // First check that the dates are valid
    let end_date = if let Some(ed) = end_date {
        if ed <= start_date {
            return Err(anyhow::Error::msg("end_date must be at least one day after the start_date"))
        }

        ed
    }else{
        start_date + Duration::days(1)
    };

    // Then check that the requested met was defined in the configuration
    let met_cfg = if let Some(c) = config.data.download.get(met_key) {
        c
    }else{
        return Err(anyhow::Error::msg(format!("The requested reanalysis '{met_key}' was not in the configuration")));
    };

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