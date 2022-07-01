use anyhow;
use chrono::NaiveDate;
use clap::{self, Args};
use log::{info, debug};
use std::{path::{PathBuf, Path}, str::FromStr, fs::File, io::BufRead, ffi::OsStr};

use orm::jobs::{ModFmt, VmrFmt, MapFmt};


#[derive(Debug, Args)]
pub struct ParseInputFilesManualCli {
    input_files: Vec<PathBuf>
}

struct FailedParsingError {
    reasons: Vec<String>,
    input_file: PathBuf,
    email: Option<String>
}

impl FailedParsingError {
    fn email_body(&self) -> String {
        let file_basename = self.input_file.file_name().unwrap_or(OsStr::new("?")).to_string_lossy();
        let problems_list = self.problems_list("  * ", "\n");
        return format!("Error parsing input file {file_basename}. Problem(s) were:\n\n{problems_list}");

    }

    fn problems_list(&self, prefix: &str, join: &str) -> String {
        let full_join = String::from_iter([join, prefix]);
        let problems = self.reasons.join(&full_join);
        return format!("{prefix}{problems}")
    }
}

impl From<std::io::Error> for FailedParsingError {
    fn from(e: std::io::Error) -> Self {
        Self {
            reasons: vec![format!("Could not open file due to: {e}")],
            input_file: PathBuf::new(),
            email: None
         }
    }
}

#[derive(Debug)]
struct InputJob {
    site_id: Vec<String>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    lat: Vec<Option<f32>>,
    lon: Vec<Option<f32>>,
    email: String,
    mod_fmt: ModFmt,
    vmr_fmt: VmrFmt,
    map_fmt: MapFmt,
}

impl InputJob {
    fn from_file(input_file: &Path) -> Result<Self, FailedParsingError> {
        let f = File::open(input_file)?;

        let mut builder = InputJobBuilder::new();
        let mut problems = vec![];
        let lines = std::io::BufReader::new(f).lines();

        for (line_idx, (line, field)) in lines.zip(Self::field_order()).enumerate() {
            if let Ok(line) = line {
                let res = if let Some((key, value)) = line.split_once('=') {
                    builder.set_field_in_place(key, value)
                }else{
                    builder.set_field_in_place(field, &line)
                };
                
                if let Err(cause) = res {
                    let i = line_idx + 1;
                    problems.push(format!("Line {i}: {cause}"))
                }
            }
        }

        // Store the email if we found it, we may need it later for an error message.
        let email = builder.email.clone();

        // Go ahead and try to make the final job - that way any error returned includes all of the problems.
        match builder.finalize() {
            Ok(input_job) => {
                if problems.len() == 0 {
                    return Ok(input_job);
                }
            },
            Err(errors) => {
                for field in errors {
                    problems.push(field);
                }
            }
        }

        return Err(FailedParsingError{
            reasons: problems,
            input_file: input_file.to_owned(),
            email: email
        })
    }

    fn field_order() -> Vec<&'static str> {
        return vec![
            "site_id",
            "start_date",
            "end_date",
            "lat",
            "lon",
            "email",
            "mod_fmt",
            "vmr_fmt",
            "map_fmt"
        ]
    }
}

struct InputJobBuilder {
    site_id: Option<Vec<String>>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    lat: Option<Vec<Option<f32>>>,
    lon: Option<Vec<Option<f32>>>,
    email: Option<String>,
    mod_fmt: Option<ModFmt>,
    vmr_fmt: Option<VmrFmt>,
    map_fmt: Option<MapFmt>,
}

impl InputJobBuilder {
    fn new() -> Self {
        Self { site_id: None, start_date: None, end_date: None, lat: None, lon: None, email: None, mod_fmt: None, vmr_fmt: None, map_fmt: None }
    }

    fn finalize(self) -> Result<InputJob, Vec<String>> {
        let mut errors = vec![];
        let mut site_id_missing = false;

        let site_ids = self.site_id.unwrap_or_else(|| {
            errors.push("missing field site_id".to_owned());
            site_id_missing = true;
            return vec![]
        });

        let (site_ids, lats, lons) = if site_id_missing{
            (vec![], self.lat.unwrap_or_default(), self.lon.unwrap_or_default())
        }else{
            let tmp = orm::jobs::Job::expand_site_lat_lon(site_ids, self.lat, self.lon);
            match tmp {
                Ok(t) => t,
                Err(e) => {
                    errors.push(format!("Inconsistent site_id/lat/lon: {e}"));
                    (vec![], vec![], vec![])
                }
            }
        };


        let input_job = InputJob {
            site_id: site_ids,
            start_date: self.start_date.unwrap_or_else(|| {errors.push("smissing field tart_date".to_owned()); NaiveDate::from_ymd(1970, 1, 1)}),
            end_date: self.end_date.unwrap_or_else(|| {errors.push("missing field end_date".to_owned()); NaiveDate::from_ymd(1970, 1, 1)}),
            lat: lats,
            lon: lons,
            email: self.email.unwrap_or_else(|| {errors.push("missing field email".to_owned()); String::new()}),
            mod_fmt: self.mod_fmt.unwrap_or_default(),
            vmr_fmt: self.vmr_fmt.unwrap_or_default(),
            map_fmt: self.map_fmt.unwrap_or_default()
        };

        if errors.len() > 0 {
            return Err(errors)
        }else{
            return Ok(input_job)
        }
    }

    fn is_field_set(&self, field: &str) -> bool {
        match field {
            "site_id" => return self.site_id.is_some(),
            "start_date" => return self.start_date.is_some(),
            "end_date" => return self.end_date.is_some(),
            "lat" => return self.lat.is_some(),
            "lon" => return self.lon.is_some(),
            "email" => return self.email.is_some(),
            "mod_fmt" => return self.mod_fmt.is_some(),
            "vmr_fmt" => return self.vmr_fmt.is_some(),
            "map_fmt" => return self.map_fmt.is_some(),
            _ => return false
        }
    }

    fn site_id(&mut self, sid: &str) -> Result<(), String> {
        let site_ids = orm::jobs::Job::parse_site_id_str(sid);
        self.site_id = Some(site_ids);
        Ok(())
    }

    fn start_date(&mut self, datestr: &str) -> Result<(), String> {
        match NaiveDate::parse_from_str(datestr, "%Y%m%d") {
            Ok(start_date) => {
                self.start_date = Some(start_date);

                // We'll do this check in both the start_date and end_date setters; that way
                // whichever one is set second will catch it.
                if let Some(end_date) = self.end_date {
                    if end_date <= start_date {
                        return Err(format!("start_date {start_date} must be at least one day before the end_date {end_date}"));
                    }
                }
                return Ok(())
            },
            Err(e) => {
                return Err(format!("{e}"))
            }
        }
    }

    fn end_date(&mut self, datestr: &str) -> Result<(), String> {
        match NaiveDate::parse_from_str(datestr, "%Y%m%d") {
            Ok(end_date) => {
                self.end_date = Some(end_date);

                // We'll do this check in both the start_date and end_date setters; that way
                // whichever one is set second will catch it.
                if let Some(start_date) = self.start_date {
                    if end_date <= start_date {
                        return Err(format!("end_date {end_date} must be at least 1 day after the start date {start_date}"));
                    }
                }
                return Ok(())
            },
            Err(e) => {
                return Err(format!("{e}"))
            }
        }
    }

    fn lat(&mut self, valstr: &str) -> Result<(), String> {
        debug!("Parsing lat string: {valstr}");
        self.lat = match orm::jobs::Job::parse_lat_str(valstr) {
            Ok(v) => v,
            Err(e) => return Err(format!("{e}"))
        };
        Ok(())
    }

    fn lon(&mut self, valstr: &str) -> Result<(), String> {
        debug!("Parsing lon string: {valstr}");
        self.lon = match orm::jobs::Job::parse_lon_str(valstr) {
            Ok(v) => v,
            Err(e) => return Err(format!("{e}"))
        };
        Ok(())
    }

    fn email(&mut self, valstr: &str) -> Result<(), String> {
        self.email = Some(valstr.to_owned());
        return Ok(())
    }

    fn mod_fmt(&mut self, valstr: &str) -> Result<(), String> {
        let the_fmt = match ModFmt::from_str(valstr) {
            Ok(v) => v,
            Err(e) => return Err(format!("{e}"))
        };
        self.mod_fmt = Some(the_fmt);
        Ok(())
    }

    fn vmr_fmt(&mut self, valstr: &str) -> Result<(), String> {
        let the_fmt = match VmrFmt::from_str(valstr) {
            Ok(v) => v,
            Err(e) => return Err(format!("{e}"))
        };
        self.vmr_fmt = Some(the_fmt);
        Ok(())
    }

    fn map_fmt(&mut self, valstr: &str) -> Result<(), String> {
        let the_fmt = match MapFmt::from_str(valstr) {
            Ok(v) => v,
            Err(e) => return Err(format!("{e}"))
        };
        self.map_fmt = Some(the_fmt);
        Ok(())
    }

    fn set_field_in_place(&mut self, field: &str, value: &str) -> Result<(), String> {
        if self.is_field_set(field) {
            return Err(format!("{field} given multiple times"))
        }

        match field {
            "site_id" => self.site_id(value),
            "start_date" => self.start_date(value),
            "end_date" => self.end_date(value),
            "lat" => self.lat(value),
            "lon" => self.lon(value),
            "email" => self.email(value),
            "mod_fmt" => self.mod_fmt(value),
            "vmr_fmt" => self.vmr_fmt(value),
            "map_fmt" => self.map_fmt(value),
            _ => Err(format!("Unknown field '{field}'"))
        }?;

        Ok(())
    }
}


pub async fn add_jobs_from_input_files(conn: &mut orm::MySqlPC, clargs: ParseInputFilesManualCli, config: &orm::config::Config) -> anyhow::Result<()> {
    let input_files = clargs.input_files;
    let mut jobs = vec![];
    let mut successful_input_files = vec![];
    let mut failed_input_files = vec![];

    for input_file in input_files {
        match InputJob::from_file(&input_file) {
            Ok(job) => {
                jobs.push(job);
                // TODO: move successful input file into the success folder
                successful_input_files.push(input_file)
            },
            Err(e) => {
                handle_failed_parsing(e);
                failed_input_files.push(input_file);
            }
        }
    }

    for (job, infile) in jobs.into_iter().zip(successful_input_files) {
        let new_id = orm::jobs::Job::add_job_from_args(conn,
            job.site_id,
            job.start_date,
            job.end_date,
            config.execution.output_path.clone(),
            Some(job.email),
            job.lat,
            job.lon,
            Some(job.mod_fmt),
            Some(job.vmr_fmt),
            Some(job.map_fmt),
            None,
            None,
            orm::jobs::TarChoice::Yes
        ).await?;

        info!("Added job {new_id} from file {}", infile.display());
    }
    Ok(())
}

fn handle_failed_parsing(error: FailedParsingError) {
    // TODO: Eventually this will need to email the user and log it as well, but for now, let's just print the message out
    // TODO: copy the failed input file to the failures directory and delete the original. Maybe also write the error?
    let file = error.input_file.display();
    eprint!("Error parsing {file}. ");
    if let Some(email) = &error.email {
        eprintln!("Email to be sent to {email}.")
    }else{
        eprintln!("");
    }

    let sep = "=".repeat(32);
    eprintln!("{sep}\n{}\n{sep}\n", error.email_body());
}