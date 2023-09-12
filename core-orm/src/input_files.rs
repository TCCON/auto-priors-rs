use std::{path::{PathBuf, Path}, ffi::OsStr, io::BufRead, str::FromStr, collections::HashSet, fmt::Display};

use chrono::{NaiveDate, DateTime, Local};
use itertools::Itertools;
use log::{debug, info, warn, error};

use crate::{jobs::{ModFmt, VmrFmt, MapFmt}, config::{Config, BlacklistIdentifier, BlacklistEntry}, utils};

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

    fn new_for_database_error(input_file: PathBuf, email: String) -> Self {
        let reasons = vec!["There was an error while adding this job to the database. You may try submitting this job again; if it continues to fail, please report this problem".to_string()];
        Self { reasons: reasons, input_file, email: Some(email) }
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
enum MissingMetError {
    CouldNotCheck(anyhow::Error),
    MissingDates(Vec<NaiveDate>)
}

impl MissingMetError {
    fn to_problem(self) -> String {
        match self {
            MissingMetError::CouldNotCheck(_) => {
                // don't use Display impl here; don't want to expose inner errors to an email
                "There was an error while verifying that the met data required for your request. Please try resubmitting. If the error persists, contact the adminstrators of the GGG priors automation.".to_string()
            },
            MissingMetError::MissingDates(_) => {
                format!("Your request could not be fulfilled: {self}. If you believe this should not be the case, contact the GGG priors automation administrators.")
            },
        }
    }
}

impl From<anyhow::Error> for MissingMetError {
    fn from(value: anyhow::Error) -> Self {
        Self::CouldNotCheck(value)
    }
}

impl Display for MissingMetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MissingMetError::CouldNotCheck(e) => write!(f, "error occurred while checking met availability for job request: {e}"),
            MissingMetError::MissingDates(dates) => {
                let n = dates.len();
                let date_str = dates.iter().map(|d| d.to_string()).join(", ");
                write!(f, "met data was unavailable for {n} of the dates requested: {date_str}")
            },
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
    confirmation: bool
}

impl InputJob {
    async fn from_file(input_file: &Path, conn: &mut crate::MySqlConn, config: &Config) -> Result<Self, FailedParsingError> {
        let f = std::fs::File::open(input_file)?;

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

        // Confirm that the required met files are available, unless missing start/end dates
        // was one of the problems encountered in the input file
        if let (Some(start), Some(end)) = (builder.start_date, builder.end_date) {
            if let Err(e) = check_met_available(conn, config, start, end).await {
                error!("Error occurred while checking met file availability for input file '{}': {e:?}", input_file.display());
                problems.push(e.to_problem());
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
            "map_fmt",
            "confirmation"
        ]
    }

    fn get_field_as_string(&self, field: &str) -> Option<String> {
        let s = match field {
            "site_id" => self.site_id.join(", "),
            "start_date" => self.start_date.to_string(),
            "end_date" => self.end_date.to_string(),
            "lat" => self.lat.iter().map(|v| v.map(|x| format!("{x:.3}")).unwrap_or_else(|| "(default)".to_string())).join(", "),
            "lon" => self.lon.iter().map(|v| v.map(|x| format!("{x:.3}")).unwrap_or_else(|| "(default)".to_string())).join(", "),
            "email" => self.email.clone(),
            "mod_fmt" => self.mod_fmt.to_string(),
            "vmr_fmt" => self.vmr_fmt.to_string(),
            "map_fmt" => self.map_fmt.to_string(),
            "confirmation" => self.confirmation.to_string(),
            _ => return None
        };

        Some(s)
    }
}

impl Display for InputJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, field) in Self::field_order().iter().enumerate() {
            let val = self.get_field_as_string(field).unwrap_or_else(|| "?".to_string());
            if i > 0 { writeln!(f, "")?; }
            write!(f, "{field} = {val}")?;
        }
        Ok(())
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
    confirmation: Option<bool>,
}

impl InputJobBuilder {
    fn new() -> Self {
        Self { site_id: None, start_date: None, end_date: None, lat: None, lon: None, email: None, mod_fmt: None, vmr_fmt: None, map_fmt: None, confirmation: None }
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
            let tmp = crate::jobs::Job::expand_site_lat_lon(site_ids, self.lat, self.lon);
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
            start_date: self.start_date.unwrap_or_else(|| {errors.push("missing field start_date".to_owned()); NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()}),
            end_date: self.end_date.unwrap_or_else(|| {errors.push("missing field end_date".to_owned()); NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()}),
            lat: lats,
            lon: lons,
            email: self.email.unwrap_or_else(|| {errors.push("missing field email".to_owned()); String::new()}),
            mod_fmt: self.mod_fmt.unwrap_or_default(),
            vmr_fmt: self.vmr_fmt.unwrap_or_default(),
            map_fmt: self.map_fmt.unwrap_or_default(),
            confirmation: self.confirmation.unwrap_or(true)
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
            "confirmation" => return self.confirmation.is_some(),
            _ => return false
        }
    }

    fn site_id(&mut self, sid: &str) -> Result<(), String> {
        let site_ids = crate::jobs::Job::parse_site_id_str(sid);
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
        self.lat = match crate::jobs::Job::parse_lat_str(valstr) {
            Ok(v) => v,
            Err(e) => return Err(format!("{e}"))
        };
        Ok(())
    }

    fn lon(&mut self, valstr: &str) -> Result<(), String> {
        debug!("Parsing lon string: {valstr}");
        self.lon = match crate::jobs::Job::parse_lon_str(valstr) {
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

    fn confirmation(&mut self, valstr: &str) -> Result<(), String> {
        let conf = match bool::from_str(valstr) {
            Ok(v) => v,
            Err(e) => return Err(format!("{e}"))
        };
        self.confirmation = Some(conf);
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
            "confirmation" => self.confirmation(value),
            _ => Err(format!("Unknown field '{field}'"))
        }?;

        Ok(())
    }
}


pub async fn add_jobs_from_input_files(
    conn: &mut crate::MySqlConn, 
    config: &Config,
    input_files: &[PathBuf],
    save_dir: &Path,
    mover: &mut InputFileMoveHandler
) -> anyhow::Result<()> {
    let mut jobs = vec![];
    let mut successful_input_files = vec![];
    let mut unmoved_input_file_errors = vec![];

    // This ended up being more complicated than I expected: there is a chance that copying or deleting the input file can fail. 
    // If it gets deleted but not copied, that's one thing, but the real problem comes if a file cannot be deleted. In that case,
    // we might end up parsing the input file over and over because it just sits there. Hence this convoluted logic, which checks
    for input_file in input_files {

        // This is the key: the first time we encounter a file, as long as we can read it, we will try to 
        // parse it. Then, if the move fails, it gets added to a list of files that failed to copy or delete
        if mover.file_previously_errored(&input_file) {
            warn!("Skipping input file {}, it looks like this was handled already but could not be moved for some reason", input_file.display());
            continue;
        }

        match InputJob::from_file(&input_file, conn, config).await {
            Ok(job) => {
                jobs.push(job);
                successful_input_files.push(input_file);
                
            },
            Err(e) => {
                email_user_for_failed_parsing(e, config);

                // Move failed files now; for successful file, we need the job number so that has to happen later
                let now = chrono::Local::now();
                let dest_file = config.execution.failure_input_file_dir.join(
                    format!("failed_input_file_{}.txt", now.format("%Y-%m-%d %H:%M:%S.%f"))
                );
                if let Err(e) = mover.move_file(&input_file, &dest_file) {
                    unmoved_input_file_errors.push(e);
                }
            }
        };

        
    }

    // Now that we've parsed the input files (successfully or not) we can add them to the database. Unless
    // we've blacklisted the person who made the request for abusing the system
    for (job, infile) in jobs.into_iter().zip(successful_input_files) {
        if let Some(blacklist_entry) = get_blacklist_match(&job, &config) {
            let file_name = infile.file_name().map(|s| s.to_string_lossy()).unwrap_or("??".into());
            handle_blacklisted_input(blacklist_entry, &job.email, &file_name, config);
            info!("Rejected input file {} due to matching blacklist entry {}", infile.display(), blacklist_entry);

            let dest_file = config.execution.failure_input_file_dir.join(
                format!("blacklisted_input_file_{}.txt", chrono::Local::now())
            );

            if let Err(e) = mover.move_file(&infile, &dest_file) {
                unmoved_input_file_errors.push(e);
            }
        } else {

            let res = crate::jobs::Job::add_job_from_args(conn,
                job.site_id.clone(),
                job.start_date,
                job.end_date,
                save_dir.to_owned(),
                Some(job.email.clone()),
                job.lat.clone(),
                job.lon.clone(),
                &config.execution.submitted_job_queue,
                Some(job.mod_fmt),
                Some(job.vmr_fmt),
                Some(job.map_fmt),
                None,
                None,
                Some(crate::jobs::TarChoice::Yes)
            ).await;

            match res {
                Ok(new_id) => {
                    // Successfully added the job to the database, so move the file and email the user a confirmation
                    // Moving the file might still error, so handle that internally
                    info!("Added job {new_id} from file {}", infile.display());

                    let dest_file = config.execution.success_input_file_dir.join(
                        format!("job_{new_id:09}_input_file.txt")
                    );

                    if let Err(e) = mover.move_file(&infile, &dest_file) {
                        unmoved_input_file_errors.push(e);
                    }

                    if job.confirmation {
                        confirm_successful_parsing(&job, config, infile);
                    }
                },
                Err(e) => {
                    warn!("Error adding job from input file {} to database: {e:?}", infile.display());
                    let parsing_err = FailedParsingError::new_for_database_error(infile.to_path_buf(), job.email);
                    email_user_for_failed_parsing(parsing_err, config);
                }
            }
        }
    }

    if unmoved_input_file_errors.is_empty() {
        Ok(())
    } else {
        let nerrs = unmoved_input_file_errors.len();
        let error_causes = unmoved_input_file_errors.into_iter()
            .map(|e| e.to_string())
            .join("\n");
        anyhow::bail!("There were problems moving {nerrs} input files. Reasons were:\n{error_causes}")
    }
}

fn confirm_successful_parsing(job: &InputJob, config: &Config, input_file: &Path) {
    let email = &job.email;
    let input_file_name = input_file.file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_else(|| "??".into());
    let body = format!("This confirms successful receipt of the following request for GGG priors:\n\n{job}\n\nTo disable these emails, set 'confirmation=false' (without the quotes) as the last line of your input file");
    config.email.send_mail(
        &[email],
        None,
        None,
        &format!("Confirming receipt of GGG priors request file {}", input_file_name),
        &body
    ).unwrap_or_else(|e| {
        warn!("Failed to send confirmation email to {email}. Reason was: {e}")
    });
}

fn email_user_for_failed_parsing(error: FailedParsingError, config: &Config) {
    // Log the error
    let file = error.input_file.display();
    let warn_msg = if let Some(email) = &error.email {
        format!("Error parsing {file}. Email to be sent to {email}.")
    } else {
        format!("Error parsing {file}. Could not identify email to contact.")
    };

    let sep = "=".repeat(32);
    warn!("{warn_msg}\n{sep}\n{}\n{sep}\n", error.email_body());

    // Email the user, if we could parse the file enough to find the email
    let input_file_name = error.input_file
        .file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_else(|| "??".into());
    if let Some(email) = &error.email {
        config.email.send_mail(
            &[email.as_str()],
            None,
            None,
            &format!("Failed parsing AutoModMaker request file {}", input_file_name), 
            &error.email_body()
        ).unwrap_or_else(|e| {
            warn!("Failed to send email to {email}, reason was: {e:?}")
        });
    }
}

fn get_blacklist_match<'b>(job: &InputJob, config: &'b Config) -> Option<&'b BlacklistEntry> {
    for entry in config.blacklist.iter() {
        match &entry.identifier {
            BlacklistIdentifier::SubmitterEmail { submitter } => {
                if submitter == &job.email {
                    return Some(entry)
                }
            },
        }
    }

    None
}

fn handle_blacklisted_input(blacklist_entry: &BlacklistEntry, submitter_email: &str, file_name: &str, config: &Config) {
    if blacklist_entry.silent {
        return;
    }

    let subj = "GGG priors request rejected";
    let body = if let Some(reason) = &blacklist_entry.reason {
        format!("Your priors request input file {file_name} has been rejected; further requests will NOT be accepted. Reason: {reason}.")
    } else {
        format!("Your priors request input file {file_name} has been rejected; further requests will NOT be accepted.")
    };

    config.email.send_mail(&[submitter_email], None, None, subj, &body)
        .unwrap_or_else(|e| warn!("Failed to send blacklist email to {submitter_email}, error was: {e:?}"));
}

async fn check_met_available(conn: &mut crate::MySqlConn, config: &Config, start_date: NaiveDate, end_date: NaiveDate) -> Result<(), MissingMetError> {
    let mut missing_dates = vec![];

    for date in utils::DateIterator::new_one_range(start_date, end_date) {
        let state = crate::met::MetFile::is_date_complete_for_default_mets(conn, config, date).await?;
        if !state.is_complete() {
            missing_dates.push(date);
        }
    }

    if !missing_dates.is_empty() {
        Err(MissingMetError::MissingDates(missing_dates))
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub struct InputFileMoveHandler {
    last_clear_time: DateTime<Local>,
    errored_files: HashSet<PathBuf>
}

impl InputFileMoveHandler {
    pub fn new() -> Self {
        Self { last_clear_time: Local::now(), errored_files: HashSet::new() }
    }

    /// Returns `true` if we tried to delete `file` recently and doing so failed
    fn file_previously_errored(&self, file: &Path) -> bool {
        self.errored_files.contains(file)
    }

    /// Move `from_file` into the directory `to_dir`
    /// 
    /// # Returns
    /// - `Ok(true)` if the move succeeded
    /// - `Ok(false)` if the move failed, but removing the file failed recently
    /// - `Err(e)` if this is the first time moving this file has failed recently
    /// 
    /// This is intended to help avoid spamming the admin emails if a file cannot be deleted.
    fn move_file(&mut self, from_file: &Path, to_file: &Path) -> anyhow::Result<bool> {
        let now = Local::now();
        // Clear the cache of failed files every 3 days. That keeps it from getting too large (hopefully)
        // but gives time to respond to emails about failed file removals.
        if now - self.last_clear_time > chrono::Duration::days(3) {
            self.errored_files.clear();
            self.last_clear_time = now;
        }

        let res = Self::move_file_inner(from_file, to_file);
        if res.is_err() && self.errored_files.contains(from_file) {
            // We've seen this file recently, so don't treat the move error as an actual error
            Ok(false)
        } else if res.is_err() {
            if let InputFileMoveError::CopyAndRemoveFail(_, _, _) | InputFileMoveError::RemoveFail(_, _) = res {
                // If we didn't remove the file, we need to ignore it on successive 
                self.errored_files.insert(from_file.to_path_buf());
            }
            anyhow::bail!(res.to_string())
        } else {
            Ok(true)
        }
    }

    fn move_file_inner<'p>(from_file: &'p Path, to_file: &Path) -> InputFileMoveError<'p> {
        let res_copy = std::fs::copy(from_file, to_file);
        let res_rm = std::fs::remove_file(from_file);

        if let (Err(e_cp), Err(e_rm)) = (&res_copy, &res_rm) {
            // anyhow::bail!("URGENT: could not copy or remove original input file {}. Errors were\nCopying: {e_cp}\nRemoving: {e_rm}", from_file.display())
            InputFileMoveError::CopyAndRemoveFail(from_file, e_cp.to_string(), e_rm.to_string())
        } else if let Err(e) = res_copy {
            // anyhow::bail!("Could not copy original input file {}, file was deleted. Error was: {e}", from_file.display())
            InputFileMoveError::CopyFail(from_file, e.to_string())
        } else if let Err(e) = res_rm {
            // anyhow::bail!("URGENT: Could not remove original input file {}. Error was: {e}", from_file.display())
            InputFileMoveError::RemoveFail(from_file, e.to_string())
        } else {
            InputFileMoveError::Ok(from_file)
        }            
    }
}

#[derive(Debug, Clone)]
enum InputFileMoveError<'p> {
    Ok(&'p Path),
    CopyFail(&'p Path, String),
    RemoveFail(&'p Path, String),
    CopyAndRemoveFail(&'p Path, String, String)
}

impl<'p> InputFileMoveError<'p> {
    fn is_err(&self) -> bool {
        match self {
            InputFileMoveError::Ok(_) => false,
            _ => true
        }
    }
}

impl<'p> Display for InputFileMoveError<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputFileMoveError::Ok(p) => write!(f, "{} moved successfully", p.display()),
            InputFileMoveError::CopyFail(p, e) => write!(f, "Could not copy original input file {}, file was deleted. Error was: {e}", p.display()),
            InputFileMoveError::RemoveFail(p, e) => write!(f, "URGENT: Could not remove original input file {}. Error was: {e}", p.display()),
            InputFileMoveError::CopyAndRemoveFail(p, e_cp, e_rm) => write!(f, "URGENT: could not copy or remove original input file {}. Errors were\nCopying: {e_cp}\nRemoving: {e_rm}", p.display()),
        }
    }
}