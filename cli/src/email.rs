use std::{path::{PathBuf, Path}, io::{Read, BufReader, BufRead}, str::FromStr, fmt::Display};

use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime};
use clap::{Args, Subcommand};
use itertools::Itertools;
use log::warn;
use orm::{MySqlConn, config::Config, jobs::Job, email::SendMail};
use regex::Regex;
use serde::Deserialize;
use std::sync::OnceLock;

static FORM_SPLIT_RE: OnceLock<Regex> = OnceLock::new();

/// Send bulk emails about the priors
#[derive(Debug, Args)]
pub struct EmailCli {
    #[clap(subcommand)]
    pub commands: EmailActions
}

#[derive(Debug, Subcommand)]
pub enum EmailActions {
    /// Send an email to anyone who has previously submitted a job
    Submitters(EmailSubmittersCli),
    /// Send an email summarizing current jobs
    CurrentJobs(CurrentJobsReportCli),
    /// Send an email about previously finished jobs
    PastJobs(CompletedJobsReportCli),
    /// Send an email summarizing new standard site requests
    StdSiteReq(StdSiteRequestCli),
}


/// Send an email to anyone who has previously submitted a job
/// 
/// To add additional emails to this list, use the "extra_submitters"
/// option in the email section of the configuration file.
#[derive(Debug, Args)]
pub struct EmailSubmittersCli {
    /// Who to use as the "to" email address; all the past submitters will be blind carbon copied
    to: String,

    /// Subject line for the email
    #[clap(short='s', long)]
    subject: String,

    /// The body of the email. For longer emails, you can use the --body-file argument instead.
    #[clap(short='b', long)]
    body: Option<String>,

    /// Path to a file containing the body of the email. For short emails, you can use --body instead.
    #[clap(short='f', long)]
    body_file: Option<PathBuf>,

    /// By default, if --body-file is used for the body, then it will be softwrapped, meaning individual
    /// newlines are removed and multiple consecutive newlines are reduced to 2. This makes the email
    /// body look nicer in viewers that do softwrapping. Use this flag to disable that and keep all
    /// newlines.
    #[clap(short='k', long)]
    keep_newlines: bool,

    /// Use a mock email backend rather that the configured one.
    #[clap(short='d', long)]
    dry_run: bool,
}

pub async fn email_past_job_submitters_cli(conn: &mut MySqlConn, config: &Config, args: EmailSubmittersCli) -> anyhow::Result<()> {
    if args.body.is_some() && args.body_file.is_some() {
        anyhow::bail!("--body and --body-file are mutually exclusive");
    }

    let body = if let Some(b) = &args.body {
        b.to_string()
    } else if let Some(path) = &args.body_file {
        let mut file = std::fs::File::open(path).context("Error occurred trying to open the --body-file")?;
        let mut buf = String::new();
        if args.keep_newlines {
            file.read_to_string(&mut buf).context("Error occurred while trying to read the --body-file")?;
        } else {
            orm::utils::softwrap(std::io::BufReader::new(file), &mut buf)?;
        }
        buf
    } else {
        anyhow::bail!("Must give one of --body or --body-file");
    };

    email_past_job_submitters(conn, config, &args.to, &args.subject, &body, args.dry_run).await
}

pub async fn email_past_job_submitters(conn: &mut MySqlConn, config: &Config, to: &str, subject: &str, body: &str, dry_run: bool) -> anyhow::Result<()> {
    let emails = make_submitter_email_list(conn, config).await?;

    let emails_ref: Vec<_> = emails.iter().map(|e| e.as_str()).collect();
    if dry_run {
        let mock = orm::email::MockEmail{};
        mock.send_mail(
            &[to],
            &config.email.from_address.to_string(),
            None,
            Some(&emails_ref),
            subject, 
            body
        )?;
    } else {
        config.email.send_mail(
            &[to],
            None,
            Some(&emails_ref),
            subject,
            body
        )?;
    }
    Ok(())
}

async fn make_submitter_email_list(conn: &mut MySqlConn, config: &Config) -> anyhow::Result<Vec<String>> {
    let mut emails = Job::get_distinct_submitter_emails(conn).await?
        .into_iter()
        .filter_map(|addr| {
            // A common mistake is to put angle brackets around the email address
            let trimmed_addr = addr.trim_start_matches('<').trim_end_matches('>');
            if orm::utils::is_valid_email(trimmed_addr) {
                Some(trimmed_addr.to_string())
            } else {
                warn!("Skipping invalid email address {trimmed_addr}");
                None
            }
        }).collect_vec();

    for extra_addr in config.email.extra_submitters.iter() {
        emails.push(extra_addr.to_string());
    }

    emails.dedup();
    emails.sort_unstable();

    Ok(emails)
}


/// Send an email reporting on pending and running jobs
#[derive(Debug, Args)]
pub struct CurrentJobsReportCli {
    /// To whom to send the email report. May give multiple emails as separate arguments,
    /// if none are given, the admins will be emailed.
    to: Vec<String>
}

pub async fn email_current_jobs_cli(conn: &mut MySqlConn, config: &Config, args: CurrentJobsReportCli) -> anyhow::Result<()> {
    let to = if args.to.is_empty() {
        config.email.admin_emails_string_list()
    } else {
        args.to
    };

    let to: Vec<_> = to.iter().map(|s| s.as_str()).collect();
    orm::email::email_current_jobs(conn, config, &to).await
}

/// Send an email reporting on jobs completed or failed in a given date range
#[derive(Debug, Args)]
pub struct CompletedJobsReportCli {
    /// Only include jobs up to (not including) midnight on this date. If not given,
    /// midnight tomorrow will be used (thus reporting on all jobs from START_DATE until
    /// now).
    #[clap(short = 'e', long)]
    end_date: Option<NaiveDate>,

    /// The first date to assemble completed jobs for.
    start_date: NaiveDate,

    /// To whom to send the email report. May give multiple emails as separate arguments,
    /// if none are given, the admins will be emailed.
    to: Vec<String>

    
}

pub async fn email_completed_jobs_cli(conn: &mut MySqlConn, config: &Config, args: CompletedJobsReportCli) -> anyhow::Result<()> {
    let to = if args.to.is_empty() {
        config.email.admin_emails_string_list()
    } else {
        args.to
    };

    let to: Vec<_> = to.iter().map(|s| s.as_str()).collect();
    orm::email::email_completed_jobs(conn, config, &to, args.start_date, args.end_date).await
}


#[derive(Debug, Args)]
pub struct StdSiteRequestCli {
    /// Path to the .csv of the priors requests, downloaded from Google sheets
    request_csv: PathBuf,

    /// Emails to send the requests to. If none given, then will send to the emails in the
    /// configuration under [email.std_site_req_emails].
    to: Vec<String>,

    /// Whether to send the emails or only send mock emails.
    #[clap(short='d', long)]
    dry_run: bool,
}


#[derive(Debug, Deserialize)]
struct RequestRow {
    #[serde(deserialize_with = "deserialize_google_datetime")]
    timestamp: NaiveDateTime,
    submitter_name: String,
    submitter_inst: String,
    _submitter_email: String,  // think this was left in the spreadsheet from before I had "collecting emails" turned on
    observations: String,
    #[serde(deserialize_with = "deserialize_google_date")]
    obs_start_date: NaiveDate,
    #[serde(deserialize_with = "deserialize_google_date_opt")]
    obs_end_date: Option<NaiveDate>,
    support: String,
    is_tccon_member: String,
    is_coccon_member: String,
    data_distribution: String,
    desired_site_id: String,
    desired_longitude: String,
    desired_latitude: String,
    custom_loc_email: Option<String>,
    custom_loc_sids: Option<String>,
    multiple_instruments: String,
    days_per_week: u8,
    frac_year: f32,
    contact_email: String,
    decision: Option<String>,
}

impl FromStr for RequestRow {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let row = split_google_sheets_csv_line(s);
        let record = csv::StringRecord::from(row);
        dbg!(&record);
        let request: RequestRow = record.deserialize(None)?;
        Ok(request)
    }
}

impl Display for RequestRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Requested on: {}", self.timestamp)?;
        writeln!(f, "Submitter name: {}", self.submitter_name)?;
        writeln!(f, "Submitter institution: {}", self.submitter_inst)?;
        writeln!(f, "Observations for which priors are requested: {}", self.observations)?;
        if let Some(end) = self.obs_end_date {
            writeln!(f, "Observations date range: {} to {}", self.obs_start_date, end)?;
        } else {
            writeln!(f, "Observations date range: {} to (no end date)", self.obs_start_date)?;
        }
        writeln!(f, "Observation funding/support: {}", self.support)?;
        writeln!(f, "TCCON member: {}", self.is_tccon_member)?;
        writeln!(f, "COCCON member: {}", self.is_coccon_member)?;
        writeln!(f, "Data distribution w/i one year: {}", self.data_distribution)?;
        writeln!(f, "Desired site ID, long, lat: {}, {}, {}", self.desired_site_id, self.desired_longitude, self.desired_latitude)?;
        writeln!(f, "Email used to request custom location: {}", self.custom_loc_email.as_deref().unwrap_or("Not supplied"))?;
        writeln!(f, "Site ID(s) used to request custom location: {}", self.custom_loc_sids.as_deref().unwrap_or("Not supplied"))?;
        writeln!(f, "Multiple instruments w/i 100 km: {}", self.multiple_instruments)?;
        writeln!(f, "Avg. days per week obs. attempted: {}", self.days_per_week)?;
        writeln!(f, "Avg. percent of year obs. attempted: {:.1}%", self.frac_year*100.0)?;
        writeln!(f, "Contact email: {}", self.contact_email)?;
        Ok(())
    }
}

pub async fn email_std_site_request_info_cli(conn: &mut MySqlConn, config: &Config, args: StdSiteRequestCli) -> anyhow::Result<()> {
    let to_emails = if !args.to.is_empty() {
        args.to
    } else if let Some(to) = &config.email.std_site_req_emails {
        to.iter().map(|addr| addr.to_string()).collect_vec()
    } else {
        anyhow::bail!("Must provide emails to send to by command line or configuration")
    };

    let to_emails = to_emails.iter().map(|s| s.as_str()).collect_vec();
    email_std_site_request_info(conn, config, &args.request_csv, &to_emails, args.dry_run).await
    // debug_csv(&args.request_csv);
    // Ok(())
}

pub async fn email_std_site_request_info(conn: &mut MySqlConn, config: &Config, request_csv: &Path, to: &[&str], dry_run: bool) -> anyhow::Result<()> {
    // Get the spreadsheet entries. We need to skip over the first line
    // because we're ignoring the headers since they are too long to use as field names.
    let f = std::fs::File::open(request_csv)?;
    let mut f = BufReader::new(f);
    let mut buf = String::new();
    f.read_line(&mut buf)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(f);
    for result in reader.deserialize() {
        let row: RequestRow = result?;
        if row.decision.is_some() {
            // We already made a decision on this site, so don't send an email about it
            continue;
        }
        let (n_by_email, n_by_sids) = count_jobs_for_request(conn, row.custom_loc_email.as_deref(), row.custom_loc_sids.as_deref()).await?;
        let body = format!("{row}\nFrom the database, found {n_by_email} jobs under the custom location request email(s) and of those {n_by_sids} contained the custom location site ID(s)");
        let subject = "Standard site priors request summary";

        if dry_run {
            let mock = orm::email::MockEmail{};
            mock.send_mail(to, &config.email.from_address.to_string(),None,None, subject, &body)?; 
        } else {
            config.email.send_mail(to, None, None, "Standard site priors request summary", &body)?;
        }
    }

    Ok(())
}

async fn count_jobs_for_request(conn: &mut MySqlConn, emails: Option<&str>, site_ids: Option<&str>) -> anyhow::Result<(usize, usize)> {
    let re = FORM_SPLIT_RE.get_or_init(|| {
        Regex::new(r"\s*[,\s]\s*").unwrap()
    });

    // Handle email filtering first. We're making our best guess that if users enter >1 email and/or site ID
    // they'll be separated by spaces maybe with a comma in there.
    let jobs = if let Some(emails) = emails {
        let after_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let mut all_jobs = vec![];
        for addr in re.split(emails) {
            dbg!(addr);
            let addr_jobs = orm::jobs::Job::get_jobs_for_user_submitted_after(conn, addr, after_date).await?;
            all_jobs.extend(addr_jobs);
        }
        all_jobs
    } else {
        orm::jobs::Job::get_jobs_list(conn, false).await?
    };
    let n_by_emails = jobs.len();

    let n_by_sids = if let Some(site_ids) = site_ids {
        let site_ids = re.split(site_ids).collect_vec();
        jobs.into_iter().filter(|j| j.site_id.iter().any(|sid| site_ids.contains(&sid.as_str()))).count()
    } else {
        jobs.len()
    };
    Ok((n_by_emails, n_by_sids))
}

fn deserialize_google_datetime<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where D: serde::Deserializer<'de>
{
    let s = String::deserialize(deserializer)?;
    let dt = chrono::NaiveDateTime::parse_from_str(&s, "%-m/%-d/%Y %H:%M:%S")
        .map_err(serde::de::Error::custom)?;
    Ok(dt)
}

fn deserialize_google_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where D: serde::Deserializer<'de>
{
    deserialize_google_date_opt(deserializer)?
        .ok_or_else(|| serde::de::Error::custom("Got an empty string when expecting a non-optional date"))
}

fn deserialize_google_date_opt<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
where D: serde::Deserializer<'de>
{

    let s = String::deserialize(deserializer)?;
    if s.is_empty() {
        return Ok(None)
    }

    let d = chrono::NaiveDate::parse_from_str(&s, "%-m/%-d/%Y")
        .map_err(serde::de::Error::custom)?;
    Ok(Some(d))
}

fn split_google_sheets_csv_line(line: &str) -> Vec<String> {
    fn prune_entry(entry: &str) -> String {
        let entry = if entry.starts_with('"') && entry.ends_with('"') {
            let n = entry.len();
            &entry[1..n-1]
        } else {
            entry
        };
        entry.replace("\"\"", "\"")
    }
    let mut entries = vec![];
    let mut it = line.char_indices().peekable();
    let mut istart = 0;
    let mut in_quotes = false;
    loop {
        if let Some((i, c)) = it.next() {
            if c == ',' && !in_quotes {
                // If there are commas in the actual text of a cell, then the cell should be quoted
                // Otherwise when we see a command, that is our cue to split. Also trim leading or
                // trailing quotes (which are usually there to allow commas in the cell) and replace
                // any "" with just " (since two " in a row is an escaped ")
                entries.push(prune_entry(&line[istart..i]));
                istart = i+1;
            } else if c == '"' {
                // Google seems to use two double quotes in a row to escape literal quote,
                // so if we see a quote, check if the next character is also a quote.
                let next_c = it.peek().map(|(_, c)| *c).unwrap_or(' ');
                if next_c == '"' {
                    // The this means that there's two quotes in a row, one is an escape, so
                    // skip over the next one.
                    it.next();
                } else {
                    // Otherwise, toggle whether we are in or out of quotes
                    in_quotes = !in_quotes;
                }
            }
        } else {
            entries.push(prune_entry(&line[istart..]));
            break;
        }
    }

    entries
}

#[cfg(test)]
mod tests {
    use super::split_google_sheets_csv_line;

    #[test]
    fn test_google_sheets_split() {
        let line = r#"Normal,"Has ""quotes""",Has 'single quotes',"Where, ""comma, quote""""#;
        let expected = ["Normal", r#"Has "quotes""#, r#"Has 'single quotes'"#, r#"Where, "comma, quote""#];
        let split_vals = split_google_sheets_csv_line(line);
        assert_eq!(split_vals, expected);
    }
}