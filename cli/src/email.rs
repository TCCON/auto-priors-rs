use std::{path::PathBuf, io::Read};

use anyhow::Context;
use chrono::NaiveDate;
use clap::{Args, Subcommand};
use orm::{MySqlConn, config::Config, jobs::Job};


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
        file.read_to_string(&mut buf).context("Error occurred while trying to read the --body-file")?;
        buf
    } else {
        anyhow::bail!("Must give one of --body or --body-file");
    };

    email_past_job_submitters(conn, config, &args.to, &args.subject, &body).await
}

pub async fn email_past_job_submitters(conn: &mut MySqlConn, config: &Config, to: &str, subject: &str, body: &str) -> anyhow::Result<()> {
    let mut emails = Job::get_distinct_submitter_emails(conn).await?;
    for extra_addr in config.email.extra_submitters.iter() {
        let extra_addr = extra_addr.to_string();
        if !emails.contains(&extra_addr) {
            emails.push(extra_addr.to_string());
        }
    }

    let emails_ref: Vec<_> = emails.iter().map(|e| e.as_str()).collect();
    config.email.send_mail(
        &[to],
        None,
        Some(&emails_ref),
        subject,
        body
    )?;
    Ok(())
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