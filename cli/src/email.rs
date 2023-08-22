use std::{path::PathBuf, io::Read};

use anyhow::Context;
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
    Submitters(EmailSubmittersCli)
}


/// Send an email to anyone who has previously submitted a job
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
    let emails = Job::get_distinct_submitter_emails(conn).await?;
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