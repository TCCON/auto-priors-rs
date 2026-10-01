use std::{io::Read, path::PathBuf};

use anyhow::Context;
use chrono::NaiveDate;
use clap::{Args, Subcommand};
use itertools::Itertools;
use log::{info, warn};
use orm::{
    config::Config,
    email::SendMail,
    jobs::Job,
    siteinfo::request_form::{get_request_csv_data, RequestIter, RequestRow},
    MySqlConn,
};
use regex::Regex;
use std::sync::OnceLock;

use crate::shared_cli;

static FORM_SPLIT_RE: OnceLock<Regex> = OnceLock::new();

/// Send bulk emails about the priors
#[derive(Debug, Args)]
pub struct EmailCli {
    #[clap(subcommand)]
    pub commands: EmailActions,
}

#[derive(Debug, Subcommand)]
pub enum EmailActions {
    /// Send an email to anyone who has previously submitted a job
    Submitters(EmailSubmittersCli),
    /// Print a list of emails who have previously submitted a job
    PrintSubs(PrintSubmittersCli),
    /// Send an email summarizing current jobs
    CurrentJobs(CurrentJobsReportCli),
    /// Send an email about previously finished jobs
    PastJobs(CompletedJobsReportCli),
    /// Send an email summarizing new standard site requests
    StdSiteReq(StdSiteRequestCli),
    /// Send a test email
    TestEmail(TestEmailCli),
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
    #[clap(short = 's', long)]
    subject: String,

    /// The body of the email. For longer emails, you can use the --body-file argument instead.
    #[clap(short = 'b', long)]
    body: Option<String>,

    /// Path to a file containing the body of the email. For short emails, you can use --body instead.
    #[clap(short = 'f', long)]
    body_file: Option<PathBuf>,

    /// By default, if --body-file is used for the body, then it will be softwrapped, meaning individual
    /// newlines are removed and multiple consecutive newlines are reduced to 2. This makes the email
    /// body look nicer in viewers that do softwrapping. Use this flag to disable that and keep all
    /// newlines.
    #[clap(short = 'n', long)]
    keep_newlines: bool,

    /// If given, then the emails will be sent out in batches of this size. This may help with email
    /// providers that block mass emails.
    #[clap(short = 'l', long)]
    batch_size: Option<usize>,

    /// This determines how many seconds to wait between batches. This may also help with email providers
    /// that block mass emails.
    #[clap(short = 'p', long, default_value_t = 0)]
    batch_pause_seconds: u64,

    /// By default, any email now on the blacklist will not
    /// be included in the list. Pass this flag to include
    /// them.
    #[clap(short = 'k', long)]
    keep_blacklisted: bool,

    /// Use this argument to give a domain to exclude, emails that
    /// end in this substring will not be printed. Specify multiple
    /// times for multiple domains.
    #[clap(short = 'e', long)]
    exclude_domain: Vec<String>,

    /// Use a mock email backend rather that the configured one.
    #[clap(short = 'd', long)]
    dry_run: bool,
}

pub async fn email_past_job_submitters_cli(
    conn: &mut MySqlConn,
    config: &Config,
    args: EmailSubmittersCli,
) -> anyhow::Result<()> {
    if args.body.is_some() && args.body_file.is_some() {
        anyhow::bail!("--body and --body-file are mutually exclusive");
    }

    let body = if let Some(b) = &args.body {
        b.to_string()
    } else if let Some(path) = &args.body_file {
        let mut file =
            std::fs::File::open(path).context("Error occurred trying to open the --body-file")?;
        let mut buf = String::new();
        if args.keep_newlines {
            file.read_to_string(&mut buf)
                .context("Error occurred while trying to read the --body-file")?;
        } else {
            orm::utils::softwrap(std::io::BufReader::new(file), &mut buf)?;
        }
        buf
    } else {
        anyhow::bail!("Must give one of --body or --body-file");
    };

    let excluded_domains = args.exclude_domain.iter().map(|s| s.as_str()).collect_vec();
    email_past_job_submitters(
        conn,
        config,
        &args.to,
        &args.subject,
        &body,
        args.batch_size,
        args.batch_pause_seconds,
        &excluded_domains,
        args.keep_blacklisted,
        args.dry_run,
    )
    .await
}

pub async fn email_past_job_submitters(
    conn: &mut MySqlConn,
    config: &Config,
    to: &str,
    subject: &str,
    body: &str,
    batch_size: Option<usize>,
    batch_pause_seconds: u64,
    exclude_domains: &[&str],
    keep_blacklisted: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    let emails = make_submitter_email_list(conn, config, keep_blacklisted, exclude_domains).await?;
    let batch_size = batch_size.unwrap_or_else(|| emails.len());
    let n_total = emails.len();
    let mut n_sent = 0;

    let emails_iter = emails.iter().map(|e| e.as_str()).chunks(batch_size);
    for email_batch in emails_iter.into_iter() {
        let emails_ref = email_batch.collect_vec();
        if dry_run {
            let mock = orm::email::MockEmail {};
            mock.send_mail(
                &[to],
                &config.email.from_address.to_string(),
                None,
                Some(&emails_ref),
                subject,
                body,
            )?;
        } else {
            config
                .email
                .send_mail(&[to], None, Some(&emails_ref), subject, body)?;
        }

        n_sent += emails_ref.len();
        info!(
            "Sent emails to {n_sent} of {n_total} addresses so far, {} more to go",
            n_total - n_sent
        );
        if batch_pause_seconds > 0 {
            info!("Waiting {batch_pause_seconds} before sending next group of emails");
            tokio::time::sleep(std::time::Duration::from_secs(batch_pause_seconds)).await;
        }
    }
    Ok(())
}

/// Print the list of people who have submitted.
#[derive(Debug, Args)]
pub struct PrintSubmittersCli {
    /// By default, the emails will be printed one per line.
    /// Use this argument to specify an alternate separator to
    /// use instead of a newline.
    #[clap(short = 's', long, default_value = "\n")]
    separator: String,

    /// By default, any email now on the blacklist will not
    /// be included in the list. Pass this flag to include
    /// them.
    #[clap(short = 'k', long)]
    keep_blacklisted: bool,

    /// Use this argument to give a domain to exclude, emails that
    /// end in this substring will not be printed. Specify multiple
    /// times for multiple domains.
    #[clap(short = 'e', long)]
    exclude_domain: Vec<String>,
}

pub async fn print_submitters_emails_cli(
    conn: &mut MySqlConn,
    config: &Config,
    args: PrintSubmittersCli,
) -> anyhow::Result<()> {
    let exclude_domains = args.exclude_domain.iter().map(|s| s.as_ref()).collect_vec();
    print_submitters_emails(
        conn,
        config,
        &args.separator,
        args.keep_blacklisted,
        &exclude_domains,
    )
    .await
}

pub async fn print_submitters_emails(
    conn: &mut MySqlConn,
    config: &Config,
    separator: &str,
    keep_blacklisted: bool,
    exclude_domains: &[&str],
) -> anyhow::Result<()> {
    let mut emails =
        make_submitter_email_list(conn, config, keep_blacklisted, exclude_domains).await?;

    let n_blacklisted = if !keep_blacklisted {
        let n_init = emails.len();
        for entry in &config.blacklist {
            match entry.identifier {
                orm::config::BlacklistIdentifier::SubmitterEmail { ref submitter } => {
                    emails.retain(|s| s != submitter);
                }
            }
        }
        n_init - emails.len()
    } else {
        0
    };

    let n_excluded = if !exclude_domains.is_empty() {
        let n_init = emails.len();
        emails.retain(|s| {
            for dom in exclude_domains {
                if s.ends_with(dom) {
                    return false;
                }
            }
            return true;
        });
        n_init - emails.len()
    } else {
        0
    };

    let to_print = emails.join(separator);
    println!("{to_print}");
    if n_blacklisted > 0 {
        eprintln!("{n_blacklisted} emails removed because they were on the blacklist.");
    }
    if n_excluded > 0 {
        eprintln!("{n_excluded} emails removed because they matched one of the excluded domains.");
    }
    Ok(())
}

async fn make_submitter_email_list(
    conn: &mut MySqlConn,
    config: &Config,
    keep_blacklisted: bool,
    exclude_domains: &[&str],
) -> anyhow::Result<Vec<String>> {
    let mut emails = Job::get_distinct_submitter_emails(conn)
        .await?
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
        })
        .collect_vec();

    for extra_addr in config.email.extra_submitters.iter() {
        emails.push(extra_addr.to_string());
    }

    emails.dedup();

    let n_blacklisted = if !keep_blacklisted {
        let n_init = emails.len();
        for entry in &config.blacklist {
            match entry.identifier {
                orm::config::BlacklistIdentifier::SubmitterEmail { ref submitter } => {
                    emails.retain(|s| s != submitter);
                }
            }
        }
        n_init - emails.len()
    } else {
        0
    };

    let n_excluded = if !exclude_domains.is_empty() {
        let n_init = emails.len();
        emails.retain(|s| {
            for dom in exclude_domains {
                if s.ends_with(dom) {
                    return false;
                }
            }
            return true;
        });
        n_init - emails.len()
    } else {
        0
    };

    if n_blacklisted > 0 {
        info!("{n_blacklisted} emails removed because they were on the blacklist.");
    }
    if n_excluded > 0 {
        info!("{n_excluded} emails removed because they matched one of the excluded domains.");
    }

    emails.sort_unstable();

    Ok(emails)
}

/// Send an email reporting on pending and running jobs
#[derive(Debug, Args)]
pub struct CurrentJobsReportCli {
    /// To whom to send the email report. May give multiple emails as separate arguments,
    /// if none are given, the admins will be emailed.
    to: Vec<String>,
}

pub async fn email_current_jobs_cli(
    conn: &mut MySqlConn,
    config: &Config,
    args: CurrentJobsReportCli,
) -> anyhow::Result<()> {
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
    to: Vec<String>,
}

pub async fn email_completed_jobs_cli(
    conn: &mut MySqlConn,
    config: &Config,
    args: CompletedJobsReportCli,
) -> anyhow::Result<()> {
    let to = if args.to.is_empty() {
        config.email.admin_emails_string_list()
    } else {
        args.to
    };

    let to: Vec<_> = to.iter().map(|s| s.as_str()).collect();
    orm::email::email_completed_jobs(conn, config, &to, args.start_date, args.end_date).await
}

#[derive(Debug, Args)]
pub struct TestEmailCli {
    /// Email address to send the test email to
    to: String,
}

pub fn send_test_email_cli(config: &Config, args: TestEmailCli) -> anyhow::Result<()> {
    config.email.send_mail(
        &[&args.to],
        None,
        None,
        "AutoMod test email",
        "This is a test email from the Rust priors generation system",
    )?;
    Ok(())
}

#[derive(Debug, Args)]
pub struct StdSiteRequestCli {
    /// Emails to send the requests to. If none given, then will send to the emails in the
    /// configuration under [email.std_site_req_emails].
    to: Vec<String>,

    #[clap(flatten)]
    csv_src: shared_cli::SiteRequetFormComponent,

    /// Whether to send the emails or only send mock emails.
    #[clap(short = 'd', long)]
    dry_run: bool,
}

pub async fn email_std_site_request_info_cli(
    conn: &mut MySqlConn,
    config: &Config,
    args: StdSiteRequestCli,
) -> anyhow::Result<()> {
    let to_emails = if !args.to.is_empty() {
        args.to
    } else if let Some(to) = &config.email.std_site_req_emails {
        to.iter().map(|addr| addr.to_string()).collect_vec()
    } else {
        anyhow::bail!("Must provide emails to send to by command line or configuration")
    };

    let csv_data = get_request_csv_data(
        config,
        args.csv_src.request_csv.as_deref(),
        args.csv_src.sheet_id.as_deref(),
    )
    .await?.ok_or_else(|| anyhow::anyhow!("Must provide one of the following: --request-csv, --sheet-id, or the std_site_req_sheet_id entry in the [email] section of the config"))?;
    let to_emails = to_emails.iter().map(|s| s.as_str()).collect_vec();
    email_std_site_request_info(conn, config, &csv_data, &to_emails, args.dry_run).await
    // debug_csv(&args.request_csv);
    // Ok(())
}

pub async fn email_std_site_request_info(
    conn: &mut MySqlConn,
    config: &Config,
    request_csv_data: &str,
    to: &[&str],
    dry_run: bool,
) -> anyhow::Result<()> {
    // Get the currently defined standard sites so we can check for conflicts in the site IDs
    let site_id_to_name = orm::siteinfo::StdSite::get_site_id_map_to_name(conn, None)
        .await
        .context("Error while getting the list of existing site IDs")?;

    let csv_iter = RequestIter::new(request_csv_data, true);
    for result in csv_iter {
        // The iterator will only return undecided requests.
        let row: RequestRow = result?;
        let (n_by_email, n_by_sids) = count_jobs_for_request(
            conn,
            row.custom_loc_email.as_deref(),
            row.custom_loc_sids.as_deref(),
        )
        .await?;
        let mut body = format!("{row}\nFrom the database, found {n_by_email} jobs under the custom location request email(s) and of those {n_by_sids} contained the custom location site ID(s)");
        if let Some(conflicting_site_name) = site_id_to_name.get(&row.desired_site_id) {
            body.push_str(&format!("\nWARNING: requested site ID ({}) conflicts with existing standard site ({conflicting_site_name})", row.desired_site_id));
        }
        let subject = "Standard site priors request summary";

        if dry_run {
            let mock = orm::email::MockEmail {};
            mock.send_mail(
                to,
                &config.email.from_address.to_string(),
                None,
                None,
                subject,
                &body,
            )?;
        } else {
            config.email.send_mail(
                to,
                None,
                None,
                "Standard site priors request summary",
                &body,
            )?;
        }
    }

    Ok(())
}

async fn count_jobs_for_request(
    conn: &mut MySqlConn,
    emails: Option<&str>,
    site_ids: Option<&str>,
) -> anyhow::Result<(usize, usize)> {
    let re = FORM_SPLIT_RE.get_or_init(|| Regex::new(r"\s*[,\s]\s*").unwrap());

    // Handle email filtering first. We're making our best guess that if users enter >1 email and/or site ID
    // they'll be separated by spaces maybe with a comma in there.
    let jobs = if let Some(emails) = emails {
        let mut all_jobs = vec![];
        for addr in re.split(emails) {
            let addr_jobs = orm::jobs::Job::get_jobs_for_user(conn, addr, None, None).await?;
            all_jobs.extend(addr_jobs);
        }
        all_jobs
    } else {
        orm::jobs::Job::get_jobs_list(conn, false).await?
    };
    let n_by_emails = jobs.len();

    let n_by_sids = if let Some(site_ids) = site_ids {
        let site_ids = re.split(site_ids).collect_vec();
        jobs.into_iter()
            .filter(|j| j.site_id.iter().any(|sid| site_ids.contains(&sid.as_str())))
            .count()
    } else {
        jobs.len()
    };
    Ok((n_by_emails, n_by_sids))
}
