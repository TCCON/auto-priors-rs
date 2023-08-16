use chrono::NaiveDate;
use clap::{self, Subcommand, Args};
use orm::{stdsitejobs,MySqlConn};

/// Manage jobs for the standard sites
#[derive(Debug, Args)]
pub struct StdSiteJobCli {
    #[clap(subcommand)]
    pub command: StdSiteJobActions
}

#[derive(Debug, Subcommand)]
pub enum StdSiteJobActions {
    UpdateTable(UpdateTableCli),

    /// Add jobs to generate standard sites' priors for days in need of priors
    /// for which met data is available.
    AddJobs,

    /// Collect completed standard site jobs outputs into the standard sites'
    /// tar files.
    TarFiles
}

/// Update the standard site jobs table: add rows for new site-days possible 
#[derive(Debug, Args)]
pub struct UpdateTableCli {
    #[clap(short='b', long)]
    not_before: Option<NaiveDate>
}

pub async fn update_std_site_job_table_cli(conn: &mut MySqlConn, config: &orm::config::Config, args: UpdateTableCli) -> anyhow::Result<()> {
    update_std_site_job_table(conn, config, args.not_before).await
}

pub async fn update_std_site_job_table(conn: &mut MySqlConn, config: &orm::config::Config, not_before: Option<NaiveDate>) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::update_std_site_job_table(
        conn, 
        config,
        not_before
    ).await?;
    Ok(())
}

pub async fn add_jobs_for_pending_rows(conn: &mut MySqlConn, config: &orm::config::Config) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::add_jobs_for_pending_rows(conn, config).await
}

pub async fn make_std_site_tarballs(conn: &mut MySqlConn, config: &orm::config::Config) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::make_standard_site_tarballs(conn, config).await
}