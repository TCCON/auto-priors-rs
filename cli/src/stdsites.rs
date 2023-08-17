use chrono::{NaiveDate, Duration};
use clap::{self, Subcommand, Args};
use orm::{stdsitejobs,MySqlConn, utils::DateIterator, siteinfo::StdSite};

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
    TarFiles,

    /// Flag a range of dates for standard priors regeneration, either for
    /// all sites or a subset
    FlagForRegen(FlagForRegenCli),
}

/// Update the standard site jobs table: add rows for new site-days possible 
#[derive(Debug, Args)]
pub struct UpdateTableCli {
    #[clap(short='b', long)]
    not_before: Option<NaiveDate>
}

#[derive(Debug, Args)]
pub struct FlagForRegenCli {
    /// Site ID to flag. Can provide this argument multiple times to flag multiple
    /// sites. Either this or --all-sites is required, but cannot have both.
    #[clap(short='s', long)]
    site_id: Vec<String>,

    /// Flag all sites for regen. Either this or --site-id is required, but cannot have both.
    #[clap(long)]
    all_sites: bool,

    /// First date to flag
    start_date: NaiveDate,

    /// Date after the last date to flag; if not given, only START_DATE is flagged
    end_date: Option<NaiveDate>
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


pub enum SitesToFlag {
    Some(Vec<String>),
    All
}

pub async fn flag_for_regen_cli(conn: &mut MySqlConn, args: FlagForRegenCli) -> anyhow::Result<()> {
    let sites = if args.all_sites && !args.site_id.is_empty() {
        anyhow::bail!("--all-sites and --site-id are mutually exclusive");
    } else if args.all_sites {
        SitesToFlag::All
    } else {
        SitesToFlag::Some(args.site_id)
    };

    flag_for_regen(conn, args.start_date, args.end_date, sites).await
}

pub async fn flag_for_regen(conn: &mut MySqlConn, start_date: NaiveDate, end_date: Option<NaiveDate>, sites: SitesToFlag) -> anyhow::Result<()> {
    let end_date = end_date.unwrap_or_else(|| start_date + Duration::days(1));
    let site_ids = if let SitesToFlag::Some(sids) = sites {
        sids
    } else {
        StdSite::get_site_ids(conn, None).await?
    };

    for sid in site_ids {
        stdsitejobs::StdSiteJob::set_regen_flag(conn, &sid, start_date, Some(end_date)).await?;
    }

    Ok(())
}