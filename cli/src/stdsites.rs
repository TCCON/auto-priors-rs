use clap::{self, Parser, Subcommand, Args};
use orm::{stdsitejobs,MySqlConn};

#[derive(Debug, Args)]
pub struct StdSiteJobCli {
    #[clap(subcommand)]
    pub command: Actions
}

#[derive(Debug, Subcommand)]
pub enum Actions {
    AddJobs,
    TarFiles
}

pub async fn standard_site_driver(conn: &mut MySqlConn, args: StdSiteJobCli, config: &orm::config::Config) -> anyhow::Result<()> {
    match args.command {
        Actions::AddJobs => add_standard_site_jobs_from_geos(conn, config).await,
        Actions::TarFiles => todo!()
    }
}

async fn add_standard_site_jobs_from_geos(conn: &mut MySqlConn, config: &orm::config::Config) -> anyhow::Result<()> {
    stdsitejobs::StdSiteJob::add_new_std_jobs_up_to_date(
        conn, 
        None, 
        &config.execution.std_sites_output_base)
    .await?;
    Ok(())
}