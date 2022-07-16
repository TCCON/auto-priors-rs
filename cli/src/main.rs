use std::path::PathBuf;

// TODOs:
//  * Commands:
//      - Status of jobs
//      - Cancel pending job(s)
//  * Update GEOS file database
//  * Download new GEOS files
//  * Check for missing GEOS files
//  * Read job input files
//  * Add job
//  * Delete expired output files
//  * Reset running jobs to pending (in case of crash)
//  * Standard sites:
//      - Add jobs for missing standard sites
//      - Scan for failed standard site jobs
//      - Proper backfilling (both forced and based on updated site dates)
//      - Make tarballs
use clap::{self, Parser, Subcommand, Args};
use clap_verbosity_flag::{Verbosity,InfoLevel};
use dotenv;
use env_logger;
use log::{self, debug};
use orm;
use tokio;

mod jobs;
mod input_files;
mod siteinfo;
mod stdsites;

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,

    #[clap(flatten)]
    verbose: Verbosity<InfoLevel>
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[clap(alias="pifm")]
    ParseInputFilesManually(input_files::ParseInputFilesManualCli),
    AddJob(jobs::AddJobCli),
    DeleteJob(jobs::DeleteJobCli),
    StdSites(stdsites::StdSiteJobCli),
    SiteInfoJson(siteinfo::InfoJsonCli),
    GenConfig(GenConfigCli)
}

#[derive(Debug, Args)]
/// Generate a default configuration file from the command line
struct GenConfigCli {
    /// Path to write the default TOML file as.
    path: PathBuf
}

fn generate_config_file(clargs: GenConfigCli) -> anyhow::Result<()> {
    orm::config::generate_config_file(&clargs.path)
}

// Had to change rust-analyzer settings as described in https://github.com/rust-lang/rust-analyzer/issues/12450
// to have it recognize this macro.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv()?;
    let args = Cli::parse();

    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    let config = orm::config::load_config_file_or_default(config_file);

    let log_level = args.verbose.log_level_filter();

    // Need to filter modules to avoid messages from sqlx. Not sure yet if log messages from submodules of
    // tccon_priors_orm will respect this. Note: it *needs* the name specified in Cargo.toml, not how we
    // refer to it in the code (though with dashes replaced with underscores).
    env_logger::Builder::from_default_env()
        .filter(Some("tccon_priors_orm"), log_level)
        .filter(Some("tccon_priors_cli"), log_level)
        .init();

    debug!("Log level set to DEBUG");
    let mut db = orm::get_database_pool(None).await.unwrap();

    match args.command {
        Commands::ParseInputFilesManually(subargs) => {
            let mut conn = db.acquire().await?;
            input_files::add_jobs_from_input_files(&mut conn, subargs, &config).await?; 
        }

        Commands::AddJob(subargs) => {
            jobs::add_job(&mut db.acquire().await?, subargs).await?;
        },

        Commands::DeleteJob(subargs) => {jobs::delete_job(
            &mut db, subargs).await?
        },

        Commands::StdSites(subargs) => {stdsites::standard_site_driver(
            &mut db, subargs, &config).await?
        },

        Commands::SiteInfoJson(subargs) => {
            siteinfo::site_info_json(&mut db.acquire().await?, &subargs).await?;
        },

        Commands::GenConfig(subargs) => {
            generate_config_file(subargs)?;
        }
    };

    Ok(())
}