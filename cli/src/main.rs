// TODOs:
//  * Commands:
//      - Status of jobs
//      - Cancel pending job(s)
//  * Update GEOS file database
//  * Download new GEOS files
//  * Check for missing GEOS files
//  * Delete expired output files
//  * Reset running jobs to pending (in case of crash)
//  * Standard sites:
//      - Scan for failed standard site jobs
//      - Proper backfilling (both forced and based on updated site dates)
//      - Make tarballs
//      - Create a new standard site/update an existing one?
use clap::{self, Parser, Subcommand, Args};
use clap_verbosity_flag::{Verbosity,InfoLevel};
use dotenv;
use env_logger;
use log::{self, debug};
use orm;
use orm::MySqlConn;
use tccon_priors_cli::config::ConfigActions;
use tccon_priors_cli::config::ConfigCli;
use tccon_priors_cli::siteinfo::StdSiteActions;
use tccon_priors_cli::siteinfo::StdSiteCli;
use tokio;

use tccon_priors_cli::utils;
use tccon_priors_cli::config;
use tccon_priors_cli::met_download;
use tccon_priors_cli::jobs;
use tccon_priors_cli::input_files;
use tccon_priors_cli::siteinfo;
use tccon_priors_cli::stdsites;

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,

    #[clap(flatten)]
    verbose: Verbosity<InfoLevel>
}

#[derive(Debug, Subcommand)]
enum Commands {
    MigrateDb(MigrateCli),
    UnmigrateDb(UnmigrateCli),
    CheckMet(met_download::CheckDatesCli),
    #[clap(alias="drbd")]
    DownloadReanalysisByDates(met_download::DownloadDatesCli),
    #[clap(alias="dmr")]
    DownloadMissingReanalysis(met_download::DownloadMissingCli),
    RescanMet(met_download::RescanMetCli),
    #[clap(alias="pifm")]
    ParseInputFilesManually(input_files::ParseInputFilesManualCli),
    AddJob(jobs::AddJobCli),
    DeleteJob(jobs::DeleteJobCli),
    StdSites(siteinfo::StdSiteCli),
    #[clap(alias="ssj")]
    StdSiteJobs(stdsites::StdSiteJobCli),
    SiteInfoJson(siteinfo::InfoJsonCli),
    Config(config::ConfigCli)
}

#[derive(Debug, Args)]
pub struct MigrateCli {
    /// Set this flag to skip the interactive verification
    #[clap(short = 'y', long = "yes")]
    yes: bool
}

#[derive(Debug, Args)]
pub struct UnmigrateCli {
    /// Set this flag to skip the interactive verification
    yes: bool,
    /// Use this to determine which is the earliest migration to revert to
    target: Option<i64>
}

async fn run_migrations(conn: &mut MySqlConn, db_url: &str, yes: bool) -> anyhow::Result<()> {
    // Only print the name of the database (assuming it's the part after the last slash)
    // so as not to expose passwords if they are in the URL.
    let db_name = db_url.split('/').last().unwrap_or("UNKNOWN (could not determine database name from url)");
    
    if !yes {
        println!("Apply any pending migrations to {db_name}?");
        let ans = tccon_priors_cli::get_user_input("[y/N]: ")?;

        if ans.to_ascii_lowercase() != "y" {
            println!("Aborting migrations");
            return Ok(())
        }
    }

    println!("Applying any pending migrations to {db_name}");
    orm::apply_migrations(conn).await
}

async fn undo_migrations(conn: &mut MySqlConn, db_url: &str, yes: bool, target: Option<i64>) -> anyhow::Result<()> {
    // Only print the name of the database (assuming it's the part after the last slash)
    // so as not to expose passwords if they are in the URL.
    let db_name = db_url.split('/').last().unwrap_or("UNKNOWN (could not determine database name from url)");
    
    if !yes {
        println!("Undo migrations in {db_name}?");
        let ans = tccon_priors_cli::get_user_input("[y/N]: ")?;

        if ans.to_ascii_lowercase() != "y" {
            println!("Aborting migration undo");
            return Ok(())
        }
    }

    println!("Undoing migrations in {db_name}");
    orm::unapply_migrations(conn, target.unwrap_or(0)).await
}

// Had to change rust-analyzer settings as described in https://github.com/rust-lang/rust-analyzer/issues/12450
// to have it recognize this macro.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv()?;
    let args = Cli::parse();
    let log_level = args.verbose.log_level_filter();

    // Need to filter modules to avoid messages from sqlx. Not sure yet if log messages from submodules of
    // tccon_priors_orm will respect this. Note: it *needs* the name specified in Cargo.toml, not how we
    // refer to it in the code (though with dashes replaced with underscores).
    env_logger::Builder::from_default_env()
        .filter(Some("tccon_priors_orm"), log_level)
        .filter(Some("tccon_priors_cli"), log_level)
        .init();

    debug!("Log level set to DEBUG");
    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    let config = orm::config::load_config_file_or_default(config_file)?;
    let db_url = orm::get_database_url(None)?;
    let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();

    // The download functions require a downloader object mainly to support mocking in tests; however, in
    // principle we could also build alternate downloaders to support systems where wget isn't available
    // for whatever reason.
    let wget_dl = utils::WgetDownloader::new();

    match args.command {
        Commands::MigrateDb(subargs) => {
            let mut conn = db.get_connection().await?;
            run_migrations(&mut conn, &db_url, subargs.yes).await?;
        }

        Commands::UnmigrateDb(subargs) => {
            let mut conn = db.get_connection().await?;
            undo_migrations(&mut conn, &db_url, subargs.yes, subargs.target).await?;
        }

        Commands::CheckMet(subargs) => {
            let mut conn = db.get_connection().await?;
            met_download::check_files_for_dates_cli(&mut conn, subargs, &config).await?;
        },

        Commands::DownloadReanalysisByDates(subargs) => {
            let mut conn = db.get_connection().await?;
            met_download::download_files_for_dates_cli(&mut conn, subargs, &config, wget_dl).await?;
        },

        Commands::DownloadMissingReanalysis(subargs) => {
            let mut conn = db.get_connection().await?;
            met_download::download_missing_files_cli(&mut conn, subargs, &config, wget_dl).await?;
        }

        Commands::RescanMet(subargs) => {
            let mut conn = db.get_connection().await?;
            met_download::rescan_met_files_cli(&mut conn, subargs, &config).await?;
        }

        Commands::ParseInputFilesManually(subargs) => {
            let mut conn = db.get_connection().await?;
            input_files::add_jobs_from_input_files_cli(&mut conn, subargs, &config).await?; 
        },

        Commands::AddJob(subargs) => {
            let mut conn = db.get_connection().await?;
            jobs::add_job(&mut conn, subargs).await?;
        },

        Commands::DeleteJob(subargs) => {
            let mut conn = db.get_connection().await?;
            jobs::delete_job(&mut conn, subargs).await?
        },


        Commands::StdSites(StdSiteCli { command: StdSiteActions::AddSite(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::add_new_std_site_cli(&mut conn, subargs).await?;
        }

        Commands::StdSites(StdSiteCli { command: StdSiteActions::EditSite(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::edit_std_site_cli(&mut conn, subargs).await?;
        }

        Commands::StdSites(StdSiteCli { command: StdSiteActions::AddInfo(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::add_std_site_info_range_cli(&mut conn, subargs).await?;
        }

        Commands::StdSites(StdSiteCli { command: StdSiteActions::PrintInfo(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::print_locations_for_site_cli(&mut conn, subargs).await?;
        }

        Commands::StdSiteJobs(subargs) => {
            let mut conn = db.get_connection().await?;
            stdsites::standard_site_driver(&mut conn, subargs, &config).await?
        },

        Commands::SiteInfoJson(subargs) => {
            let mut conn = db.get_connection().await?;
            siteinfo::site_info_json(&mut conn, &subargs).await?;
        },

        Commands::Config(ConfigCli { command: ConfigActions::GenConfig(subargs)}) => {
            config::generate_config_file(subargs)?;
        },

        Commands::Config(ConfigCli { command: ConfigActions::DebugConfig }) => {
            config::debug_config(config);
        }
    };

    Ok(())
}