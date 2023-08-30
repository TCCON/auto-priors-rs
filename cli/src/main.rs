use clap::{self, Parser, Subcommand, CommandFactory, Args};
use clap_complete::Shell;
use clap_verbosity_flag::{Verbosity,InfoLevel};
use dotenv;
use env_logger;
use log::{self, debug};
use orm;
use tccon_priors_cli::config::{self, ConfigActions, ConfigCli};
use tccon_priors_cli::email::{self, EmailActions, EmailCli};
use tccon_priors_cli::input_files::{self, InputFilesActions, InputFilesCli};
use tccon_priors_cli::met_download::{self, MetActions, MetCli};
use tccon_priors_cli::jobs::{self, JobActions, JobCli};
use tccon_priors_cli::siteinfo::{self, StdSiteActions, StdSiteCli};
use tccon_priors_cli::stdsites::{self, StdSiteJobActions, StdSiteJobCli};
use tokio;

use tccon_priors_cli::utils;

#[derive(Debug, Parser)]
#[clap(version)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,

    #[clap(flatten)]
    verbose: Verbosity<InfoLevel>
}

#[derive(Debug, Subcommand)]
enum Commands {
    Met(MetCli),
    Jobs(JobCli),
    InputFiles(InputFilesCli),
    Email(EmailCli),
    SiteInfo(StdSiteCli),
    SiteJobs(StdSiteJobCli),
    Config(ConfigCli),
    Completions(CompletionsCli),

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
    let db_url = orm::get_database_url(None)?;
    let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();

    // The download functions require a downloader object mainly to support mocking in tests; however, in
    // principle we could also build alternate downloaders to support systems where wget isn't available
    // for whatever reason.
    let wget_dl = utils::WgetDownloader::new();

    match args.command {
        Commands::Met(MetCli{ command: MetActions::Check(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            met_download::check_files_for_dates_cli(&mut conn, subargs, &loaded_config).await?;    
        },

        Commands::Met(MetCli{ command: MetActions::DownloadDates(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            met_download::download_files_for_dates_cli(&mut conn, subargs, &loaded_config, wget_dl).await?;
        },

        Commands::Met(MetCli{ command: MetActions::DownloadMissing(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            met_download::download_missing_files_cli(&mut conn, subargs, &loaded_config, wget_dl).await?;
        },

        Commands::Met(MetCli{ command: MetActions::Rescan(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            met_download::rescan_met_files_cli(&mut conn, subargs, &loaded_config).await?;
        },

        Commands::Met(MetCli{ command: MetActions::Report(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            met_download::report_default_met_status_cli(&mut conn, &loaded_config, subargs).await?;
        },

        Commands::Met(MetCli{ command: MetActions::Table(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            met_download::print_met_availability_table_cli(&mut conn, &loaded_config, subargs).await?;
        },

        Commands::Jobs(JobCli { commands: JobActions::Add(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            jobs::add_job(&mut conn, subargs, &loaded_config).await?;
        },

        Commands::Jobs(JobCli { commands: JobActions::Delete(subargs) }) => {
            let mut conn = db.get_connection().await?;
            jobs::delete_job(&mut conn, subargs).await?;
        },

        Commands::Jobs(JobCli { commands: JobActions::CleanErrored(subargs) }) => {
            let mut conn = db.get_connection().await?;
            jobs::clean_errored_jobs_cli(&mut conn, subargs).await?;
        }

        Commands::Jobs(JobCli { commands: JobActions::Print(subargs) }) => {
            let mut conn = db.get_connection().await?;
            jobs::print_jobs_table_cli(&mut conn, subargs).await?;
        },

        Commands::Jobs(JobCli { commands: JobActions::Reset(subargs) }) => {
            let mut conn = db.get_connection().await?;
            jobs::reset_job(&mut conn, subargs).await?;
        },

        Commands::InputFiles(InputFilesCli { commands: InputFilesActions::Parse(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            input_files::add_jobs_from_input_files_cli(&mut conn, subargs, &loaded_config).await?; 
        },

        Commands::Email( EmailCli { commands: EmailActions::Submitters(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            email::email_past_job_submitters_cli(&mut conn, &loaded_config, subargs).await?;
        },

        Commands::Email( EmailCli { commands: EmailActions::CurrentJobs(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            email::email_current_jobs_cli(&mut conn, &loaded_config, subargs).await?;
        },

        Commands::Email( EmailCli { commands: EmailActions::PastJobs(subargs) }) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            email::email_completed_jobs_cli(&mut conn, &loaded_config, subargs).await?;
        },

        Commands::SiteInfo(StdSiteCli { command: StdSiteActions::AddSite(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::add_new_std_site_cli(&mut conn, subargs).await?;
        },

        Commands::SiteInfo(StdSiteCli { command: StdSiteActions::Edit(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::edit_std_site_cli(&mut conn, subargs).await?;
        },

        Commands::SiteInfo(StdSiteCli { command: StdSiteActions::Print(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::print_sites_cli(&mut conn, subargs).await?;
        },

        Commands::SiteInfo(StdSiteCli { command: StdSiteActions::AddInfo(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::add_std_site_info_range_cli(&mut conn, subargs).await?;
        },

        Commands::SiteInfo(StdSiteCli { command: StdSiteActions::SetNonop(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::clear_site_info_range_cli(&mut conn, subargs).await?;
        },

        Commands::SiteInfo(StdSiteCli { command: StdSiteActions::DeleteInfo(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::delete_info_row_cli(&mut conn, subargs).await?;
        },

        Commands::SiteInfo(StdSiteCli { command: StdSiteActions::PrintInfo(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::print_locations_for_site_cli(&mut conn, subargs).await?;
        },

        Commands::SiteJobs( StdSiteJobCli { command: StdSiteJobActions::UpdateTable(subargs) } ) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            stdsites::update_std_site_job_table_cli(&mut conn, &loaded_config, subargs).await?;
        },

        Commands::SiteJobs( StdSiteJobCli { command: StdSiteJobActions::AddJobs } ) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            stdsites::add_jobs_for_pending_rows(&mut conn, &loaded_config).await?;
        },

        Commands::SiteJobs( StdSiteJobCli { command: StdSiteJobActions::TarFiles } ) => {
            let mut conn = db.get_connection().await?;
            let loaded_config = load_config()?;
            stdsites::make_std_site_tarballs(&mut conn, &loaded_config).await?;
        },

        Commands::SiteJobs( StdSiteJobCli { command: StdSiteJobActions::FlagForRegen(subargs) }) => {
            let mut conn = db.get_connection().await?;
            stdsites::flag_for_regen_cli(&mut conn, subargs).await?;
        },

        Commands::SiteJobs( StdSiteJobCli { command: StdSiteJobActions::Print(subargs) }) => {
            let mut conn = db.get_connection().await?;
            stdsites::print_std_jobs_summary_cli(&mut conn, subargs).await?;
        },

        Commands::SiteInfo( StdSiteCli { command: StdSiteActions::Json(subargs) }) => {
            let mut conn = db.get_connection().await?;
            siteinfo::site_info_json(&mut conn, &subargs).await?;
        },

        Commands::Config(ConfigCli { command: ConfigActions::Generate(subargs)}) => {
            config::generate_config_file(subargs)?;
        },

        Commands::Config(ConfigCli { command: ConfigActions::Debug }) => {
            let loaded_config = load_config()?;
            config::debug_config(loaded_config);
        },

        Commands::Completions(CompletionsCli { commands: CompletionsActions::Generate(subargs) }) => {
            generate_shell_completions(subargs.shell);
        }
    };

    Ok(())
}

fn load_config() -> anyhow::Result<orm::config::Config> {
    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    if let Some(cf) = &config_file {
        debug!("Loading configuration from {}", cf.to_string_lossy());
    } else {
        debug!("Will use default config");
    }
    orm::config::load_config_file_or_default(config_file)
}

#[derive(Debug, Args)]
struct CompletionsCli {
    #[clap(subcommand)]
    commands: CompletionsActions
}

#[derive(Debug, Subcommand)]
enum CompletionsActions {
    Generate(GenCompletionsCli)
}

#[derive(Debug, Args)]
/// Generate completions for a shell, printing to stdout
struct GenCompletionsCli {
    /// Which shell to generate for, options are "bash", "elvish", "fish",
    /// "powershell", and "zsh"
    shell: Shell
}

fn generate_shell_completions(shell: Shell) {
    let mut tmp = Cli::into_app();
    clap_complete::generate(shell, &mut tmp, "tccon-priors-cli", &mut std::io::stdout())
}