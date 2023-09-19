use std::path::PathBuf;

use clap::{self, Args, Subcommand};
use orm::MySqlConn;

/// Manage job input files
#[derive(Debug, Args)]
pub struct InputFilesCli {
    #[clap(subcommand)]
    pub commands: InputFilesActions
}

#[derive(Debug, Subcommand)]
pub enum InputFilesActions {
    /// Manually parse specific input files.
    Parse(ParseInputFilesManualCli)
}

#[derive(Debug, Args)]
/// Parse input files specified on the command line.  This does not parse all input files
/// matching the pattern specified in the config, it only parses those files listed as 
/// arguments here.
pub struct ParseInputFilesManualCli {
    /// Paths to input files to parse. 
    input_files: Vec<PathBuf>
}


pub async fn add_jobs_from_input_files_cli(conn: &mut MySqlConn, clargs: ParseInputFilesManualCli, config: &orm::config::Config) -> anyhow::Result<()> {
    let mut mover = orm::input_files::InputFileCleanupHandler::new();
    orm::input_files::add_jobs_from_input_files(conn, config, &clargs.input_files, &config.execution.output_path, &mut mover).await
}