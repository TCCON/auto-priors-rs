use std::path::PathBuf;

use clap::{self, Args};
use orm::MySqlConn;


#[derive(Debug, Args)]
/// Parse input files specified on the command line [alias: pifm]
pub struct ParseInputFilesManualCli {
    /// Paths to input files to parse. 
    input_files: Vec<PathBuf>
}


pub async fn add_jobs_from_input_files_cli(conn: &mut MySqlConn, clargs: ParseInputFilesManualCli, config: &orm::config::Config) -> anyhow::Result<()> {
    orm::input_files::add_jobs_from_input_files(conn, &clargs.input_files, &config.execution.output_path).await
}