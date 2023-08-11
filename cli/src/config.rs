use std::path::PathBuf;

use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub struct ConfigCli {
    #[clap(subcommand)]
    pub command: ConfigActions
}

#[derive(Debug, Subcommand)]
pub enum ConfigActions {
    GenConfig(GenConfigCli),
    DebugConfig
}

#[derive(Debug, Args)]
/// Generate a default configuration file from the command line
pub struct GenConfigCli {
    /// Path to write the default TOML file as.
    path: PathBuf
}

pub fn generate_config_file(clargs: GenConfigCli) -> anyhow::Result<()> {
    orm::config::generate_config_file(&clargs.path)
}

pub fn debug_config(config: orm::config::Config) {
    dbg!(config);
}
