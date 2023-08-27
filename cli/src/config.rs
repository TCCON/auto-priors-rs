use std::path::PathBuf;

use clap::{Args, Subcommand};

/// Generate or check a configuration
#[derive(Debug, Args)]
pub struct ConfigCli {
    #[clap(subcommand)]
    pub command: ConfigActions
}

#[derive(Debug, Subcommand)]
pub enum ConfigActions {
    #[clap(alias = "gen")]
    Generate(GenConfigCli),

    /// Read the configuration file pointed to by the PRIOR_CONFIG_FILE environment variable
    /// and print the internal representation to the screen. (Useful for checking that a config
    /// file is being parsed as you expect.) If the PRIOR_CONFIG_FILE variable is not set,
    /// then the default configuration is displayed.
    Debug
}

#[derive(Debug, Args)]
/// Generate a default configuration file from the command line (alias: gen)
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
