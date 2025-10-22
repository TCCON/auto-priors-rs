use std::path::PathBuf;

use clap::{Args, Subcommand};

use orm::{export, MySqlConn};

#[derive(Debug, Args)]
pub struct DbCli {
    #[clap(subcommand)]
    pub command: DbActions,
}

/// Back up/restore the database
#[derive(Debug, Subcommand)]
pub enum DbActions {
    Export(ExportCli),
    Import(ImportCli),
}

/// Export the database to a JSON file.
///
/// By default this will include information about the SQLx migrations
/// in the JSON file so that we can verify that we are importing
/// compatible data. If you did not set up this database with SQLx migrations,
/// you will need to include the --allow-missing-migrations flag.
#[derive(Debug, Args)]
pub struct ExportCli {
    /// File to dump the database to
    json_file: PathBuf,

    /// Allow overwriting the JSON_FILE
    #[clap(short = 'c', long)]
    clobber: bool,

    /// Output the JSON in the more compact (less readable) form
    #[clap(short = 'k', long)]
    compact: bool,

    /// Pass this flag to avoid an error if the database does not include
    /// the SQLx migrations table.
    #[clap(short = 'a', long)]
    allow_missing_migrations: bool,
}

pub async fn export_cli(conn: &mut MySqlConn, args: ExportCli) -> anyhow::Result<()> {
    if args.json_file.exists() && !args.clobber {
        anyhow::bail!(
            "{} exists, cannot overwrite. Use --clobber to permit overwriting.",
            args.json_file.display()
        );
    }

    export::export_database(
        conn,
        &args.json_file,
        args.compact,
        !args.allow_missing_migrations,
    )
    .await
}

/// Import the database contents from a JSON file, deleting the existing contents of the database after the JSON is read successfully.
///
/// By default, this will check that the SQLx migrations match between the database and the
/// JSON file. If they don't, it will not proceed. You can reduce or disable this check with
/// the --migration-validation option. If you did not use SQLx to set up the database, that
/// will be a necessary step.
#[derive(Debug, Args)]
pub struct ImportCli {
    /// File to read the previous database contents from
    json_file: PathBuf,

    /// Change the migration checks; "off" will disable them, "loose" will only
    /// confirm that the checksums appear in the right order, and "strict" (the
    /// default) will confirm that the versions, descriptions, and checksums all
    /// appear in the right order.
    #[clap(short='m', long, default_value_t = export::MigrationValidation::Strict)]
    migration_validation: export::MigrationValidation,
}

pub async fn import_cli(conn: &mut MySqlConn, args: ImportCli) -> anyhow::Result<()> {
    if !args.json_file.exists() {
        anyhow::bail!("JSON file {} does not exist", args.json_file.display());
    }

    export::import_database(conn, &args.json_file, args.migration_validation).await
}
