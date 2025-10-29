use std::{fmt::Display, path::Path, str::FromStr};

use anyhow::Context;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sqlx::{prelude::FromRow, Connection};

use crate::{
    jobs::QJob, met::MetFile, siteinfo::ExportSiteInfo, siteinfo::QStdSite,
    stdsitejobs::ExportStdSiteJob, MySqlConn,
};

pub async fn export_database(
    conn: &mut MySqlConn,
    json_path: &Path,
    compact: bool,
    require_migrations: bool,
) -> anyhow::Result<()> {
    let migration_result = sqlx::query_as!(
        Migration,
        "SELECT version, description, checksum FROM _sqlx_migrations"
    )
    .fetch_all(&mut *conn)
    .await;

    info!("Querying migrations...");
    let migrations = match (migration_result, require_migrations) {
        (Ok(m), _) => Some(m),
        (Err(e), false) => {
            warn!("Could not get SQLx migrations from the database (error was: {e}). Output JSON file will not have migration information for validation.");
            None
        }
        (Err(e), true) => anyhow::bail!("Could not find SQLx migrations in the database: {e}"),
    };

    info!("Querying met files...");
    let met_files = sqlx::query_as!(MetFile, "SELECT * FROM MetFiles")
        .fetch_all(&mut *conn)
        .await?;

    info!("Querying jobs...");
    let jobs = sqlx::query_as!(crate::jobs::QJob, "SELECT * FROM Jobs")
        .fetch_all(&mut *conn)
        .await?;

    info!("Querying standard site list...");
    let std_site_list = sqlx::query_as!(crate::siteinfo::QStdSite, "SELECT * FROM StdSiteList")
        .fetch_all(&mut *conn)
        .await?;

    info!("Querying standard site information...");
    let std_site_info = sqlx::query_as!(ExportSiteInfo, "SELECT * FROM StdSiteInfo")
        .fetch_all(&mut *conn)
        .await?;

    info!("Querying standard site jobs...");
    let std_site_jobs = sqlx::query_as!(ExportStdSiteJob, "SELECT * FROM StdSiteJobs")
        .fetch_all(&mut *conn)
        .await?;

    info!("Exporting to JSON...");
    let db_data = Db {
        migrations,
        met_files,
        jobs,
        std_site_list,
        std_site_info,
        std_site_jobs,
    };
    let mut f = std::fs::File::create(json_path)?;
    if compact {
        serde_json::to_writer(&mut f, &db_data)?;
    } else {
        serde_json::to_writer_pretty(&mut f, &db_data)?;
    }

    info!("Done");

    Ok(())
}

pub async fn import_database(
    conn: &mut MySqlConn,
    json_path: &Path,
    migration_check_level: MigrationValidation,
) -> anyhow::Result<()> {
    // If we can't open the JSON file, let's figure that out before we touch the database.
    let f = std::fs::File::open(json_path).context("Failed to open JSON file")?;
    info!("Deserializing database dump...");
    let db_data: Db = serde_json::from_reader(f).context("Failed to read JSON file")?;
    info!("Deserialization complete.");

    // Then check that the migrations are correct; again, no reason to futz with the database if they are not
    if !migration_check_level.is_off() {
        let json_migrations = db_data.migrations
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("JSON file {} does not include migrations; you must disable migration checks to import it", json_path.display()))?;

        let db_migrations = sqlx::query_as!(
            Migration,
            "SELECT version, description, checksum FROM _sqlx_migrations",
        )
        .fetch_all(&mut *conn)
        .await?;

        migration_check_level
            .check_migrations(json_migrations, &db_migrations)
            .await
            .context("You may need to reduce the migration check level to import the JSON file")?;
    }

    let mut transaction = conn.begin().await?;
    if let Err(e) = import_db_inner(&mut transaction, db_data).await {
        eprintln!("Database import failed, rolling database back");
        transaction
            .rollback()
            .await
            .context("Error occurred while rolling back transaction!")?;
        return Err(e);
    } else {
        transaction
            .commit()
            .await
            .context("Error occurred while committing the transaction")?;
    }
    Ok(())
}

async fn import_db_inner(conn: &mut MySqlConn, db_data: Db) -> anyhow::Result<()> {
    // We'll clear each of the tables before we write to it. Then for now we'll iterate over the elements to
    // insert; if that is painfully slow, we could use a query builder:
    // https://docs.rs/sqlx-core/latest/sqlx_core/query_builder/struct.QueryBuilder.html#method.push_values
    // Note that we need to delete things in a specific order to avoid foreign key issues, and that we
    // need to delete everything up front

    // TODO: make a method to insert a new metfile, use it here and update MetFile::add_met_file
    //  (ditto for Job, maybe others)
    info!("Clearing existing tables");
    sqlx::query!("DELETE FROM StdSiteJobs")
        .execute(&mut *conn)
        .await?;
    sqlx::query!("DELETE FROM StdSiteInfo")
        .execute(&mut *conn)
        .await?;
    sqlx::query!("DELETE FROM StdSiteList")
        .execute(&mut *conn)
        .await?;
    sqlx::query!("DELETE FROM Jobs").execute(&mut *conn).await?;
    sqlx::query!("DELETE FROM MetFiles")
        .execute(&mut *conn)
        .await?;

    info!("Filling table MetFiles");
    for row in progress_iter(db_data.met_files) {
        sqlx::query!(
            "INSERT INTO MetFiles(file_id, file_path, product_key, filedate) VALUES (?, ?, ?, ?)",
            row.file_id,
            row.file_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Unable to convert met file path to string"))?,
            row.product_key,
            row.filedate.to_string(),
        )
        .execute(&mut *conn)
        .await?;
    }
    info!("MetFile complete.");

    info!("Filling table Jobs");
    for row in progress_iter(db_data.jobs) {
        sqlx::query!(
            "INSERT INTO Jobs(job_id, state, site_id, start_date, end_date, lat, lon, email, delete_time, priority, queue, processing_key, save_dir, save_tarball, mod_fmt, vmr_fmt, map_fmt, submit_time, complete_time, output_file) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row.job_id,
            row.state,
            row.site_id,
            row.start_date,
            row.end_date,
            row.lat,
            row.lon,
            row.email,
            row.delete_time,
            row.priority,
            row.queue,
            row.processing_key,
            row.save_dir,
            row.save_tarball,
            row.mod_fmt,
            row.vmr_fmt,
            row.map_fmt,
            row.submit_time,
            row.complete_time,
            row.output_file
        ).execute(&mut *conn)
        .await?;
    }
    info!("Jobs complete");

    info!("Filling table StdSiteList");
    for row in progress_iter(db_data.std_site_list) {
        sqlx::query!(
            "INSERT INTO StdSiteList(id, site_id, name, site_type, output_structure) VALUE (?, ?, ?, ?, ?)",
            row.id, row.site_id, row.name, row.site_type, row.output_structure
        ).execute(&mut *conn).await?;
    }
    info!("StdSiteList complete");

    info!("Filling table StdSiteInfo");
    for row in progress_iter(db_data.std_site_info) {
        sqlx::query!(
            "INSERT INTO StdSiteInfo(id, site, location, latitude, longitude, start_date, end_date, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            row.id, row.site, row.location, row.latitude, row.longitude, row.start_date, row.end_date, row.comment
        ).execute(&mut *conn).await?;
    }
    info!("StdSiteInfo complete");

    info!("Filling table StdSiteJobs");
    for row in progress_iter(db_data.std_site_jobs) {
        sqlx::query!(
            "INSERT INTO StdSiteJobs(id, site, date, state, job, tarfile) VALUES (?, ?, ?, ?, ?, ?)",
            row.id, row.site, row.date, row.state, row.job, row.tarfile
        ).execute(&mut *conn).await?;
    }
    info!("StdSiteJobs complete");
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Db {
    migrations: Option<Vec<Migration>>,
    met_files: Vec<MetFile>,
    jobs: Vec<QJob>,
    std_site_list: Vec<QStdSite>,
    std_site_info: Vec<ExportSiteInfo>,
    std_site_jobs: Vec<ExportStdSiteJob>,
}

#[derive(Debug, FromRow, Serialize, Deserialize, PartialEq, Eq)]
struct Migration {
    version: i64,
    description: String,
    checksum: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum MigrationValidation {
    Strict,
    Loose,
    Off,
}

impl Display for MigrationValidation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationValidation::Strict => write!(f, "strict"),
            MigrationValidation::Loose => write!(f, "loose"),
            MigrationValidation::Off => write!(f, "off"),
        }
    }
}

impl MigrationValidation {
    fn is_off(&self) -> bool {
        if let Self::Off = self {
            true
        } else {
            false
        }
    }

    async fn check_migrations(
        &self,
        json_migrations: &[Migration],
        db_migrations: &[Migration],
    ) -> anyhow::Result<()> {
        for (i, (jm, dm)) in json_migrations.iter().zip(db_migrations.iter()).enumerate() {
            match self {
                MigrationValidation::Strict => {
                    if jm != dm {
                        return Err(anyhow::anyhow!("Migration check failed: migration {} version, description, and/or checksum do not match", i+1));
                    }
                }
                MigrationValidation::Loose => {
                    if jm.checksum != dm.checksum {
                        return Err(anyhow::anyhow!(
                            "Migration check failed: migration {} checksum does not match",
                            i + 1
                        ));
                    }
                }
                MigrationValidation::Off => {}
            }
        }

        Ok(())
    }
}

impl FromStr for MigrationValidation {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "strict" => Ok(Self::Strict),
            "loose" => Ok(Self::Loose),
            "off" => Ok(Self::Off),
            _ => Err(anyhow::anyhow!(
                "Invalid value for MigrationValidation: {s}"
            )),
        }
    }
}

fn progress_iter<T>(items: Vec<T>) -> impl Iterator<Item = T> {
    let n = items.len();
    let n10 = n / 10 + if n % 10 < 5 { 0 } else { 1 };
    let n10 = n10.max(10); // avoid panics later if this was 0; will just print out 100% for short lists
    items.into_iter().enumerate().map(move |(i, el)| {
        if i == (n - 1) {
            println!("100% complete ({}/{n})", i + 1)
        } else if i % n10 == 0 && i > 0 {
            let percent = 100.0 * i as f32 / n as f32;
            println!("{percent:.1}% complete ({}/{n})", i + 1);
        }
        el
    })
}
