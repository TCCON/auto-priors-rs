#![allow(dead_code)]
use anyhow::Context;
use orm::config::{MetCfgKey, ProcCfgKey};
use orm::met::MetFile;
use orm::{self, MySqlConn};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tccon_priors_cli::utils::Downloader;

pub fn test_proc_key() -> ProcCfgKey {
    ProcCfgKey("std-geosfpit".to_string())
}

pub fn test_geosfpit_met_keys() -> [MetCfgKey; 3] {
    [
        MetCfgKey("geosfpit-eta-met".to_string()),
        MetCfgKey("geosfpit-surf-met".to_string()),
        MetCfgKey("geosfpit-eta-chm".to_string()),
    ]
}

pub fn test_geosit_met_keys() -> [MetCfgKey; 3] {
    [
        MetCfgKey("geosit-eta-met".to_string()),
        MetCfgKey("geosit-surf-met".to_string()),
        MetCfgKey("geosit-eta-chm".to_string()),
    ]
}

pub fn test_geosfp_met_keys() -> [MetCfgKey; 3] {
    [
        MetCfgKey("geosfp-eta-met".to_string()),
        MetCfgKey("geosfp-surf-met".to_string()),
        MetCfgKey("geosfp-eta-chm".to_string()),
    ]
}

pub fn init_logging() {
    let _ = env_logger::builder()
        .filter_module("sqlx", log::LevelFilter::Warn)
        .format_source_path(true)
        .is_test(true)
        .try_init();
}

pub(crate) fn md5sum(p: &Path) -> anyhow::Result<Vec<u8>> {
    use md5::Digest;
    let mut hasher = md5::Md5::new();
    let mut buf: Vec<u8> = vec![0; 1_000_000];
    let mut f = File::open(p)?;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        } else {
            hasher.update(&buf[..n]);
        }
    }

    Ok(hasher.finalize().to_vec())
}

#[derive(Debug, Clone)]
pub(crate) struct TestDownloader {
    files: Vec<String>,
}

impl TestDownloader {
    pub(crate) fn new() -> Self {
        Self { files: vec![] }
    }
}

impl Downloader for TestDownloader {
    fn add_file_to_download(&mut self, url: String) -> anyhow::Result<()> {
        self.files.push(url);
        Ok(())
    }

    fn download_files(
        &mut self,
        save_dir: &std::path::Path,
    ) -> Result<(), tccon_priors_cli::utils::DownloadError> {
        for url in self.files.iter() {
            let basename = url.split('/').last().ok_or_else(|| {
                anyhow::Error::msg(format!("Could not determine basename of URL {url}"))
            })?;
            let new_file = save_dir.join(basename);
            let mut h = std::fs::File::create(&new_file).with_context(|| {
                format!(
                    "Error occurred while trying to create dummy file {}",
                    new_file.display()
                )
            })?;
            write!(h, "Dummy file created for tccon-priors-cli testing").with_context(|| {
                format!(
                    "Error occurred while trying to write to dummy file {}",
                    new_file.display()
                )
            })?;
        }

        Ok(())
    }

    fn iter_files(&self) -> std::slice::Iter<'_, String> {
        self.files.iter()
    }
}

pub(crate) fn are_met_files_present_on_disk(root: &Path, files: &[&str]) -> anyhow::Result<()> {
    let missing: Vec<_> = files
        .iter()
        .filter_map(|f| {
            let p = root.join(f);
            if !p.exists() {
                Some(*f)
            } else {
                None
            }
        })
        .collect();

    if missing.is_empty() {
        Ok(())
    } else {
        let nmiss = missing.len();
        let nexpect = files.len();
        let mstr = missing.join(", ");
        anyhow::bail!(
            "Out of {nexpect} expected files, {nmiss} were missing from {}: {mstr}",
            root.display()
        )
    }
}

pub(crate) async fn are_met_file_present_in_database(
    conn: &mut MySqlConn,
    files: &[&str],
) -> anyhow::Result<()> {
    // First strip off any leading directories - that way this works more easily with
    // `are_met_files_present_on_disk`, where we might need to specify subdirectories
    let nexpected = files.len();
    let files: Vec<String> = files
        .iter()
        .map(|&f| {
            PathBuf::from(f)
                .file_name()
                .expect("Test file name must not terminate in `..`")
                .to_string_lossy()
                .to_string()
        })
        .collect();

    let mut missing = vec![];

    for file in files {
        let check = MetFile::get_file_by_name(conn, &file)
            .await
            .with_context(|| format!("Query for {file} failed"))?;
        if check.is_none() {
            missing.push(file);
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        let nmiss = missing.len();
        let mstr = missing.join(", ");
        anyhow::bail!(
            "Out of {nexpected} expected files, {nmiss} were missing from the database: {mstr}"
        )
    }
}
