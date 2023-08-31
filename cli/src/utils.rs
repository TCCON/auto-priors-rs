use std::error::Error;
use std::fmt::Display;
use std::{io::Write, process::Command};
use std::path::Path;

use anyhow::Context;

#[derive(Debug)]
pub enum DownloadError {
    FilesNotAvailable,
    Other(anyhow::Error),
}

impl From<std::io::Error> for DownloadError {
    fn from(value: std::io::Error) -> Self {
        Self::Other(value.into())
    }
}

impl From<anyhow::Error> for DownloadError {
    fn from(value: anyhow::Error) -> Self {
        Self::Other(value)
    }
}

impl Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::FilesNotAvailable => write!(f, "One or more requested files could not be downloaded"),
            DownloadError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl Error for DownloadError {}


/// A trait to implement for any mechanism to download a list of files.
/// This is usually used to download met data, for example.
pub trait Downloader {
    /// Add a file to the list to download. This is allowed to fail in case
    /// the internal list of files to download may error when appended to.
    fn add_file_to_download(&mut self, url: String) -> anyhow::Result<()>;

    /// Download the files currently stored in the internal list. This should
    /// avoid redownloading the same file if it already exists locally and has
    /// not changed; for example, if using wget, include the --timestamping flag
    /// to only download files with a newer server modification time than the local
    /// copy. 
    /// 
    /// If *any* of the listed files failed to download because it was not available
    /// from the server, then this function must return a [`DownloadError:FilesNotAvailable`].
    /// However, callers of this method should be aware that it is not always possible
    /// to uniquely identify a failed download as due to unavailability of the requested
    /// files. For example, most `wget` implementations return status code 8 for a server
    /// side error, but it is not clear that this only happens when a file is not available.
    /// 
    /// Callers should also recognize that it is possible some, but not all, of the
    /// files requested were available. If it is important that any downloaded files 
    /// be recognized, the caller should check if each file exists locally.
    fn download_files(&mut self, save_dir: &Path) -> Result<(), DownloadError>;

    /// Provide an iterator over the internal list of URLs.
    fn iter_files(&self) -> std::slice::Iter<'_, String>;
}

#[derive(Debug, Clone)]
pub struct WgetDownloader {
    urls: Vec<String>,
    verbosity: u8
}

impl WgetDownloader {
    pub fn new() -> Self {
        Self { urls: Vec::new(), verbosity: 1 }
    }

    pub fn new_with_verbosity(verbosity: u8) -> Self {
        Self { urls: Vec::new(), verbosity }
    }

    fn verb_argument(&self) -> &'static str {
        match self.verbosity {
            0 => "--quiet",
            1 => "--no-verbose",
            2 => "",
            _ => "--verbose"
        }
    }
}

impl Downloader for WgetDownloader {
    fn add_file_to_download(&mut self, url: String) -> anyhow::Result<()> {
        self.urls.push(url);
        Ok(())
    }

    fn download_files(&mut self, save_dir: &Path) -> Result<(), DownloadError> {
        let wget_list = save_dir.join("wget_list.txt");
        let mut f = std::fs::File::create(&wget_list)
            .with_context(|| format!("Unable to create file for list of URLs for wget to {}", wget_list.display()))?;
        for url in self.urls.iter() {
            writeln!(f, "{}", url).with_context(|| format!("Unable to write URL to wget list {}", wget_list.display()))?;
        }

        let output = Command::new("wget")
            .args([self.verb_argument(), "--timestamping", "-i", "wget_list.txt"])
            .current_dir(&save_dir)
            .output()
            .with_context(|| format!("wget command to download in {} failed to execute", save_dir.display()))?;

        // If these fail, it's not worth propagating that error
        // TODO: maybe pipe to info! or debug!
        let _ = std::io::stdout().write_all(&output.stdout);
        let _ = std::io::stderr().write_all(&output.stderr);
        
        if output.status.success() {
            Ok(())
        } else if output.status.code() == Some(8) {
            // At least in the version of wget in Ubuntu 20.04 and RHEL 8, an exit code of 8 indicates a server
            // error returned, which implies that we did everything right but the files aren't available.
            Err(DownloadError::FilesNotAvailable)
        } else {
            Err(anyhow::anyhow!("wget call to download files failed with status {}", output.status).into())
        }
    }

    fn iter_files(&self) -> std::slice::Iter<'_, String>{
        self.urls.iter()
    }
}
