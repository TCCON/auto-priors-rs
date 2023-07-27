use std::{io::Write, process::Command};
use std::path::Path;

use anyhow::{Result, Context};

pub trait Downloader {
    fn add_file_to_download(&mut self, url: String) -> Result<()>;
    fn download_files(&mut self, save_dir: &Path) -> Result<()>;
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
    fn add_file_to_download(&mut self, url: String) -> Result<()> {
        self.urls.push(url);
        Ok(())
    }

    fn download_files(&mut self, save_dir: &Path) -> Result<()> {
        let wget_list = save_dir.join("wget_list.txt");
        let mut f = std::fs::File::create(&wget_list)
            .with_context(|| format!("Unable to create file for list of URLs for wget to {}", wget_list.display()))?;
        for url in self.urls.iter() {
            writeln!(f, "{}", url).with_context(|| format!("Unable to write URL to wget list {}", wget_list.display()))?;
        }

        let output = Command::new("wget")
            .args([self.verb_argument(), "-i", "wget_list.txt"])
            .current_dir(&save_dir)
            .output()
            .with_context(|| format!("wget command to download in {} failed to execute", save_dir.display()))?;

        // If these fail, it's not worth propagating that error
        // TODO: maybe pipe to info! or debug!
        let _ = std::io::stdout().write_all(&output.stdout);
        let _ = std::io::stderr().write_all(&output.stderr);
        
        if output.status.success() {
            Ok(())
        } else {
            Err(anyhow::Error::msg(format!("wget call to download files failed with status {}", output.status)))
        }
    }

    fn iter_files(&self) -> std::slice::Iter<'_, String>{
        self.urls.iter()
    }
}
