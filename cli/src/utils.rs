use std::{io::Write, process::Command};
use std::path::Path;

use anyhow::{Result, Context};

pub trait Downloader {
    fn add_file_to_download(&mut self, url: String) -> Result<()>;
    fn download_files(&mut self, save_dir: &Path) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct WgetDownloader {
    urls: Vec<String>
}

impl WgetDownloader {
    pub fn new() -> Self {
        Self { urls: Vec::new() }
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

        Command::new("wget")
            .args(["-i", "wget_list.txt"])
            .current_dir(&save_dir)
            .spawn()
            .with_context(|| format!("wget command to download in {} failed", save_dir.display()))?;
        
            Ok(())
    }
}