use std::error::Error;
use std::fmt::Display;
use std::path::Path;
use std::{io::Write, process::Command};

use anyhow::Context;
use futures::StreamExt;

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
            DownloadError::FilesNotAvailable => {
                write!(f, "One or more requested files could not be downloaded")
            }
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
    verbosity: u8,
}

impl WgetDownloader {
    pub fn new() -> Self {
        Self {
            urls: Vec::new(),
            verbosity: 1,
        }
    }

    pub fn new_with_verbosity(verbosity: u8) -> Self {
        Self {
            urls: Vec::new(),
            verbosity,
        }
    }

    fn verb_argument(&self) -> &'static str {
        match self.verbosity {
            0 => "--quiet",
            1 => "--no-verbose",
            2 => "",
            _ => "--verbose",
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
        let mut f = std::fs::File::create(&wget_list).with_context(|| {
            format!(
                "Unable to create file for list of URLs for wget to {}",
                wget_list.display()
            )
        })?;
        for url in self.urls.iter() {
            writeln!(f, "{}", url).with_context(|| {
                format!("Unable to write URL to wget list {}", wget_list.display())
            })?;
        }

        let output = Command::new("wget")
            .args([
                self.verb_argument(),
                "--timestamping",
                "-i",
                "wget_list.txt",
            ])
            .current_dir(&save_dir)
            .output()
            .with_context(|| {
                format!(
                    "wget command to download in {} failed to execute",
                    save_dir.display()
                )
            })?;

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
            Err(anyhow::anyhow!(
                "wget call to download files failed with status {}",
                output.status
            )
            .into())
        }
    }

    fn iter_files(&self) -> std::slice::Iter<'_, String> {
        self.urls.iter()
    }
}

/// Downloader using the [`reqwest`] crate rather than an external dependency.
pub struct ReqwestDownloader {
    auth: Option<netrc_rs::Machine>,
}

impl ReqwestDownloader {
    pub fn new() -> Self {
        Self { auth: None }
    }

    pub fn new_netrc(host: &str) -> anyhow::Result<Self> {
        let machine = crate::utils::get_netrc_credentials(host, None)?
            .ok_or_else(|| anyhow::anyhow!("Could not find host '{host}' in netrc file"))?;
        Ok(Self {
            auth: Some(machine),
        })
    }

    pub async fn download_one_file_to(&self, url: &str, dest: &Path) -> Result<(), DownloadError> {
        let client = reqwest::Client::new();
        Self::download_file_to_inner(&client, url, dest, self.auth.as_ref()).await
    }

    async fn download_file_to_inner(
        client: &reqwest::Client,
        url: &str,
        dest: &Path,
        auth: Option<&netrc_rs::Machine>,
    ) -> Result<(), DownloadError> {
        let result = if let Some(auth) = auth {
            let user = auth.login.as_deref().unwrap_or("anonymous");
            let pass = auth.password.as_deref();
            log::debug!("Downloading from {url} with basic authentication");
            client.get(url).basic_auth(user, pass).send().await
        } else {
            log::debug!("Downloading from {url} without authentication");
            client.get(url).send().await
        };

        let response = result
            .map_err(|e| DownloadError::Other(anyhow::anyhow!("Error sending get request: {e}")))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(DownloadError::FilesNotAvailable);
        }

        let response = response
            .error_for_status()
            .map_err(|e| anyhow::anyhow!("Server error during request for {url}: {e}"))?;

        // The blocking API does not seem to have a streaming option, unfortunately
        let mut data_stream = response.bytes_stream();

        let mut out = std::fs::File::create(dest)
            .map_err(|e| anyhow::anyhow!("Error creating output file {}: {e}", dest.display()))?;
        let mut nbytes = 0;
        while let Some(chunk_result) = data_stream.next().await {
            let chunk = chunk_result
                .with_context(|| anyhow::anyhow!("Error getting chunk of data from GET request"))?;
            nbytes += out.write(&chunk).with_context(|| {
                anyhow::anyhow!("Error writing data to file {}", dest.display())
            })?;
        }
        log::debug!("{nbytes} bytes from {url} written to {}", dest.display());

        Ok(())
    }
}

// Implementation of the Downloader trait for `reqwest` is tricky. The `reqwest` blocking
// module can't be run in any kind of async context, and using the async module is hard
// to do inside a sync function, because at some point we need to block on the async request.
// If we're running a single threaded runtime, which I think we are, it's easy to deadlock.
// tokio's block_in_place function might help, but it requires a multi-threaded runtime.
// I tried an approach Gemini suggested of checking if we have a current runtime with
// tokio::runtime::Handle::try_current(), but that fell apart because there wasn't a clear
// way to run an async function in a non-async function with a single thread. handle.spawn()
// had lifetime issues, and Gemini later thought its way would cause a deadlock.
// Its last suggestion was futures::executor::block_on (https://docs.rs/futures/latest/futures/executor/fn.block_on.html),
// but by this point I didn't believe it and decided not to worry about this anymore.
