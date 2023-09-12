use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use log::{debug, info, warn};
use orm::config::Config;
use orm::stdsitejobs::StdSiteJob;
use tokio::sync::RwLock;

use crate::error::ErrorHandler;


#[derive(Debug, Clone, Copy)]
pub(crate) enum StdSiteMessage {
    AddJobs,
    MakeTarballs,
    UpdateJson,
    StopGracefully,
    StopRapidly
}

#[derive(Debug)]
pub(crate) struct StdSiteManager {
    pub(crate) pool: orm::PoolWrapper,
    pub(crate) shared_config: Arc<RwLock<Config>>,
    pub(crate) error_handler: ErrorHandler,
    pub(crate) msg_recv: tokio::sync::mpsc::Receiver<StdSiteMessage>
}

impl StdSiteManager {
    pub(crate) async fn new_with_pool(
        pool: orm::PoolWrapper, 
        shared_config: Arc<RwLock<Config>>, 
        error_handler: ErrorHandler,
        msg_recv: tokio::sync::mpsc::Receiver<StdSiteMessage>
    ) -> Self {
        Self { 
            pool,
            shared_config,
            error_handler,
            msg_recv
        }
    }

    pub(crate) async fn message_loop(&mut self) {
        loop {
            debug!("StdSiteManager waiting for next message");
            let msg = self.msg_recv.recv().await;
            // Must always handle messages, otherwise the shutdown messages aren't processed.
            // Check if this component is disabled in the working functions.
            if let Some(m) = msg {
                debug!("StdSiteManager received message: {m:?}");
                let res = match m {
                    StdSiteMessage::AddJobs => self.add_needed_std_site_jobs().await,
                    StdSiteMessage::MakeTarballs => self.tar_std_sites_output().await,
                    StdSiteMessage::UpdateJson => self.update_site_json().await,
                    StdSiteMessage::StopGracefully => break,
                    StdSiteMessage::StopRapidly => break,
                };

                if let Err(e) = res {
                    self.error_handler.report_error_with_context(
                        e.as_ref(),
                        "Error in StdSiteManager message loop"
                    );
                }
            } else {
                info!("StdSiteManager receiver closed, exiting message loop");
                break;
            }
        }
    }

    async fn am_i_disabled(&self) -> bool {
        self.shared_config.read().await.timing.disable_std_site_gen
    }

    async fn add_needed_std_site_jobs(&mut self) -> anyhow::Result<()> {
        if self.am_i_disabled().await {
            warn!("Standard site priors disabled in config");
            return Ok(());
        }
        let mut conn = self.pool.get_connection().await
            .context("Error occurred trying to get the database connection to update the standard sites jobs")?;

        let config = self.shared_config.read().await;

        info!("Updating the list of rows requiring jobs in the standard sites job table");
        StdSiteJob::update_std_site_job_table(&mut conn, &config, None).await
            .context("Error occurred while trying to update the standard sites job table")?;

        info!("Adding jobs for the standard sites");
        StdSiteJob::add_jobs_for_pending_rows(&mut conn, &config).await
            .context("Error occurred while trying to add jobs for standard sites")?;

        info!("Standard site table/job update complete");
        Ok(())
    }

    async fn tar_std_sites_output(&mut self) -> anyhow::Result<()> {
        if self.am_i_disabled().await {
            warn!("Standard site priors disabled in config");
            return Ok(());
        }
        
        let mut conn = self.pool.get_connection().await
            .context("Error occurred trying to get the database connection to make standard sites tarballs")?;

        let config = self.shared_config.read().await;
        info!("Checking for standard sites output ready to be put in tarballs");
        StdSiteJob::make_standard_site_tarballs(&mut conn, &config).await
            .context("Error occurred while making standard site tarballs")?;
        info!("Standard site tarball check complete");

        Ok(())
    }

    async fn update_site_json(&self) -> anyhow::Result<()> {
        
        let (flat_file, grouped_file) = {
            let config = self.shared_config.read().await;
            (config.execution.flat_stdsite_json_file.clone(), config.execution.grouped_stdsite_json_file.clone())
        };

        // No reason to open a database connection if nothing to write!
        if flat_file.is_none() && grouped_file.is_none() {
            warn!("No output paths defined for the standard site JSON file (flat or grouped)");
            return Ok(());
        }

        let mut conn = self.pool.get_connection().await
        .context("Could not get database connection while updating standard site JSON")?;

        let infos = orm::siteinfo::SiteInfo::get_all_site_info(&mut conn).await?;
        if let Some(p) = flat_file {
            let json_string = orm::siteinfo::SiteInfo::to_flat_json(&infos, true)
                .context("Error occurred while making flat standard site JSON string")?;
            Self::write_json(&p, &json_string)
                .context("Error occurred while writing flat standard site JSON")?;
        }

        if let Some(p) = grouped_file {
            let json_string = orm::siteinfo::SiteInfo::to_grouped_json(&infos, true)
                .context("Error occurred while making grouped standard site JSON string")?;
            Self::write_json(&p, &json_string)
                .context("Error occurred while writing grouped standard site JSON")?;
        }
        
        Ok(())
    }

    fn write_json(json_file: &Path, json_string: &str) -> anyhow::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(json_file)
            .with_context(|| format!("Could not create standard site JSON file at {}", json_file.display()))?;
        write!(f, "{}", json_string)
            .with_context(|| format!("Could not write to standard site JSON file {}", json_file.display()))?;
        Ok(())
    }
}