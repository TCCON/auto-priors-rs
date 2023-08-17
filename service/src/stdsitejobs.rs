use std::sync::Arc;

use anyhow::Context;
use log::{debug, info, warn};
use orm::config::Config;
use orm::stdsitejobs::StdSiteJob;
use tokio::sync::RwLock;

use crate::error::ErrorHandler;

// TODO:
//  - Periodically add jobs for the standard sites for new days AND days marked for regen
//  - (If marked for regen, need to make sure that previous days are cleared out first)
//  - After jobs are submitted, periodically check if they are done. If so, make the standard site tarball and update the StdSiteJobs table plus the Jobs table
//  - EM27 sites: add a flag for how to organize the output tarball - EGI/PROFFAST or straight GGG?
//      No, inclined to keep them flat and work with Jacob/Benedikt/Lena to handle the flat output format.

#[derive(Debug, Clone, Copy)]
pub(crate) enum StdSiteMessage {
    AddJobs,
    MakeTarballs,
    StopGracefully,
    StopRapidly
}

#[derive(Debug)]
pub(crate) struct StdSiteManager<H: ErrorHandler> {
    pub(crate) pool: orm::PoolWrapper,
    pub(crate) shared_config: Arc<RwLock<Config>>,
    pub(crate) error_handler: H,
    pub(crate) msg_recv: tokio::sync::mpsc::Receiver<StdSiteMessage>
}

impl<H: ErrorHandler> StdSiteManager<H> {
    pub(crate) async fn new_with_pool(
        pool: orm::PoolWrapper, 
        shared_config: Arc<RwLock<Config>>, 
        error_handler: H,
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
            if self.am_i_disabled().await {
                warn!("Standard site priors generation disabled in config");
            } else if let Some(m) = msg {
                debug!("StdSiteManager received message: {m:?}");
                let res = match m {
                    StdSiteMessage::AddJobs => self.add_needed_std_site_jobs().await,
                    StdSiteMessage::MakeTarballs => self.tar_std_sites_output().await,
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
        let mut conn = self.pool.get_connection().await
            .context("Error occurred trying to get the database connection to make standard sites tarballs")?;

        let config = self.shared_config.read().await;
        info!("Checking for standard sites output ready to be put in tarballs");
        StdSiteJob::make_standard_site_tarballs(&mut conn, &config).await
            .context("Error occurred while making standard site tarballs")?;
        info!("Standard site tarball check complete");

        Ok(())
    }
}