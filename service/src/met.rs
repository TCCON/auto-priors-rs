use std::sync::Arc;

use cli::met_download::download_missing_files;
use cli::utils::WgetDownloader;
use log::{warn, debug, info};
use orm::config::Config;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::error::ErrorHandler;


pub(crate) enum MetMessage {
    DownloadMet,
    StopGracefully,
    StopRapidly
}

#[derive(Debug)]
pub(crate) struct MetManager {
    pub(crate) pool: orm::PoolWrapper,
    pub(crate) shared_config: Arc<RwLock<Config>>,
    pub(crate) error_handler: ErrorHandler,
    pub(crate) msg_recv: tokio::sync::mpsc::Receiver<MetMessage>,
    inner_runner: Option<JoinHandle<()>>
}

impl MetManager {
    pub(crate) async fn new_with_pool(
        pool: orm::PoolWrapper, 
        shared_config: Arc<RwLock<Config>>, 
        error_handler: ErrorHandler,
        msg_recv: tokio::sync::mpsc::Receiver<MetMessage>,
    ) -> Self {
        Self { 
            pool,
            shared_config,
            error_handler,
            msg_recv,
            inner_runner: None
        }
    }

    pub(crate) async fn message_loop(&mut self) {
        loop {
            debug!("MetManager waiting for next message");
            let msg = self.msg_recv.recv().await;
            // Must always handle messages, otherwise the shutdown messages aren't processed.
            // Check if this component is disabled in the working functions.
            if let Some(m) = msg {
                match m {
                    MetMessage::DownloadMet => self.scheduler_entry_point().await,
                    MetMessage::StopGracefully => {
                        self.wait_for_download_to_finish().await;
                        break;
                    },
                    MetMessage::StopRapidly => {
                        self.stop_running_download().await;
                        break;
                    },
                };
            } else {
                info!("MetManager receiver closed, exiting message loop");
                break;
            }
        }
    }

    async fn am_i_disabled(&self) -> bool {
        self.shared_config.read().await.timing.disable_met_download
    }

    async fn scheduler_entry_point(&mut self) {
        if self.am_i_disabled().await {
            warn!("Met download disabled in config");
            return;
        }

        if let Some(handle) = &self.inner_runner {
            if handle.is_finished() {
                self.inner_runner = None;
            } else {
                warn!("Cannot start a second met download task while one is ongoing.");
                return ;
            }
        }

        let downloader = WgetDownloader::new();
        let config = (self.shared_config.read().await).clone();
        let mut conn = self.pool.get_connection().await.unwrap();
        let err_handler = self.error_handler.clone();

        let child = tokio::spawn(async move {
            let res = download_missing_files(
                &mut conn, 
                None, 
                None, 
                None, 
                false, 
                &config, 
                downloader, 
                false
            ).await;

            if let Err(e) = res {
                err_handler.report_error(e.as_ref())
            }
        });
        self.inner_runner = Some(child);

    }

    async fn stop_running_download(&mut self) {        
        if let Some(runner) = self.inner_runner.take() {
            info!("Cancelling met download in progress");
            runner.abort();
            match runner.await {
                Ok(_) => info!("Met download task had completed before being cancelled"),
                Err(e) if e.is_cancelled() => info!("Met download task cancelled"),
                Err(e) => warn!("Error while cancelling met download task: {e:?}"),
            }
        } else {
            info!("No active met download task to cancel");
        }
    }

    async fn wait_for_download_to_finish(&mut self) {
        if let Some(runner) = self.inner_runner.take() {
            match runner.await {
                Ok(_) => info!("Met download task complete, proceeding with service shutdown"),
                Err(e) => warn!("Error while waiting for met download task to complete: {e:?}")
            }
        } else {
            info!("No active met download task to wait on");
        }
    }
}