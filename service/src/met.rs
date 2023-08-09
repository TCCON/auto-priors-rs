use std::sync::Arc;

use cli::met_download::download_missing_files;
use cli::utils::WgetDownloader;
use log::warn;
use orm::config::Config;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::error::ErrorHandler;


#[derive(Debug)]
pub(crate) struct MetManager<H: ErrorHandler> {
    pub(crate) pool: orm::PoolWrapper,
    pub(crate) shared_config: Arc<RwLock<Config>>,
    pub(crate) error_handler: H,
    inner_runner: Option<JoinHandle<()>>,
}

impl<H: ErrorHandler + 'static> MetManager<H> {
    pub(crate) async fn new_with_pool(
        pool: orm::PoolWrapper, 
        shared_config: Arc<RwLock<Config>>, 
        error_handler: H,
    ) -> Self {
        Self { 
            pool,
            shared_config,
            error_handler,
            inner_runner: None
        }
    }

    pub(crate) async fn scheduler_entry_point(&mut self) {
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
}