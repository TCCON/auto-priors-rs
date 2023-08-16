use std::sync::Arc;

use log::{debug, info};
use orm::config::Config;
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
            if let Some(m) = msg {
                debug!("StdSiteManager received message: {m:?}");
                match m {
                    StdSiteMessage::AddJobs => todo!(),
                    StdSiteMessage::MakeTarballs => todo!(),
                    StdSiteMessage::StopGracefully => todo!(),
                    StdSiteMessage::StopRapidly => todo!(),
                }
            } else {
                info!("StdSiteManager receiver closed, exiting message loop");
                break;
            }
        }
    }

    async fn add_needed_std_site_jobs(&mut self) {
        todo!()
    }
}