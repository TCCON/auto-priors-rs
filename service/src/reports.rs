use std::sync::Arc;

use anyhow::Context;
use log::{debug, info, warn};
use orm::{config::Config, email};
use tokio::sync::RwLock;

use crate::error::ErrorHandler;


pub(crate) enum ReportMessage {
    DailyReport,
    WeeklyReport,
    StopGracefully,
    StopRapidly
}

pub(crate) struct ReportManager {
    pub(crate) pool: orm::PoolWrapper,
    pub(crate) shared_config: Arc<RwLock<Config>>,
    pub(crate) error_handler: ErrorHandler,
    pub(crate) msg_recv: tokio::sync::mpsc::Receiver<ReportMessage>,
}

impl ReportManager {
    pub(crate) async fn new_with_pool(
        pool: orm::PoolWrapper, 
        shared_config: Arc<RwLock<Config>>, 
        error_handler: ErrorHandler,
        msg_recv: tokio::sync::mpsc::Receiver<ReportMessage>,
    ) -> Self {
        Self { 
            pool,
            shared_config,
            error_handler,
            msg_recv,
        }
    }

    pub(crate) async fn message_loop(&mut self) {
        loop {
            debug!("MetManager waiting for next message");
            let msg = self.msg_recv.recv().await;
            // Must always handle messages, otherwise the shutdown messages aren't processed.
            // Check if this component is disabled in the working functions.
            if let Some(m) = msg {
                let res = match m {
                    ReportMessage::DailyReport => self.send_daily_report().await,
                    ReportMessage::WeeklyReport => self.send_weekly_report().await,
                    ReportMessage::StopGracefully => break,
                    ReportMessage::StopRapidly => break,
                };

                if let Err(e) = res {
                    self.error_handler.report_error_with_context(
                        e.as_ref(), 
                        "Error occurred in ReportManager message loop"
                    );
                }
            } else {
                info!("MetManager receiver closed, exiting message loop");
                break;
            }
        }
    }

    async fn am_i_disabled(&self) -> bool {
        self.shared_config.read().await.timing.disable_reports
    }

    async fn send_daily_report(&mut self) -> anyhow::Result<()> {
        if self.am_i_disabled().await {
            warn!("Sending reports is disabled, not sending daily report.");
            return Ok(());
        }

        let to_emails = self.shared_config.read().await.email.report_emails_string_list(true);
        let to_emails: Vec<_> = to_emails.iter().map(|s| s.as_str()).collect();

        let mut conn = self.pool.get_connection().await
            .context("Error occurred trying to get database connection to send daily report")?;
        let config = self.shared_config.read().await;
        email::email_current_jobs(&mut conn, &config, &to_emails).await
            .context("Error occurred sending daily report email")?;

        Ok(())
    }

    async fn send_weekly_report(&mut self) -> anyhow::Result<()> {
        if self.am_i_disabled().await {
            warn!("Sending reports is disabled, not sending weekly report");
            return Ok(());
        }

        let to_emails = self.shared_config.read().await.email.report_emails_string_list(true);
        let to_emails: Vec<_> = to_emails.iter().map(|s| s.as_str()).collect();

        let mut conn = self.pool.get_connection().await
            .context("Error occurred trying to get database connection to send daily report")?;
        let config = self.shared_config.read().await;

        let start_date = chrono::Local::now().date_naive() - chrono::Duration::days(7);
        email::email_completed_jobs(&mut conn, &config, &to_emails, start_date, None).await
            .context("Error occurred trying to send weekly report email")?;

        Ok(())
    }

}