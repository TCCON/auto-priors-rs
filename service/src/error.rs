use std::sync::Arc;

use log::error;
use orm::config::{Config, EmailConfig};
use tokio::sync::{watch, RwLock};

#[derive(Debug, Clone)]
pub enum ErrorHandler {
    Logging(LoggingErrorHandler),
    EmailAdmins(EmailAdminsErrorHandler),
}

impl ErrorHandler {
    pub fn report_error(&self, err: &(dyn std::error::Error + Send + Sync + 'static)) {
        match self {
            ErrorHandler::Logging(h) => h.report_error(err),
            ErrorHandler::EmailAdmins(h) => h.report_error(err),
        }
    }

    pub fn report_error_with_context<S: AsRef<str>>(
        &self,
        err: &(dyn std::error::Error + Send + Sync + 'static),
        context: S,
    ) {
        match self {
            ErrorHandler::Logging(h) => h.report_error_with_context(err, context),
            ErrorHandler::EmailAdmins(h) => h.report_error_with_context(err, context),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoggingErrorHandler {}

impl LoggingErrorHandler {
    pub fn report_error(&self, err: &(dyn std::error::Error + Send + Sync + 'static)) {
        error!("{err:?}")
    }

    pub fn report_error_with_context<S: AsRef<str>>(
        &self,
        err: &(dyn std::error::Error + Send + Sync + 'static),
        context: S,
    ) {
        error!("{err:?}");
        error!("{}", context.as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct EmailAdminsErrorHandler {
    cached_email_config: EmailConfig,
    config_watcher: watch::Receiver<orm::config::Config>,
}

impl EmailAdminsErrorHandler {
    pub async fn new(
        shared_config: Arc<RwLock<Config>>,
        rx_config: watch::Receiver<orm::config::Config>,
    ) -> Self {
        let cached_email_config = shared_config.read().await.email.clone();
        Self {
            cached_email_config,
            config_watcher: rx_config,
        }
    }

    fn send_email(&self, body: &str) -> anyhow::Result<()> {
        // Try to get an updated email configuration
        let has_changed = self.config_watcher.has_changed()
            .unwrap_or_else(|e| {
                error!("EmailAdminsErrorHandler: Could not check if email configuration has updated, error was: {e:?}");
                false
            });

        let this_email_cfg = if has_changed {
            self.config_watcher.borrow().email.clone()
        } else {
            self.cached_email_config.clone()
        };

        this_email_cfg.send_mail_to_admins("Error in AutoModRust execution", body)?;
        Ok(())
    }

    fn report_error(&self, err: &(dyn std::error::Error + Send + Sync + 'static)) {
        error!("{err:?}");

        let now = chrono::Local::now();
        let body = format!("An error occurred in AutoModRust at {now}:\n\n{err:?}");
        self.send_email(&body).unwrap_or_else(|e| {
            error!("Error occured while sending error report to admins: {e:?}");
        })
    }

    fn report_error_with_context<S: AsRef<str>>(
        &self,
        err: &(dyn std::error::Error + Send + Sync + 'static),
        context: S,
    ) {
        error!("{err:?}");
        error!("{}", context.as_ref());

        let now = chrono::Local::now();
        let body = format!("An error occurred in AutoModRust at {now}:\n\n{err:?}\n\nThe context of the error was:\n\n{}", context.as_ref());
        self.send_email(&body).unwrap_or_else(|e| {
            error!("Error occured while sending error report to admins: {e:?}");
        });
    }
}
