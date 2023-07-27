use std::{sync::Arc, collections::HashMap, time::Duration};

use clokwerk::TimeUnits;
use tokio::sync::RwLock;

mod error;
mod jobs;

#[derive(Debug, Clone, Copy)]
enum ExitCommand {
    Continue,
    Graceful,
    Rapid
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = orm::get_database_url(None)?;
    let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();

    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    let config = orm::config::load_config_file_or_default(config_file)?;
    let config = Arc::new(RwLock::new(config));

    let err_handler = error::LoggingErrorHandler{};
    let (_sx, rx) = tokio::sync::watch::channel(ExitCommand::Continue);


    let mut job_manager: jobs::JobManager<jobs::ServiceJobRunner, error::LoggingErrorHandler> = jobs::JobManager{
        db_conn: db.get_connection().await.expect("Failed to initialize database connection for job manager"),
        shared_config: config,
        error_handler: err_handler,
        exit_signal: rx.clone(),
        job_queues: HashMap::new()
    };

    let job_task = tokio::spawn(async move {
        loop {
            if job_manager.scheduler_entry_point().await {
                break;
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    Ok(())
}