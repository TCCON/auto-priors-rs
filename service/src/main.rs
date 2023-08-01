use std::{sync::Arc, collections::HashMap, time::Duration};

use clokwerk::{TimeUnits, Job};
use tokio::sync::{RwLock, Mutex, OnceCell};

mod error;
mod jobs;

#[derive(Debug, Clone, Copy)]
enum ExitCommand {
    Continue,
    Graceful,
    Rapid
}

static JOBS_MANAGER: OnceCell<Mutex<jobs::JobManager<jobs::ServiceJobRunner, error::LoggingErrorHandler>>> = OnceCell::const_new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = orm::get_database_url(None)?;
    let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();

    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    let config = orm::config::load_config_file_or_default(config_file)?;
    let config = Arc::new(RwLock::new(config));

    let err_handler = error::LoggingErrorHandler{};
    let (_exit_sx, exit_rx) = tokio::sync::watch::channel(ExitCommand::Continue);

    let job_manager: jobs::JobManager<jobs::ServiceJobRunner, error::LoggingErrorHandler> = jobs::JobManager{
        db_conn: db.get_connection().await.expect("Failed to initialize database connection for job manager"),
        shared_config: config,
        error_handler: err_handler,
        exit_signal: exit_rx.clone(),
        lut_regen_time: None,
        job_queues: HashMap::new()
    };
    JOBS_MANAGER.set(Mutex::new(job_manager)).expect("Could not set the global job manager");

    let mut scheduler = clokwerk::AsyncScheduler::new();
    scheduler
        .every(1.minute())
            .run(|| async {
                let mutex = JOBS_MANAGER.get().unwrap();
                let mut jm = mutex.lock().await;
                jm.scheduler_entry_point().await;
            });
    scheduler
        .every(1.day())
            .at("00:00")
            .run(|| async { 
                let mutex = JOBS_MANAGER.get().unwrap();
                let mut jm = mutex.lock().await;
                jm.schedule_lut_regen();
            });
    

    Ok(())
}