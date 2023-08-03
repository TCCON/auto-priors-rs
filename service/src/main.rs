use std::{sync::Arc, collections::HashMap, time::Duration};

use clokwerk::{TimeUnits, Job};
use tokio::sync::{RwLock, Mutex, OnceCell};

mod error;
mod jobs;

static JOBS_MANAGER: OnceCell<Mutex<jobs::JobManager<jobs::ServiceJobRunner, error::LoggingErrorHandler>>> = OnceCell::const_new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = orm::get_database_url(None)?;
    let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();

    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    let config = orm::config::load_config_file_or_default(config_file)?;
    let timing_config = (&config.timing).clone();
    let config = Arc::new(RwLock::new(config));

    let err_handler = error::LoggingErrorHandler{};
    let (_exit_sx, exit_rx) = tokio::sync::watch::channel(ExitCommand::Continue);

    let job_manager: jobs::JobManager<jobs::ServiceJobRunner, error::LoggingErrorHandler> = jobs::JobManager{
        db_conn: db.get_connection().await.expect("Failed to initialize database connection for job manager"),
        shared_config: config,
        error_handler: err_handler,
        exit_signal: exit_rx.clone(),
        job_queues: HashMap::new()
    };
    JOBS_MANAGER.set(Mutex::new(job_manager)).expect("Could not set the global job manager");

    let mut scheduler = clokwerk::AsyncScheduler::new();
    scheduler
        .every(timing_config.job_start_seconds.seconds())
            .run(|| async {
                let mutex = JOBS_MANAGER.get().unwrap();
                let mut jm = mutex.lock().await;
                jm.scheduler_entry_point().await;
            });

    let lut_job = scheduler
        .every(timing_config.lut_regen_hours.hours())
            .run(|| async { 
                let mutex = JOBS_MANAGER.get().unwrap();
                let mut jm = mutex.lock().await;
                jm.schedule_lut_regen().await;
            });
    if let Some(at) = timing_config.lut_regen_at {
        lut_job.at(&at);
    }

    // Start the scheduler
    {
        let exit = exit_rx.clone();
        tokio::spawn(async move {
            loop {
                let sig = { exit.borrow().to_owned() };
                if let ExitCommand::Continue = sig {
                    scheduler.run_pending().await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                } else {
                    break;
                }
            }
        });
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ExitCommand {
    Continue,
    Graceful,
    Rapid
}