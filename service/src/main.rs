use std::{sync::{Arc, atomic::AtomicBool}, time::Duration};

use clokwerk::{TimeUnits, Job};
use error::LoggingErrorHandler;
use jobs::JobMessage;
use log::{info, warn, error, debug};
use signal_hook::{consts::signal, iterator::Signals};
use tokio::sync::{RwLock, Mutex, OnceCell, mpsc::{self, error::TrySendError, error::TryRecvError, Sender}};

use crate::error::ErrorHandler;

mod error;
mod jobs;
mod met;

static MET_MANAGER: OnceCell<Mutex<met::MetManager<LoggingErrorHandler>>> = OnceCell::const_new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Uncomment this, and restore the console-subscriber dependency in the service Cargo.toml
    // and the tokio tracing feature in the workspace Cargo.toml to use the tokio-console app
    // to measure tokio behavior. See https://github.com/tokio-rs/console for RUSTFLAGS needed
    // as well
    // console_subscriber::init();

    env_logger::Builder::from_default_env()
        .filter_module("sqlx", log::LevelFilter::Warn)
        .init();

    println!("Service starting");
    info!("Starting tccon-priors-service");
    let db_url = orm::get_database_url(None)?;
    let db = orm::get_database_pool(Some(db_url.clone())).await.unwrap();

    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    let config = orm::config::load_config_file_or_default(config_file)?;
    let timing_config = (&config.timing).clone();
    let config = Arc::new(RwLock::new(config));

    let err_handler = error::LoggingErrorHandler{};

    let mut scheduler = clokwerk::AsyncScheduler::new();
    let mut sync_scheduler = clokwerk::Scheduler::new();
    let signals = setup_signals().expect("Could not set up signal handling");

    // JOB MANAGER SETUP //

    let (tx_jobs, rx_jobs) = mpsc::channel::<jobs::JobMessage>(256);        
    let job_man_handle = {
        let db = db.clone();
        let shared_config = Arc::clone(&config);
        let err_handler = err_handler.clone();
        tokio::spawn(async move {
            let mut job_manager: jobs::JobManager<jobs::ServiceJobRunner, error::LoggingErrorHandler> = jobs::JobManager::new_from_pool(
                db, 
                shared_config, 
                err_handler.clone(), 
                rx_jobs
            ).await.expect("Failed to initialize job manager");

            if let Err(e) = job_manager.reset_running_jobs().await {
                err_handler.report_error_with_context(
                    e.as_ref(), 
                    "Error occurred during job manager start up while trying to reset jobs left as 'running' in the database. These jobs will likely still be stuck as 'running'."
                );
            }
            job_manager.message_loop().await;
        })  
    };

    if !timing_config.disable_job {
        info!("Setting up job parsing and execution to run");
        let tx_run_jobs = tx_jobs.clone();
        sync_scheduler
            .every(timing_config.job_start_seconds.seconds())
            .run(move || {
                debug!("Scheduler: sending StartJobs message");
                match tx_run_jobs.try_send(jobs::JobMessage::StartJobs) {
                    Ok(_) => debug!("Scheduler: StartJobs message sent"),
                    Err(TrySendError::Closed(_)) => warn!("Could not send StartJobs message, channel closed"),
                    Err(TrySendError::Full(_)) => warn!("Could not send StartJobs message, channel full"),
                }
                
            });
    } else {
        warn!("Job parsing/execution will NOT run");
    }

    if !timing_config.disable_lut_regen {
        info!("Setting up strat LUT regen to run");
        let tx_lut_regen = tx_jobs.clone();
        let lut_job = sync_scheduler
            .every(timing_config.lut_regen_days.days())
            .run(move || {
                debug!("Scheduler: sending RegenLut message");
                match tx_lut_regen.try_send(jobs::JobMessage::RegenLut) {
                    Ok(_) => (),
                    Err(TrySendError::Closed(_)) => warn!("Could not send RegenLut message, channel closed"),
                    Err(TrySendError::Full(_)) => warn!("Could not send RegenLut message, channel closed")
                }
            });
        if let Some(at) = timing_config.lut_regen_at {
            lut_job.at(&at);
            }
    } else {
        warn!("LUT regen will NOT be run");
    }

    // END JOB MANAGER SETUP //

    // MET MANAGER SETUP //

    let met_manager: met::MetManager::<LoggingErrorHandler> = met::MetManager::new_with_pool(
        db.clone(), 
        Arc::clone(&config), 
        LoggingErrorHandler {  }
    ).await;

    MET_MANAGER.set(Mutex::new(met_manager)).expect("Could not set the global met manager");

    if !timing_config.disable_met_download {
        info!("Setting up met download to run");
        scheduler
            .every(timing_config.met_download_hours.hours())
            .run(|| async {
                // Should be safe to unwrap, will only be None if MET_MANAGER wasn't set, and 
                // we did that above
                let mutex = MET_MANAGER.get().unwrap();
                let mut mm = mutex.lock().await;
                mm.scheduler_entry_point().await;
            });
    } else {
        warn!("Met downloads will NOT run");
    }

    // END MET MANAGER SETUP //

    // Start scheduler

    let (tx_scheduler, mut rx_scheduler) = mpsc::channel::<bool>(4);
    let schedule_handle = tokio::spawn(async move {
        loop {
            match rx_scheduler.try_recv() {
                Ok(true) => { 
                    info!("Stopping scheduler loop");
                    break;
                },
                Ok(false) => {
                    debug!("Heartbeat scheduler message received");
                },
                Err(TryRecvError::Disconnected) => {
                    warn!("Scheduler receiver disconnected, aborting loop");
                    break;
                },
                Err(TryRecvError::Empty) => ()
            }
            debug!("Running pending jobs in scheduler");
            sync_scheduler.run_pending();
            debug!("Finished running pending jobs in scheduler");
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });

    // Start signal processing
    let signal_handle = {
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            process_signals(signals, config, tx_scheduler, tx_jobs).await
                .unwrap_or_else(|e| error!("Error occurred while processing signals: {e}"));
        })
    };

    tokio::try_join!(
        job_man_handle,
        schedule_handle,
        signal_handle
    ).map(|_| ())
    .unwrap_or_else(|e| {
        err_handler.report_error_with_context(&e, "Error occurred in join on all top level threads");
    });

    

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ExitCommand {
    Graceful,
    Rapid
}

fn setup_signals() -> std::io::Result<Signals> {
    // Copied from https://docs.rs/signal-hook/latest/signal_hook/index.html
    // This should make it so that two Ctrl+C signals will immediately exit
    let term_now = Arc::new(AtomicBool::new(true));
    for sig in signal_hook::consts::TERM_SIGNALS {
        signal_hook::flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now))?;
        signal_hook::flag::register(*sig, Arc::clone(&term_now))?;
    }

    let mut sigs = vec![
        signal::SIGHUP, // we'll use this to reload the config, signal_hook docs imply that is common for daemons
    ];
    // this should include SIGTERM, SIGQUIT, and SIGINT. INT will be our graceful shutdown, the other two our rapid
    // shutdown.
    sigs.extend(signal_hook::consts::TERM_SIGNALS);

    Signals::new(sigs)
}

async fn process_signals(
    mut signals: Signals, 
    config: Arc<RwLock<orm::config::Config>>, 
    tx_scheduler: Sender<bool>,
    tx_jobs: Sender<JobMessage>
    ) -> anyhow::Result<()> {
    // If I understand the signal_hook docs correctly, this should be an infinite loop.
    for sig in &mut signals {
        match sig {
            signal::SIGHUP => {
                let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
                info!("Reloading configuration");
                let new_config = orm::config::load_config_file_or_default(config_file)?;
                let mut global_config = config.write().await;
                *global_config = new_config; // todo: verify this works
                
            }, // reload config
            signal::SIGINT => {
                info!("Beginning graceful shutdown");
                shutdown_components(ExitCommand::Graceful, tx_scheduler, tx_jobs).await;
                info!("Graceful shutdown complete");
                break;
            },
            signal::SIGTERM | signal::SIGQUIT => {
                info!("Beginning rapid shutdown");
                shutdown_components(ExitCommand::Rapid, tx_scheduler, tx_jobs).await;
                info!("Rapid shutdown complete");
                break;
            },
            _ => {
                info!("Received signal {sig}, taking no action");
            }
        }
    }

    Ok(())
}

async fn shutdown_components(
    exit_cmd: ExitCommand, 
    tx_scheduler: Sender<bool>,
    tx_jobs: Sender<JobMessage>
) {
    tx_scheduler.send(true).await
        .unwrap_or_else(|e| error!("Could not send shutdown message to scheduler: {e}"));

    match exit_cmd {
        ExitCommand::Graceful => {
            tx_jobs.send(JobMessage::StopGracefully).await
                .unwrap_or_else(|e| error!("Could not send graceful shutdown message to jobs manager: {e}"));
        },
        ExitCommand::Rapid => {
            tx_jobs.send(JobMessage::StopRapidly).await
            .unwrap_or_else(|e| error!("Could not send rapid shutdown message to jobs manager: {e}"));
        },
    }
}