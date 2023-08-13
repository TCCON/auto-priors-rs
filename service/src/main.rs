use std::{sync::{Arc, atomic::AtomicBool}, time::Duration};

use clokwerk::{TimeUnits, Job};
use error::LoggingErrorHandler;
use log::{info, warn};
use signal_hook::{consts::signal, iterator::Signals};
use tokio::sync::{RwLock, Mutex, OnceCell};

mod error;
mod jobs;
mod met;

static JOBS_MANAGER: OnceCell<Mutex<jobs::JobManager<jobs::ServiceJobRunner,LoggingErrorHandler>>> = OnceCell::const_new();
static MET_MANAGER: OnceCell<Mutex<met::MetManager<LoggingErrorHandler>>> = OnceCell::const_new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // env_logger::Builder::from_default_env()
    //     .filter_module("tccon-priors-service", log::LevelFilter::Info)
    //     .filter_module("tccon_priors_orm", log::LevelFilter::Info)
    //     .init();
    env_logger::init();
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
    let mut signals = setup_signals().expect("Could not set up signal handling");

    // JOB MANAGER SETUP //

    let job_manager: jobs::JobManager<jobs::ServiceJobRunner, error::LoggingErrorHandler> = jobs::JobManager::new_from_pool(
        db.clone(), 
        Arc::clone(&config), 
        err_handler.clone()
    ).await.expect("Failed to initialize job manager");
        
    JOBS_MANAGER.set(Mutex::new(job_manager)).expect("Could not set the global job manager");


    if !timing_config.disable_job {
        info!("Setting up job parsing and execution to run");
        scheduler
            .every(timing_config.job_start_seconds.seconds())
            .run(|| async {
                let mutex = JOBS_MANAGER.get().unwrap();
                let mut jm = mutex.lock().await;
                jm.scheduler_entry_point().await;
            });
    } else {
        warn!("Job parsing/execution will NOT run");
    }

    if !timing_config.disable_lut_regen {
        info!("Setting up strat LUT regen to run");
        let lut_job = scheduler
            .every(timing_config.lut_regen_days.days())
            .run(|| async { 
                // Should be safe to unwrap, will only be None if JOBS_MANAGER wasn't set, and 
                // we did that above
                let mutex = JOBS_MANAGER.get().unwrap();
                let mut jm = mutex.lock().await;
                jm.schedule_lut_regen().await;
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

    let schedule_handle = tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

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
                schedule_handle.abort();
                shutdown_components(ExitCommand::Graceful).await;
                break;
            },
            signal::SIGTERM | signal::SIGQUIT => {
                schedule_handle.abort();
                shutdown_components(ExitCommand::Rapid).await;
                break;
            },
            _ => {
                info!("Received signal {sig}, taking no action");
            }
        }
    }

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

async fn shutdown_components(exit_cmd: ExitCommand) {
    if let Some(jobs_manager) = JOBS_MANAGER.get() {
        let mut lock = jobs_manager.lock().await;
        lock.stop_jobs(exit_cmd).await;
    } else {
        warn!("Could not get access to jobs manager to properly stop running jobs; some jobs may be in an incomplete state.");
    }
}