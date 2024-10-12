use std::{sync::Arc, fmt::Debug};

use axum::{extract::State, http::StatusCode, routing::{get, post}, Router};
use axum_login::{login_required, AuthManagerLayerBuilder};
use log::{error, info, debug};
use tower_http::services::ServeDir;

use orm;

mod templates_common;
mod auth;
mod auth_web;
mod home;
mod jobs;
mod met_data;
mod std_sites;

use tower_sessions::cookie::time::Duration;

type AppStateRef = State<Arc<AppState>>;

struct AppState {
    pool: orm::PoolWrapper,
    root_uri: String,
}

impl AppState {
    async fn new() -> anyhow::Result<Self> {
        let pool = orm::get_database_pool(None).await?;
        let root_uri = match std::env::var("PRIORS_WEB_ROOT_URI") {
            Ok(v) => v,
            Err(std::env::VarError::NotPresent) => "/".to_string(),
            Err(e) => return Err(e.into())
        };

        Ok(Self { pool, root_uri })
    }

    pub(crate) fn clone_pool(&self) -> orm::PoolWrapper {
        self.pool.clone()
    }
}


fn server_error<T, E: Debug>(res: Result<T, E>) -> Result<T, StatusCode> {
    match res {
        Ok(v) => Ok(v),
        Err(e) => {
            error!("Encountered an error: {e:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn load_automation_config() -> anyhow::Result<orm::config::Config> {
    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    if let Some(cf) = &config_file {
        debug!("Loading configuration from {}", cf.to_string_lossy());
    } else {
        debug!("Will use default config");
    }
    orm::config::load_config_file_or_default(config_file)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let address = "127.0.0.1:8080";

    let state = AppState::new().await.expect("Could not set up shared state");
    let pool = state.clone_pool();
    let shared_state = Arc::new(state);

    // Set up session middleware. Should use SQL session eventually, memory is just for
    // initial development.
    let session_store = tower_sessions::MemoryStore::default();
    let key = tower_sessions::cookie::Key::generate();
    let session_layer = tower_sessions::SessionManagerLayer::new(session_store)
        .with_expiry(tower_sessions::Expiry::OnInactivity(Duration::days(1)))
        .with_signed(key);
    let backend = auth::Backend::new(pool);
    let auth_layer = AuthManagerLayerBuilder::new(backend, session_layer).build();

    let protected_routes = Router::new()
        .route("/job-statuses", get(jobs::get::job_statuses))
        .route("/submit-job", get(jobs::get::submit_job))
        .route("/job-queue", get(jobs::get::job_queue))
        .route("/job-downloads", get(jobs::get::job_download))
        .route("/std-sites", get(std_sites::get::std_sites))
        .route("/met-data", get(met_data::get::met_data));

    let unprotected_routes = Router::new()
        .route("/", get(home::get::home))
        .route("/login", post(auth_web::post::login))
        .route("/login", get(auth_web::get::login))
        .route("/logout", get(auth_web::get::logout));

    // TODO: static directory configuration
    let static_server = ServeDir::new("static");
    let app = protected_routes
        .route_layer(login_required!(auth::Backend, login_url = "/login"))
        .merge(unprotected_routes)
        .layer(auth_layer)
        .with_state(shared_state)
        .nest_service("/static", static_server);

    info!("Will serve priors website from {address}");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;

    Ok(())
}