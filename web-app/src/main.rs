use std::{sync::Arc, fmt::Debug};

use axum::{extract::State, http::StatusCode, response::Html, routing::{get, post}, Router};
use axum_login::{login_required, AuthManagerLayerBuilder};
use log::{error, info, debug};
use tower_http::services::ServeDir;

use orm;

mod auth;
mod auth_web;
mod templates;
mod jobs;

use templates::{TEMPLATES, make_base_context};
use tower_sessions::cookie::time::Duration;


struct AppState {
    pool: orm::PoolWrapper
}

impl AppState {
    async fn new() -> anyhow::Result<Self> {
        let pool = orm::get_database_pool(None).await?;
        Ok(Self { pool })
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

fn load_config() -> Result<orm::config::Config, StatusCode> {
    let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
    if let Some(cf) = &config_file {
        debug!("Loading configuration from {}", cf.to_string_lossy());
    } else {
        debug!("Will use default config");
    }
    server_error(orm::config::load_config_file_or_default(config_file))
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
        .route("/job-statuses", get(job_statuses))
        .route("/submit-job", get(submit_job))
        .route("/job-queue", get(job_queue))
        .route("/std-sites", get(std_sites))
        .route("/met-data", get(met_data));

    let unprotected_routes = Router::new()
        .route("/", get(home))
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


async fn home() -> Result<Html<String>, StatusCode> {
    let context = make_base_context("Home", "home");
    let page_source = TEMPLATES.render("home.html", &context).unwrap();
    Ok(Html(page_source))
}

async fn job_statuses() -> Result<Html<String>, StatusCode> {
    let context = make_base_context("Job statuses", "job-statuses");
    let page_source = TEMPLATES.render("job-statuses.html", &context).unwrap();
    Ok(Html(page_source))
}


async fn job_queue(State(state): State<Arc<AppState>>) -> Result<Html<String>, StatusCode> {
    let mut context = make_base_context("Job queue", "job-queue");

    let mut conn = server_error(state.pool.get_connection().await)?;
    let config = load_config()?;
    let jobs_list = server_error(jobs::make_queue_jobs_list(&mut conn, &config).await)?;
    let jobs_table = server_error(jobs::make_jobs_queue_html(
        &jobs_list, &["state", "short_site_locs", "start_date", "end_date", "email", "mod_fmt", "vmr_fmt", "map_fmt"]
    ))?;
    context.insert("queue_table", &jobs_table);
    
    let page_source = TEMPLATES.render("job-queue.html", &context).unwrap();
    Ok(Html(page_source))
}


async fn submit_job() -> Result<Html<String>, StatusCode> {
    let context = make_base_context("Submit job", "submit-job");
    let page_source = TEMPLATES.render("submit-job.html", &context).unwrap();
    Ok(Html(page_source))
}


async fn met_data() -> Result<Html<String>, StatusCode> {
    let context = make_base_context("Met data", "met-data");
    let page_source = TEMPLATES.render("met-data.html", &context).unwrap();
    Ok(Html(page_source))
}

async fn std_sites() -> Result<Html<String>, StatusCode> {
    let context = make_base_context("Standard sites", "std-sites");
    let page_source = TEMPLATES.render("std-sites.html", &context).unwrap();
    Ok(Html(page_source))
}