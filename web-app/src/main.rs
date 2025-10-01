use std::{fmt::Debug, sync::Arc};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use axum_login::{login_required, AuthManagerLayerBuilder};
use jsonwebtoken::{DecodingKey, EncodingKey};
use log::{debug, error, info};
use tower_http::services::ServeDir;

use orm::auth;
use orm::{self, config::Config};

mod api;
mod auth_web;
mod home;
mod jobs;
mod met_data;
mod std_sites;
mod templates_common;

use tower_sessions::cookie::time::Duration;
use utoipa_axum::routes;

use crate::api::middleware::{api_has_download_perm, api_has_query_perm, api_has_submit_perm};

type AppStateRef = State<Arc<AppState>>;

#[derive(Clone)]
struct AppState {
    pool: orm::PoolWrapper,
    config: Config,
    root_uri: String,
    decoding_key: DecodingKey,
    encoding_key: EncodingKey,
}

impl AppState {
    async fn new() -> anyhow::Result<Self> {
        let pool = orm::get_database_pool(None).await?;
        let root_uri = match std::env::var("PRIORS_WEB_ROOT_URI") {
            Ok(v) => v,
            Err(std::env::VarError::NotPresent) => "/".to_string(),
            Err(e) => return Err(e.into()),
        };

        let config_file = std::env::var_os(orm::config::CFG_FILE_ENV_VAR);
        let config = orm::config::load_config_file_or_default(config_file)?;
        info!("Loaded config file");

        let (encoding_key, decoding_key) =
            auth::load_jwt_hmac_secret(&config.auth.hmac_secret_file)?;

        Ok(Self {
            pool,
            config,
            root_uri,
            decoding_key,
            encoding_key,
        })
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

    // TODO: add root URI to configuration
    let state = AppState::new()
        .await
        .expect("Could not set up shared state");
    let pool = state.clone_pool();
    let shared_state = Arc::new(state);

    // Set up session middleware. Should use SQL session eventually, memory is just for
    // initial development.
    let session_store = tower_sessions::MemoryStore::default();
    let key = tower_sessions::cookie::Key::generate();
    let session_layer = tower_sessions::SessionManagerLayer::new(session_store)
        .with_expiry(tower_sessions::Expiry::OnInactivity(Duration::days(1)))
        .with_signed(key);
    let backend = auth::WebBackend::new(pool);
    let auth_layer = AuthManagerLayerBuilder::new(backend, session_layer).build();

    let protected_routes = Router::new()
        .route("/submit-job", post(jobs::post::submit_job))
        .route("/submit-job", get(jobs::get::submit_job))
        .route("/job-queue", get(jobs::get::job_queue))
        .route("/job-downloads", get(jobs::get::job_download))
        .route("/std-sites", get(std_sites::get::std_sites))
        .route("/met-data", get(met_data::get::met_data));

    let api_routes = set_up_api(shared_state.clone());

    let unprotected_routes = Router::new()
        .route("/", get(home::get::home))
        .route("/api-docs", get(api::get::api_docs))
        .route("/login", post(auth_web::post::login))
        .route("/login", get(auth_web::get::login))
        .route("/logout", get(auth_web::get::logout));

    // TODO: static directory configuration
    let static_server = ServeDir::new("static");
    let app = protected_routes
        .route_layer(login_required!(auth::WebBackend, login_url = "/login"))
        .merge(api_routes)
        .merge(unprotected_routes)
        .layer(auth_layer)
        .with_state(shared_state)
        .nest_service("/static", static_server);

    info!("Will serve priors website from {address}");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn set_up_api(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let query_routes = Router::new()
        .route(
            "/api/v1/query/check",
            get(api::check::get::check_api_access),
        )
        .route(
            "/api/v1/query/all-jobs",
            get(api::query::get::query_all_jobs),
        )
        .route(
            "/api/v1/query/active-jobs",
            get(api::query::get::query_active_jobs),
        )
        .route(
            "/api/v1/query/job-status/{job_id}",
            get(api::query::get::query_job),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            api_has_query_perm,
        ));

    // let submit_routes = Router::new()
    //     .route(
    //         "/api/v1/submit/check",
    //         get(api::check::get::check_api_access),
    //     )
    //     .route("/api/v1/jobs/submit", post(api::jobs::post::submit_job))
    //     .route_layer(axum::middleware::from_fn_with_state(
    //         state.clone(),
    //         api_has_submit_perm,
    //     ));

    let (submit_routes, api) = utoipa_axum::router::OpenApiRouter::new()
        .routes(routes!(api::jobs::post::submit_job))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            api_has_submit_perm,
        ))
        .split_for_parts();

    let (download_routes, api) = utoipa_axum::router::OpenApiRouter::with_openapi(api)
        .routes(routes!(api::download::get::download_job_output))
        .routes(routes!(api::download::get::download_std_site_output))
        .split_for_parts();

    let download_routes = download_routes.route_layer(axum::middleware::from_fn_with_state(
        state.clone(),
        api_has_download_perm,
    ));

    let doc_routes = api::documentation::DocAllEndpointBuilder::new(api)
        .json_url("/api/v1/docs/json")
        .build();

    query_routes
        .merge(submit_routes)
        .merge(download_routes)
        .merge(doc_routes)
}
