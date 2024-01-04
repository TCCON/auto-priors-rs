use axum::{Router, routing::get, response::Html, http::StatusCode};
use lazy_static::lazy_static;
use log::{info, debug};
use tera::{Tera, Context};
use tower_http::services::ServeDir;

lazy_static! {
    pub static ref TEMPLATES: Tera = {
        let mut tera = match Tera::new("templates/**/*") {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Parsing error: {e}");
                ::std::process::exit(1);
            }
        };

        tera.autoescape_on(vec![".html", ".sql"]);
        tera.register_function("uri", TeraUris::default());
        tera.register_function("sblink", TeraSidebarEntry::default());
        tera
    };
}

struct TeraUris {
    root_uri: String
}

impl Default for TeraUris {
    fn default() -> Self {
        Self { root_uri: "/".to_string() }
    }
}

impl tera::Function for TeraUris {
    fn call(&self, args: &std::collections::HashMap<String, tera::Value>) -> tera::Result<tera::Value> {
        let uri = match args.get("uri") {
            Some(val) => match tera::from_value::<String>(val.clone()) {
                Ok(p) => p,
                Err(_) => return Err("Unable to convert the 'file' parameter into a string".into())
            },
            None => return Err("Must provide a 'uri' parameter, e.g. uri='static/main.css'".into())
        };

        let url = format!("{}/{}", self.root_uri.trim_end_matches("/"), uri.trim_start_matches("/"));
        debug!("Joining URI {uri} to root {}: {url}", self.root_uri);
        Ok(tera::to_value(url)?)
    }

    fn is_safe(&self) -> bool {
        true
    }
}

struct TeraSidebarEntry {
    root_uri: String
}

impl Default for TeraSidebarEntry {
    fn default() -> Self {
        Self { root_uri: "/".to_string() }
    }
}

impl tera::Function for TeraSidebarEntry {
    fn call(&self, args: &std::collections::HashMap<String, tera::Value>) -> tera::Result<tera::Value> {
        let uri = match args.get("uri") {
            Some(val) => match tera::from_value::<String>(val.clone()) {
                Ok(p) => p,
                Err(_) => return Err("Unable to convert the 'uri' parameter into a string".into())
            },
            None => return Err("Must provide a 'uri' parameter, e.g. uri='/queue'".into())
        };

        let text = match args.get("text") {
            Some(val) => match tera::from_value::<String>(val.clone()) {
                Ok(p) => p,
                Err(_) => return Err("Unable to convert the 'text' parameter into a string".into())
            },
            None => return Err("Must provide a 'text' parameter, e.g. text='Job queue'".into())
        };

        let link_page_id = match args.get("link_page_id") {
            Some(val) => match tera::from_value::<String>(val.clone()) {
                Ok(p) => p,
                Err(_) => return Err("Unable to convert the 'link_page_id' parameter into a string".into())
            },
            None => uri.trim_start_matches("/").to_string()
        };

        let curr_page_id = match args.get("page_id") {
            Some(val) => match tera::from_value::<String>(val.clone()) {
                Ok(p) => p,
                Err(_) => return Err("Unable to convert the 'page_id' parameter into a string".into())
            },
            None => return Err("Must provide a 'page_id' parameter, e.g. page_id=page_id".into())
        };

        let classes = if curr_page_id == link_page_id {
            "sidebar-current-page"
        } else {
            ""
        };

        let url = format!("{}/{}", self.root_uri.trim_end_matches("/"), uri.trim_start_matches("/"));
        let html = format!(r#"<div class="{classes}"><a href="{url}">{text}</a></div>"#);
        Ok(tera::to_value(html)?)
    }

    fn is_safe(&self) -> bool {
        true
    }
}

fn make_base_context(page_subtitle: &str, page_id: &str) -> Context {
    let mut context = Context::new();
    context.insert("subtitle", page_subtitle);
    context.insert("page_id", page_id);
    context
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let address = "127.0.0.1:8080";

    // TODO: static directory configuration
    let static_server = ServeDir::new("static");
    let app = Router::new()
        .route("/", get(home))
        .route("/job-statuses", get(job_statuses))
        .route("/submit-job", get(submit_job))
        .route("/job-queue", get(job_queue))
        .route("/std-sites", get(std_sites))
        .route("/met-data", get(met_data))
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


async fn job_queue() -> Result<Html<String>, StatusCode> {
    let context = make_base_context("Job queue", "job-queue");
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