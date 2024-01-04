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