use std::collections::HashMap;

use askama::Template;
use itertools::Itertools;
use orm::{config::Config, met::MetDayState, MySqlConn};

use crate::{
    auth::User,
    templates_common::{sblink_inner, BaseContext, ContextWithSidebar, Sblink},
};

#[derive(Debug, Template)]
#[template(path = "met-data.html")]
struct MetDataContext {
    root_uri: String,
    user: Option<User>,
    start_date: chrono::NaiveDate,
    end_date: chrono::NaiveDate,
    products: Vec<String>,
    dates: Vec<MetDateStatus>,
    files: Vec<MetFileRow>,
}

#[derive(Debug)]
struct MetDateStatus {
    date: chrono::NaiveDate,
    prod_status: HashMap<String, MetDayState>,
}

impl MetDateStatus {
    fn get_status_str(&self, product: &str) -> &'static str {
        let opt_state = self.prod_status.get(product);
        if let Some(state) = opt_state {
            if state.is_complete() {
                return "Complete";
            }
            if state.is_incomplete() {
                return "Partial";
            }
            if state.is_missing() {
                return "Missing";
            }
        }

        return "Unknown";
    }
}

#[derive(Debug)]
struct MetFileRow {
    date: chrono::NaiveDateTime,
    product: String,
    name: String,
}

impl From<orm::met::MetFile> for MetFileRow {
    fn from(value: orm::met::MetFile) -> Self {
        let date = value.filedate;
        let product = value.product_key.to_string();
        let name = value
            .file_path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "?".to_string());
        Self {
            date,
            product,
            name,
        }
    }
}

impl MetDataContext {
    async fn new_from_db(
        root_uri: String,
        user: Option<User>,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate,
        config: &Config,
        conn: &mut MySqlConn,
    ) -> anyhow::Result<Self> {
        // The MetFile methods treat end date as exclusive, but for user-friendliness we want to include it,
        // so we need to add a day to the end date to include it in the results.
        let end_date_excl = end_date + chrono::TimeDelta::days(1);

        let available_products =
            orm::met::MetFile::get_products_with_files_for_dates(conn, start_date, end_date_excl)
                .await?;

        let mut met_files = vec![];
        for prod in available_products.iter() {
            let prod_files =
                orm::met::MetFile::get_files_by_dates(conn, start_date, end_date_excl, Some(prod))
                    .await?
                    .into_iter()
                    .map(|f| MetFileRow::from(f));
            met_files.extend(prod_files);
        }

        let mut date_statuses = vec![];
        for date in orm::utils::DateIterator::new_one_range(start_date, end_date_excl) {
            let proc_cfg_keys = config.get_possible_proc_cfgs_for_date(date);
            let mut prod_status = HashMap::new();
            for key in proc_cfg_keys {
                let keyed_met_cfgs = config.get_mets_for_processing_config(key)?;
                let status =
                    orm::met::MetFile::is_date_complete_for_config_set(conn, date, &keyed_met_cfgs)
                        .await?;
                prod_status.insert(key.to_string(), status);
            }
            date_statuses.push(MetDateStatus { date, prod_status });
        }

        let product_strings = available_products
            .into_iter()
            .map(|p| p.to_string())
            .collect_vec();
        Ok(Self {
            root_uri,
            user,
            start_date,
            end_date,
            products: product_strings,
            dates: date_statuses,
            files: met_files,
        })
    }
}

impl BaseContext for MetDataContext {
    fn subtitle(&self) -> &str {
        "Met data"
    }

    fn page_id(&self) -> &str {
        "met-data"
    }

    fn root_uri(&self) -> &str {
        &self.root_uri
    }

    fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }
}

impl ContextWithSidebar for MetDataContext {
    fn sblink(
        &self,
        resource_uri: &str,
        text: &str,
        curr_page_id: &str,
        link_page_id: &str,
    ) -> Sblink {
        sblink_inner(
            &self.root_uri,
            resource_uri,
            text,
            curr_page_id,
            link_page_id,
        )
    }
}

pub(crate) mod get {
    use askama::Template;
    use axum::{extract::State, http::StatusCode, response::Html, Form};

    use crate::{auth::AuthSession, load_automation_config, server_error, AppStateRef};

    use super::MetDataContext;

    #[derive(serde::Deserialize)]
    pub(crate) struct MetDatesForm {
        start_date: Option<chrono::NaiveDate>,
        end_date: Option<chrono::NaiveDate>,
    }

    pub(crate) async fn met_data(
        State(state): AppStateRef,
        session: AuthSession,
        Form(met_dates): Form<MetDatesForm>,
    ) -> Result<Html<String>, StatusCode> {
        let today = chrono::Local::now().date_naive();
        let start_date = met_dates
            .start_date
            .unwrap_or_else(|| today - chrono::TimeDelta::days(7));
        let end_date = met_dates.end_date.unwrap_or(today);
        let config = server_error(load_automation_config())?;
        let mut conn = server_error(state.pool.get_connection().await)?;
        let res = MetDataContext::new_from_db(
            state.root_uri.clone(),
            session.user,
            start_date,
            end_date,
            &config,
            &mut conn,
        )
        .await;
        let context = server_error(res)?;
        let raw = server_error(context.render())?;
        Ok(Html(raw))
    }
}
