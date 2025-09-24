use std::fmt::Display;

use chrono::NaiveDate;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{config::Config, met::CheckMetAvailableError, MySqlConn};

/// Inner structure to serialize the locations requested for a job
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct RequestSite {
    pub site_id: String,
    pub lat: Option<f32>,
    pub lon: Option<f32>,
}

pub(crate) struct FinalizedRequestValues {
    pub(crate) site_ids: Vec<String>,
    pub(crate) lats: Vec<Option<f32>>,
    pub(crate) lons: Vec<Option<f32>>,
    pub(crate) ginput_key: Option<String>,
    pub(crate) met_key: Option<String>,
}

pub(super) async fn check_and_transform_request_params(
    conn: &mut MySqlConn,
    config: &Config,
    sites: Vec<RequestSite>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    reanalysis: Option<&str>,
) -> Result<FinalizedRequestValues, Vec<String>> {
    let mut errors = vec![];

    if sites.iter().any(|s| s.site_id.len() != 2) {
        errors.push("site IDs be all be two characters".to_string());
    }

    let mut site_ids = vec![];
    let mut lats = vec![];
    let mut lons = vec![];

    for (isite, site) in sites.into_iter().enumerate() {
        let mut is_ok = true;
        if site.site_id.len() != 2 {
            errors.push(format!(
                "Site IDs must all be two characters: the ID for site #{} ({}) is not",
                isite + 1,
                site.site_id
            ));
            is_ok = false;
        }

        if site.lat.is_none() != site.lon.is_none() {
            errors.push(format!(
                "Site #{} ({}) specifies lat OR lon, but not both - sites must give both lat and lon or neither",
                isite+1,
                site.site_id
            ));
            is_ok = false;
        }

        if site.lat.is_some_and(|y| y < -90.0 || y > 90.0) {
            errors.push(format!(
                "Latitude for site #{} ({}) is outside the allowed range of -90 to +90",
                isite + 1,
                site.site_id
            ));
            is_ok = false;
        }

        if site.lon.is_some_and(|x| x < -180.0 || x > 180.0) {
            errors.push(format!(
                "Longitude for site #{} ({}) is outside the allowed range of -180 to +180",
                isite + 1,
                site.site_id
            ));
        }

        if is_ok {
            site_ids.push(site.site_id);
            lats.push(site.lat);
            lons.push(site.lon);
        }
    }

    // Check that the request asks for at least one day
    if start_date >= end_date {
        errors.push(format!(
            "End date ({end_date}) must be at least one day after the start date ({start_date})"
        ));
    }

    // Check that the user's job isn't too long, unless missing start/end dates was one of the problems
    // encountered in the input file
    if let Some(max_days) = config.execution.job_max_days {
        let job_ndays = (end_date - start_date).num_days();
        if job_ndays > max_days as i64 {
            log::info!("Rejecting job from because it requests too many days: {job_ndays} requested vs. {max_days} allowed");
            errors.push(format!("Too many days requested: {job_ndays} requested but the maximum allowed is {max_days}"));
        }
    }
    // Confirm that the required met files are available, unless missing start/end dates
    // was one of the problems encountered in the input file
    if let Err(e) = check_met_available(conn, config, start_date, end_date).await {
        log::error!(
            "Error occurred while checking met file availability for request between {start_date} and {end_date}: {e:?}"
        );
        errors.push(e.to_problem());
    }

    // Check if the user requested a met other than the default and, if so, whether
    // it is a valid key and if the dates are okay.
    let (met_key, ginput_key) = if let Some(key) = reanalysis {
        match config.requests.check_met_request(key, start_date, end_date) {
            Ok(met) => (
                Some(met.met_key.to_string()),
                Some(met.ginput_key.to_string()),
            ),
            Err(msg) => {
                errors.push(format!("Invalid reanalysis: {msg}"));
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    if errors.is_empty() {
        Ok(FinalizedRequestValues {
            site_ids,
            lats,
            lons,
            ginput_key,
            met_key,
        })
    } else {
        return Err(errors);
    }
}

async fn check_met_available(
    conn: &mut crate::MySqlConn,
    config: &Config,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(), MissingMetError> {
    let mut missing_dates = vec![];
    let mut unsupported_dates = vec![];

    for date in crate::utils::DateIterator::new_one_range(start_date, end_date) {
        let (missing, unsupported) = match crate::met::MetFile::is_date_complete_for_default_mets(
            conn, config, date,
        )
        .await
        {
            Ok(state) => (!state.is_complete(), false),
            Err(CheckMetAvailableError::NoDefaultsDefined(_)) => (true, true),
            Err(CheckMetAvailableError::Other(e)) => return Err(e.into()),
        };

        if missing {
            missing_dates.push(date);
        }

        if unsupported {
            unsupported_dates.push(date);
        }
    }

    if !unsupported_dates.is_empty() {
        // Unsupported dates take precedence over missing, since these will never be available
        Err(MissingMetError::UnsupportedDate(unsupported_dates))
    } else if !missing_dates.is_empty() {
        Err(MissingMetError::MissingDates(missing_dates))
    } else {
        Ok(())
    }
}

#[derive(Debug)]
enum MissingMetError {
    CouldNotCheck(anyhow::Error),
    MissingDates(Vec<NaiveDate>),
    UnsupportedDate(Vec<NaiveDate>),
}

impl MissingMetError {
    fn to_problem(self) -> String {
        match self {
            MissingMetError::CouldNotCheck(_) => {
                // don't use Display impl here; don't want to expose inner errors to an email
                "There was an error while verifying that the met data required for your request. Please try resubmitting. If the error persists, contact the adminstrators of the GGG priors automation.".to_string()
            }
            MissingMetError::MissingDates(_) => {
                format!("Your request could not be fulfilled: {self}. If you believe this should not be the case, contact the GGG priors automation administrators.")
            }
            MissingMetError::UnsupportedDate(_) => {
                format!("Your request could not be fulfilled: {self}. Please review the TCCON wiki (https://tccon-wiki.caltech.edu/Main/ObtainingGinputData) for supported date ranges.")
            }
        }
    }
}

impl From<anyhow::Error> for MissingMetError {
    fn from(value: anyhow::Error) -> Self {
        Self::CouldNotCheck(value)
    }
}

impl Display for MissingMetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MissingMetError::CouldNotCheck(e) => write!(
                f,
                "error occurred while checking met availability for job request: {e}"
            ),
            MissingMetError::MissingDates(dates) => {
                let n = dates.len();
                let date_str = dates.iter().map(|d| d.to_string()).join(", ");
                write!(
                    f,
                    "met data was unavailable for {n} of the dates requested: {date_str}"
                )
            }
            MissingMetError::UnsupportedDate(dates) => {
                let n = dates.len();
                let date_str = dates.iter().map(|d| d.to_string()).join(", ");
                write!(f, "{n} of the requested dates are not supported (likely due to lack of met data): {date_str}")
            }
        }
    }
}
