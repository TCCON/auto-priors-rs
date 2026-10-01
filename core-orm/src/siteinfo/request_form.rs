use std::{
    fmt::Display,
    io::{BufRead, BufReader, Read},
    path::Path,
    str::FromStr,
};

use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime};
use serde::Deserialize;

use crate::config::Config;

pub async fn get_request_csv_data(
    config: &Config,
    request_csv_file: Option<&Path>,
    request_sheet_id: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let sheet_id = request_sheet_id.or(config.email.std_site_req_sheet_id.as_deref());

    let csv_data = if let Some(csv_path) = request_csv_file {
        read_request_csv(csv_path).with_context(|| {
            format!(
                "Error reading from local request CSV file {}",
                csv_path.display()
            )
        })?
    } else if let Some(sheet_id) = sheet_id {
        get_std_site_request_csv(sheet_id)
            .await
            .with_context(|| format!("Error downloading request sheet with ID {sheet_id}"))?
    } else {
        return Ok(None);
    };

    Ok(Some(csv_data))
}

async fn get_std_site_request_csv(sheet_id: &str) -> anyhow::Result<String> {
    let url = format!("https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv");
    log::info!("Downloading latest standard site requests from {url}");
    let response = reqwest::get(&url).await?.error_for_status()?;
    // Recommended by Claude: check that the content type is CSV
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("text/csv") {
        anyhow::bail!("Expected 'text/csv' content type, got '{content_type}'");
    }
    let body = response.text().await?;
    Ok(body)
}

fn read_request_csv(csv_file: &Path) -> anyhow::Result<String> {
    log::info!("Reading standard site requests from {}", csv_file.display());
    let mut f = std::fs::File::open(csv_file)?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;
    Ok(buf)
}

pub struct RequestIter<'r> {
    inner: csv::DeserializeRecordsIntoIter<BufReader<&'r [u8]>, RequestRow>,
    only_new_requests: bool,
}

impl<'r> RequestIter<'r> {
    pub fn new(request_csv_data: &'r str, only_new_requests: bool) -> Self {
        let mut rdr = BufReader::new(request_csv_data.as_bytes());
        // We need to skip over the first line because we're ignoring the headers
        // since they are too long to use as field names.
        rdr.skip_until(b'\n')
            .expect("'reading' from a byte slice should not error");
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(rdr);
        let inner = reader.into_deserialize();
        Self {
            inner,
            only_new_requests,
        }
    }
}

impl<'r> Iterator for RequestIter<'r> {
    type Item = Result<RequestRow, csv::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.inner.next();
            if let Some(Ok(row)) = &next {
                if row.decision.is_some() && self.only_new_requests {
                    continue;
                }
            }
            return next;
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RequestRow {
    #[serde(deserialize_with = "deserialize_google_datetime")]
    pub timestamp: NaiveDateTime,
    pub submitter_name: String,
    pub submitter_inst: String,
    _submitter_email: String, // think this was left in the spreadsheet from before I had "collecting emails" turned on
    pub observations: String,
    #[serde(deserialize_with = "deserialize_google_date")]
    pub obs_start_date: NaiveDate,
    #[serde(deserialize_with = "deserialize_google_date_opt")]
    pub obs_end_date: Option<NaiveDate>,
    pub support: String,
    pub is_tccon_member: String,
    pub is_coccon_member: String,
    pub data_distribution: String,
    pub desired_site_id: String,
    pub desired_longitude: String,
    pub desired_latitude: String,
    pub custom_loc_email: Option<String>,
    pub custom_loc_sids: Option<String>,
    pub multiple_instruments: String,
    pub days_per_week: u8,
    pub frac_year: f32,
    pub contact_email: String,
    pub decision: Option<String>,
}

impl FromStr for RequestRow {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let row = split_google_sheets_csv_line(s);
        let record = csv::StringRecord::from(row);
        let request: RequestRow = record.deserialize(None)?;
        Ok(request)
    }
}

impl Display for RequestRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Requested on: {}", self.timestamp)?;
        writeln!(f, "Submitter name: {}", self.submitter_name)?;
        writeln!(f, "Submitter institution: {}", self.submitter_inst)?;
        writeln!(
            f,
            "Observations for which priors are requested: {}",
            self.observations
        )?;
        if let Some(end) = self.obs_end_date {
            writeln!(
                f,
                "Observations date range: {} to {}",
                self.obs_start_date, end
            )?;
        } else {
            writeln!(
                f,
                "Observations date range: {} to (no end date)",
                self.obs_start_date
            )?;
        }
        writeln!(f, "Observation funding/support: {}", self.support)?;
        writeln!(f, "TCCON member: {}", self.is_tccon_member)?;
        writeln!(f, "COCCON member: {}", self.is_coccon_member)?;
        writeln!(
            f,
            "Data distribution w/i one year: {}",
            self.data_distribution
        )?;
        writeln!(
            f,
            "Desired site ID, long, lat: {}, {}, {}",
            self.desired_site_id, self.desired_longitude, self.desired_latitude
        )?;
        writeln!(
            f,
            "Email used to request custom location: {}",
            self.custom_loc_email.as_deref().unwrap_or("Not supplied")
        )?;
        writeln!(
            f,
            "Site ID(s) used to request custom location: {}",
            self.custom_loc_sids.as_deref().unwrap_or("Not supplied")
        )?;
        writeln!(
            f,
            "Multiple instruments w/i 100 km: {}",
            self.multiple_instruments
        )?;
        writeln!(
            f,
            "Avg. days per week obs. attempted: {}",
            self.days_per_week
        )?;
        writeln!(
            f,
            "Avg. percent of year obs. attempted: {:.1}%",
            self.frac_year * 100.0
        )?;
        writeln!(f, "Contact email: {}", self.contact_email)?;
        Ok(())
    }
}

fn deserialize_google_datetime<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let dt = chrono::NaiveDateTime::parse_from_str(&s, "%-m/%-d/%Y %H:%M:%S")
        .map_err(serde::de::Error::custom)?;
    Ok(dt)
}

fn deserialize_google_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_google_date_opt(deserializer)?.ok_or_else(|| {
        serde::de::Error::custom("Got an empty string when expecting a non-optional date")
    })
}

fn deserialize_google_date_opt<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.is_empty() {
        return Ok(None);
    }

    let d =
        chrono::NaiveDate::parse_from_str(&s, "%-m/%-d/%Y").map_err(serde::de::Error::custom)?;
    Ok(Some(d))
}

fn split_google_sheets_csv_line(line: &str) -> Vec<String> {
    fn prune_entry(entry: &str) -> String {
        let entry = if entry.starts_with('"') && entry.ends_with('"') {
            let n = entry.len();
            &entry[1..n - 1]
        } else {
            entry
        };
        entry.replace("\"\"", "\"")
    }
    let mut entries = vec![];
    let mut it = line.char_indices().peekable();
    let mut istart = 0;
    let mut in_quotes = false;
    loop {
        if let Some((i, c)) = it.next() {
            if c == ',' && !in_quotes {
                // If there are commas in the actual text of a cell, then the cell should be quoted
                // Otherwise when we see a command, that is our cue to split. Also trim leading or
                // trailing quotes (which are usually there to allow commas in the cell) and replace
                // any "" with just " (since two " in a row is an escaped ")
                entries.push(prune_entry(&line[istart..i]));
                istart = i + 1;
            } else if c == '"' {
                // Google seems to use two double quotes in a row to escape literal quote,
                // so if we see a quote, check if the next character is also a quote.
                let next_c = it.peek().map(|(_, c)| *c).unwrap_or(' ');
                if next_c == '"' {
                    // The this means that there's two quotes in a row, one is an escape, so
                    // skip over the next one.
                    it.next();
                } else {
                    // Otherwise, toggle whether we are in or out of quotes
                    in_quotes = !in_quotes;
                }
            }
        } else {
            entries.push(prune_entry(&line[istart..]));
            break;
        }
    }

    entries
}

#[cfg(test)]
mod tests {
    use crate::test_utils;
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_google_sheets_split() {
        let line = r#"Normal,"Has ""quotes""",Has 'single quotes',"Where, ""comma, quote""""#;
        let expected = [
            "Normal",
            r#"Has "quotes""#,
            r#"Has 'single quotes'"#,
            r#"Where, "comma, quote""#,
        ];
        let split_vals = split_google_sheets_csv_line(line);
        assert_eq!(split_vals, expected);
    }

    #[test]
    fn test_request_sheet_iter_all() {
        test_utils::init_logging();
        let request_csv_data =
            include_str!("../test_inputs/apriori_request_responses_20260930.csv");
        let iter = RequestIter::new(request_csv_data, false);

        let requests: Vec<RequestRow> = iter
            .try_collect()
            .expect("parsing test request CSV should work");

        let desired_sids = requests
            .iter()
            .map(|r| r.desired_site_id.as_str())
            .collect_vec();
        let custom_sids = requests
            .iter()
            .map(|r| r.custom_loc_sids.as_deref().unwrap_or(""))
            .collect_vec();
        let decisions = requests.iter().map(|r| r.decision.is_some()).collect_vec();

        assert_eq!(
            desired_sids,
            vec![
                "mu",
                "to",
                "al",
                "cg",
                "",
                "GG",
                "ed",
                "sg and sh",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                ""
            ]
        );
        assert_eq!(
            custom_sids,
            vec![
                "tu",
                "ta, tb, tc, td, te, tf, tg, th",
                "al (Altzomoni)",
                "cg",
                "",
                "",
                "ed",
                "",
                "",
                "ac",
                "ci",
                "ps",
                "ar",
                "ma",
                "bc",
                "sc"
            ]
        );
        assert_eq!(
            decisions,
            vec![
                true, true, true, true, true, true, true, true, true, false, false, false, false,
                false, false, false
            ]
        );
    }

    #[test]
    fn test_request_sheet_iter_new_only() {
        test_utils::init_logging();
        let request_csv_data =
            include_str!("../test_inputs/apriori_request_responses_20260930.csv");
        let iter = RequestIter::new(request_csv_data, true);

        let requests: Vec<RequestRow> = iter
            .try_collect()
            .expect("parsing test request CSV should work");

        let desired_sids = requests
            .iter()
            .map(|r| r.desired_site_id.as_str())
            .collect_vec();
        let custom_sids = requests
            .iter()
            .map(|r| r.custom_loc_sids.as_deref().unwrap_or(""))
            .collect_vec();
        let decisions = requests.iter().map(|r| r.decision.is_some()).collect_vec();

        assert_eq!(desired_sids, vec!["", "", "", "", "", "", ""]);
        assert_eq!(custom_sids, vec!["ac", "ci", "ps", "ar", "ma", "bc", "sc"]);
        assert_eq!(
            decisions,
            vec![false, false, false, false, false, false, false]
        );
    }
}
