use std::{collections::HashMap, fmt::Display, path::Path};

use anyhow::{self, Context};
use chrono::{Datelike, NaiveDate};
use clap::{self, Args, Subcommand};
use inquire::{validator::Validation, InquireError};
use itertools::Itertools;
use log::warn;
use orm::{
    self,
    config::Config,
    siteinfo::{
        request_form::{get_request_csv_data, RequestIter, RequestRow},
        JsonType, SiteInfo, SiteType, StdOutputStructure, StdSite,
    },
    MySqlConn,
};
use sqlx::Connection;
use strum::VariantArray;

use crate::shared_cli;

/// Manage definition of standard sites and their locations
#[derive(Debug, Args)]
pub struct StdSiteCli {
    #[clap(subcommand)]
    pub command: StdSiteActions,
}

#[derive(Debug, Subcommand)]
pub enum StdSiteActions {
    AddSite(AddNewStdSiteCli),
    Edit(EditSiteCli),
    Print(PrintSitesCli),
    AddInfo(AddSiteInfoCli),
    AddFromReq(AddSitesFromRequestCli),
    SetNonop(SetNonopCli),
    DeleteInfo(DeleteInfoCli),
    PrintInfo(PrintLocsCli),
    Json(InfoJsonCli),
}

#[derive(Debug, Args)]
/// Return a JSON string of information about standard sites
pub struct InfoJsonCli {
    /// Which type of JSON to return. "flat" will be a list with one entry per
    /// site time period. If the same site has multiple time periods (e.g. how
    /// Darwin moved slightly), there will be multiple elements in the list with
    /// the same site ID. "grouped" will be a map with one element per site ID,
    /// each time period will be in a list of maps in each element.
    json_type: JsonType,

    /// Return the JSON in minified format, rather than pretty-printed
    #[clap(short = 'm', long = "minified")]
    minified: bool,

    /// Provide site information for which sites were active on a given date,
    /// rather than all information. By default, only sites which were active
    /// on this date are returned, but this can be modified by the --inactive flag.
    #[clap(short = 'd', long = "date")]
    date: Option<NaiveDate>,

    /// Changes the behavior of --date such that the returned JSON includes a value
    /// for every site, even if it was not active on the given date. In that case,
    /// the site information closest in time to the given date is provided.
    #[clap(short = 'i', long = "inactive")]
    inactive: bool,
}

pub async fn site_info_json(db: &mut orm::MySqlConn, clargs: &InfoJsonCli) -> anyhow::Result<()> {
    let infos = if clargs.date.is_some() {
        orm::siteinfo::SiteInfo::get_site_info_for_date(db, clargs.date.unwrap(), !clargs.inactive)
            .await?
    } else {
        orm::siteinfo::SiteInfo::get_all_site_info(db).await?
    };

    let json = match clargs.json_type {
        JsonType::Flat => orm::siteinfo::SiteInfo::to_flat_json(&infos, !clargs.minified)?,
        JsonType::Grouped => orm::siteinfo::SiteInfo::to_grouped_json(&infos, !clargs.minified)?,
    };

    if clargs.minified {
        print!("{json}");
    } else {
        println!("{json}");
    }

    Ok(())
}

/// Define a new standard site
#[derive(Debug, Args)]
pub struct AddNewStdSiteCli {
    /// The two character ID for the new site
    site_id: String,
    /// The long, human-readable name for this site
    site_name: String,
    /// Whether this is a TCCON or EM27 site
    site_type: SiteType,
}

pub async fn add_new_std_site_cli(
    conn: &mut MySqlConn,
    args: AddNewStdSiteCli,
) -> anyhow::Result<()> {
    add_new_std_site(conn, &args.site_id, &args.site_name, args.site_type).await
}

pub async fn add_new_std_site(
    conn: &mut MySqlConn,
    site_id: &str,
    site_name: &str,
    site_type: SiteType,
) -> anyhow::Result<()> {
    StdSite::create(conn, site_id, site_name, site_type).await?;
    Ok(())
}

/// Modify an existing standard site
#[derive(Debug, Args)]
pub struct EditSiteCli {
    /// The current two-letter ID for the site
    site_id: String,

    /// A new two-letter ID for the site - must be unique among all sites
    #[clap(long = "site-id")]
    new_site_id: Option<String>,

    /// If given, the new name to assign for this site
    #[clap(long = "name")]
    site_name: Option<String>,

    /// If given, the new type (TCCON or EM27) for this site
    #[clap(long = "type")]
    site_type: Option<SiteType>,

    /// If given, the new output structure ("FlatModVmr", "FlatAll", "TreeModVmr", or "TreeAll")
    /// for this site. The "Flat" structures will put all the files in the root of the tarball,
    /// while the "Tree" structure retain ginputs `fpit/xx/*` directory structure. The "ModVmr"
    /// options only keep the `.mod` and `.vmr` files, while the "All" structures include the
    /// `.map` files as well.
    #[clap(long = "output")]
    output_structure: Option<StdOutputStructure>,
}

pub async fn edit_std_site_cli(conn: &mut MySqlConn, args: EditSiteCli) -> anyhow::Result<()> {
    edit_std_site(
        conn,
        &args.site_id,
        args.new_site_id,
        args.site_name,
        args.site_type,
        args.output_structure,
    )
    .await
}

pub async fn edit_std_site(
    conn: &mut MySqlConn,
    site_id: &str,
    new_site_id: Option<String>,
    site_name: Option<String>,
    site_type: Option<SiteType>,
    output_structure: Option<StdOutputStructure>,
) -> anyhow::Result<()> {
    let mut trans = conn.begin().await?;

    let mut site = if let Some(s) = StdSite::get_by_site_id(&mut trans, site_id).await? {
        s
    } else {
        anyhow::bail!("No site with site ID '{site_id}'");
    };

    if let Some(sid) = new_site_id {
        site.set_site_id(&mut trans, sid.clone()).await?;
        warn!("Site ID has been changed from '{site_id}' to '{sid}', but any standard site tarballs will not be renamed. Please see to that manually.");
    }

    if let Some(name) = site_name {
        site.set_name(&mut trans, name).await?;
    }

    if let Some(typ) = site_type {
        site.set_type(&mut trans, typ).await?;
    }

    if let Some(out_struct) = output_structure {
        site.set_output_structure(&mut trans, out_struct).await?;
    }

    trans.commit().await?;
    Ok(())
}

/// Add a new date range defining the location of a standard site.
///
/// If this is the first date range added for this site, then location,
/// latitude, and longitude must all be given. If you are adding a new date
/// range that overlaps an existing date range, then location, latitude, and/or
/// longitude may be omitted so long as their values are consistent in
/// all of the date ranges overlapped. In that case, any omitted values are copied
/// from the overlapped existing periods.
#[derive(Debug, Args)]
pub struct AddSiteInfoCli {
    /// The two letter ID of the site
    site_id: String,

    /// The first date, in YYYY-MM-DD format, that this location applies.
    start_date: NaiveDate,

    /// The final date (exclusive) in YYYY-MM-DD format, that this location applies.
    /// If not given, this location is assumed to have no end date.
    end_date: Option<NaiveDate>,

    /// A human-readable description of the site's location, e.g. "Park Fall, WI, USA".
    #[clap(short = 'l', long)]
    location: Option<String>,

    /// The longitude of the site. Must be between -180 and +360 and will be rectified to
    /// be within -180 to +180. When giving a negative value, using the = format, i.e.
    /// `--longitude=-90` may work better than `--longitude -90`.
    #[clap(short = 'x', long)]
    longitude: Option<f32>,

    /// The latitude of the site. Must be between -90 and +90. See note on longitude for
    /// entering negative values.
    #[clap(short = 'y', long)]
    latitude: Option<f32>,

    /// An optional comment giving more information about this date range.
    #[clap(short = 'c', long)]
    comment: Option<String>,
}

pub async fn add_std_site_info_range_cli(
    conn: &mut MySqlConn,
    config: &Config,
    args: AddSiteInfoCli,
) -> anyhow::Result<()> {
    add_std_site_info_range(
        conn,
        config,
        &args.site_id,
        args.start_date,
        args.end_date,
        args.location,
        args.longitude,
        args.latitude,
        args.comment.as_deref(),
    )
    .await
}

pub async fn add_std_site_info_range(
    conn: &mut MySqlConn,
    config: &Config,
    site_id: &str,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    location: Option<String>,
    longitude: Option<f32>,
    latitude: Option<f32>,
    comment: Option<&str>,
) -> anyhow::Result<()> {
    SiteInfo::set_site_info_for_dates(
        conn, config, site_id, start_date, end_date, location, longitude, latitude, comment, false,
    )
    .await
}

/// Clear existing location information for a site
///
/// Note that this will delete output files for the cleared
/// dates the next time regen flags are processed.
#[derive(Debug, Args)]
pub struct SetNonopCli {
    /// The two letter ID of the site
    site_id: String,

    /// The first date, in YYYY-MM-DD format, to clear info for.
    start_date: NaiveDate,

    /// The final date (exclusive) in YYYY-MM-DD format, to clear info for.
    /// If not given, this location is assumed to have no end date.
    end_date: Option<NaiveDate>,
}

pub async fn clear_site_info_range_cli(
    conn: &mut MySqlConn,
    config: &Config,
    args: SetNonopCli,
) -> anyhow::Result<()> {
    clear_site_info_range(conn, config, &args.site_id, args.start_date, args.end_date).await
}

pub async fn clear_site_info_range(
    conn: &mut MySqlConn,
    config: &Config,
    site_id: &str,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
) -> anyhow::Result<()> {
    SiteInfo::set_site_info_for_dates(
        conn, config, site_id, start_date, end_date, None, None, None, None, true,
    )
    .await
}

/// Delete a site info entry by its ID
///
/// WARNING: this is meant *only* for cleaning up row entries that should not exist.
/// If you want to indicate that a site should not have priors generated for a given
/// time, use set-nonop instead. This subcommand does not update the site jobs table.
#[derive(Debug, Args)]
pub struct DeleteInfoCli {
    /// The ID for the info entry to delete. This can be obtained from the print-info
    /// subcommand
    row_id: i32,
}

pub async fn delete_info_row_cli(conn: &mut MySqlConn, args: DeleteInfoCli) -> anyhow::Result<()> {
    let info = SiteInfo::get_location_by_id(conn, args.row_id)
        .await
        .with_context(|| {
            format!(
                "Error occurred retrieving info row with id = {}",
                args.row_id
            )
        })?;
    info.delete(conn).await.with_context(|| {
        format!(
            "Error occurred while trying to delete info row with id = {}",
            args.row_id
        )
    })?;
    Ok(())
}

/// Print out a table of defined standard sites
#[derive(Debug, Args)]
pub struct PrintSitesCli {
    /// Limit to only sites of a certain type
    #[clap(short = 't', long = "type")]
    site_type: Option<SiteType>,
}

pub async fn print_sites_cli(conn: &mut MySqlConn, args: PrintSitesCli) -> anyhow::Result<()> {
    print_sites(conn, args.site_type).await
}

pub async fn print_sites(conn: &mut MySqlConn, site_type: Option<SiteType>) -> anyhow::Result<()> {
    let sites = StdSite::get_by_type(conn, site_type).await?;
    let table = orm::utils::to_std_table(sites);
    println!("{table}");
    Ok(())
}

/// Print currently defined location info for a given site
#[derive(Debug, Args)]
pub struct PrintLocsCli {
    /// The two-letter ID for the site to print information about. If omitted,
    /// all sites' information is printed.
    site_id: Option<String>,
}

pub async fn print_locations_for_site_cli(
    conn: &mut MySqlConn,
    args: PrintLocsCli,
) -> anyhow::Result<()> {
    print_locations_for_site(conn, args.site_id.as_deref()).await
}

pub async fn print_locations_for_site(
    conn: &mut MySqlConn,
    site_id: Option<&str>,
) -> anyhow::Result<()> {
    let infos = if let Some(sid) = site_id {
        SiteInfo::get_site_locations(conn, sid).await?
    } else {
        let mut all_info = SiteInfo::get_all_site_info(conn).await?;
        all_info.sort_unstable_by_key(|info| {
            let sid = info.site_id.as_deref().unwrap_or("??").to_string();
            let start_date = info.start_date;
            (sid, start_date)
        });
        all_info
    };
    let table = orm::utils::to_std_table(infos);
    println!("{table}");

    Ok(())
}

/// Add new sites and their initial location/time span from the
/// request form CSV.
#[derive(Debug, clap::Args)]
pub struct AddSitesFromRequestCli {
    #[clap(flatten)]
    csv_src: shared_cli::SiteRequetFormComponent,
}

pub async fn add_sites_from_request_cli(
    conn: &mut MySqlConn,
    config: &Config,
    args: AddSitesFromRequestCli,
) -> anyhow::Result<()> {
    add_sites_from_request(
        conn,
        config,
        args.csv_src.request_csv.as_deref(),
        args.csv_src.sheet_id.as_deref(),
    )
    .await
}

pub(crate) async fn add_sites_from_request(
    conn: &mut MySqlConn,
    config: &Config,
    request_csv_file: Option<&Path>,
    request_sheet_id: Option<&str>,
) -> anyhow::Result<()> {
    let csv_data = get_request_csv_data(config, request_csv_file, request_sheet_id).await?
        .ok_or_else(|| anyhow::anyhow!("Must provide one of the following: --request-csv, --sheet-id, or the std_site_req_sheet_id entry in the [email] section of the config"))?;
    let requests: Vec<RequestRow> = RequestIter::new(&csv_data, true)
        .try_collect()
        .context("Error occurred while reading the request CSV file")?;
    let mut existing_site_ids = StdSite::get_site_id_map_to_name(conn, None)
        .await
        .context("Error getting the existing list of standard sites")?;
    let mut sites_to_add = vec![];
    for row in requests.iter() {
        // Custom errors from the interactive prompts we don't know should cancel the run.
        // The other variants indicate that the user cancelled or there is a problem interacting
        // with the terminal, so those should all immediately return - either because we want
        // to stop or because the issue is probably going to happen again.
        match SiteToAdd::from_request_row(row, &existing_site_ids) {
            Ok(site) => {
                // Make sure we can't duplicate a site ID accidentally
                existing_site_ids.insert(site.site_id.clone(), site.site_name.clone());
                sites_to_add.push(Ok(site))
            }
            Err(InquireError::Custom(e)) => sites_to_add.push(Err(e)),
            Err(e) => return Err(e.into()),
        }
    }

    loop {
        println!("{} sites to add:", sites_to_add.len());
        let mut choices = vec![AddSiteChoice::AddAll, AddSiteChoice::Abort];

        for (isite, site_res) in sites_to_add.iter().enumerate() {
            choices.push(AddSiteChoice::Edit(isite));
            match site_res {
                Ok(site) => println!("== {} ==\n{site}", isite + 1),
                Err(err) => println!("== {} ==\nERROR: {err}", isite + 1),
            }
        }

        let choice = inquire::Select::new("What do you want to do?", choices).prompt()?;
        match choice {
            AddSiteChoice::Edit(index) => match sites_to_add.get_mut(index) {
                Some(Ok(site)) => {
                    // This site's own ID is in the map (we inserted it when the site was
                    // created), so take it out for the duration of the edit; otherwise
                    // keeping the current site ID would be rejected as a duplicate.
                    existing_site_ids.remove(&site.site_id);
                    let edit_result = site.edit_interactive(&existing_site_ids);
                    // Re-register the site under its (possibly new) ID and name whether or
                    // not the edit succeeded - a failed edit leaves the site unchanged, so
                    // this restores the original entry in that case.
                    existing_site_ids.insert(site.site_id.clone(), site.site_name.clone());
                    match edit_result {
                        Ok(()) => (),
                        Err(InquireError::Custom(e)) => {
                            println!("Site {} was not edited: {e}", index + 1)
                        }
                        Err(InquireError::OperationCanceled) => {
                            println!("Cancelled edits, site {} was not edited", index + 1)
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Some(Err(err)) => println!(
                    "Site {} could not be read from the request form ({err}), so there is nothing to edit.",
                    index + 1
                ),
                None => println!("There is no site {} to edit.", index + 1),
            },
            AddSiteChoice::AddAll => {
                let n_bad = sites_to_add
                    .iter()
                    .fold(0, |n, res| if res.is_err() { n + 1 } else { n });
                let valid_sites = sites_to_add
                    .iter()
                    .filter_map(|res| res.as_ref().ok())
                    .collect_vec();
                let msg = if n_bad > 0 {
                    format!(
                        "Are you sure you want to add {} sites? ({n_bad} skipped due to errors.)",
                        valid_sites.len()
                    )
                } else {
                    format!("Are you sure you want to add {} sites?", valid_sites.len())
                };
                let confirmed = inquire::Confirm::new(&msg).prompt()?;
                if confirmed {
                    add_request_sites(conn, config, &valid_sites)
                        .await
                        .context(
                        "Error occurred while adding sites; transaction aborted - no sites added",
                    )?;
                    break;
                }
            }
            AddSiteChoice::Abort => break,
        }
    }
    Ok(())
}

enum AddSiteChoice {
    Edit(usize),
    AddAll,
    Abort,
}

impl Display for AddSiteChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AddSiteChoice::Edit(index) => write!(f, "Edit {}", index + 1),
            AddSiteChoice::AddAll => write!(f, "Add all"),
            AddSiteChoice::Abort => write!(f, "Abort"),
        }
    }
}

async fn add_request_sites(
    conn: &mut MySqlConn,
    config: &Config,
    sites: &[&SiteToAdd],
) -> anyhow::Result<()> {
    let mut transaction = conn.begin().await?;
    for site in sites {
        add_new_std_site(
            &mut transaction,
            &site.site_id,
            &site.site_name,
            site.site_type,
        )
        .await
        .with_context(|| format!("An error occurred adding the site '{}'", site.site_id))?;

        add_std_site_info_range(
            &mut transaction,
            config,
            &site.site_id,
            site.start_date,
            site.end_date,
            Some(site.location.clone()),
            Some(site.longitude),
            Some(site.latitude),
            site.comment.as_deref(),
        )
        .await
        .with_context(|| {
            format!(
                "An error occurred adding the first info range for site '{}'",
                site.site_id
            )
        })?;
    }
    transaction.commit().await?;
    Ok(())
}

struct SiteToAdd {
    site_id: String,
    site_name: String,
    site_type: SiteType,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    location: String,
    longitude: f32,
    latitude: f32,
    comment: Option<String>,
}

impl SiteToAdd {
    fn from_request_row(
        row: &RequestRow,
        existing_site_ids: &HashMap<String, String>,
    ) -> Result<Self, inquire::InquireError> {
        let req_site_id = if !row.desired_site_id.is_empty() {
            &row.desired_site_id
        } else if let Some(custom_ids) = &row.custom_loc_sids {
            custom_ids
        } else {
            "?"
        };
        println!(
            "Request from {} for site {req_site_id} for lat = {}, lon = {}",
            row.contact_email, row.desired_latitude, row.desired_longitude
        );

        let longitude = Self::parse_latlon(&row.desired_longitude, "longitude", 180.0)?;
        let latitude = Self::parse_latlon(&row.desired_latitude, "latitude", 90.0)?;
        let site_id = Self::get_site_id(&row.desired_site_id, existing_site_ids)?;
        let site_name =
            inquire::Text::new("Enter the name for the site, e.g., 'Park Falls' (without quotes)")
                .with_validator(Self::validate_no_quotes)
                .prompt()?;
        // Assume that EM27s are the most commonly requested site type
        let i_init = SiteType::VARIANTS
            .iter()
            .position(|v| v == &SiteType::EM27)
            .unwrap_or(0);
        let site_type = inquire::Select::new("Select the site type", SiteType::VARIANTS.to_vec())
            .with_starting_cursor(i_init)
            .prompt()?;
        let location = inquire::Text::new(
            "Enter the location for the site, e.g. 'Wisconsin, USA' (without quotes)",
        )
        .with_validator(Self::validate_no_quotes)
        .prompt()?;

        // Prefer generation to start/end on month boundaries
        let start_date = row
            .obs_start_date
            .with_day(1)
            .expect("day = 1 should be valid");
        // Flattening will squash errors for out of range dates, but that
        // makes sense to turn those into open ended ranges.
        let end_date = row
            .obs_end_date
            .map(|d| {
                d.with_day(1)
                    .expect("day = 1 should be valid")
                    .checked_add_months(chrono::Months::new(1))
            })
            .flatten();

        let comment = inquire::Text::new("Enter an optional comment (empty for none)").prompt()?;
        let comment = if comment.is_empty() {
            None
        } else {
            Some(comment)
        };

        Ok(Self {
            site_id,
            site_name,
            site_type,
            start_date,
            end_date,
            location,
            latitude,
            longitude,
            comment,
        })
    }

    /// Interactively edit an existing site in place.
    ///
    /// This prompts for the same values as [`Self::from_request_row`] (plus the date
    /// range, which that function takes from the request row), with the site's current
    /// value pre-filled as the initial value of each prompt, so pressing enter keeps
    /// the current value. `self` is only modified once every prompt has completed, so
    /// an aborted or failed edit leaves it untouched.
    ///
    /// `existing_site_ids` must *not* contain this site's own site ID, otherwise
    /// keeping the current site ID will be rejected as a duplicate.
    fn edit_interactive(
        &mut self,
        existing_site_ids: &HashMap<String, String>,
    ) -> Result<(), inquire::InquireError> {
        println!(
            "Editing {} ({}) - press enter to keep the current value of a field.",
            self.site_name, self.site_id
        );

        let site_id = inquire::Text::new("Enter site ID (2 characters)")
            .with_initial_value(&self.site_id)
            .with_validator(|inp: &str| Self::validate_site_id(inp, existing_site_ids))
            .prompt()?;
        let site_name =
            inquire::Text::new("Enter the name for the site, e.g., 'Park Falls' (without quotes)")
                .with_initial_value(&self.site_name)
                .with_validator(Self::validate_no_quotes)
                .prompt()?;
        // Start the cursor on this site's current type rather than the default EM27
        let i_init = SiteType::VARIANTS
            .iter()
            .position(|v| v == &self.site_type)
            .unwrap_or(0);
        let site_type = inquire::Select::new("Select the site type", SiteType::VARIANTS.to_vec())
            .with_starting_cursor(i_init)
            .prompt()?;
        let location = inquire::Text::new(
            "Enter the location for the site, e.g. 'Wisconsin, USA' (without quotes)",
        )
        .with_initial_value(&self.location)
        .with_validator(Self::validate_no_quotes)
        .prompt()?;
        let latitude = Self::edit_latlon(self.latitude, "latitude", 90.0)?;
        let longitude = Self::edit_latlon(self.longitude, "longitude", 180.0)?;

        let start_date = Self::edit_date(
            "Enter the first date this location applies (YYYY-MM-DD)",
            Some(self.start_date),
            false,
            None,
        )?
        .expect("a start date should be required by the validator");
        let end_date = Self::edit_date(
            "Enter the last date (exclusive) this location applies (YYYY-MM-DD, empty for open ended)",
            self.end_date,
            true,
            Some(start_date),
        )?;

        let comment = inquire::Text::new("Enter an optional comment (empty for none)")
            .with_initial_value(self.comment.as_deref().unwrap_or(""))
            .prompt()?;
        let comment = if comment.is_empty() {
            None
        } else {
            Some(comment)
        };

        // Only commit the new values now that every prompt has succeeded, so that
        // cancelling partway through doesn't leave a half-edited site behind.
        self.site_id = site_id;
        self.site_name = site_name;
        self.site_type = site_type;
        self.start_date = start_date;
        self.end_date = end_date;
        self.location = location;
        self.latitude = latitude;
        self.longitude = longitude;
        self.comment = comment;

        Ok(())
    }

    fn parse_latlon(
        input: &str,
        field: &str,
        max_value: f32,
    ) -> Result<f32, inquire::InquireError> {
        if let Ok(value) = input.parse() {
            return Ok(value);
        }

        let new_input = inquire::Text::new(&format!("Input correct {field} value"))
            .with_validator(|input: &str| Self::validate_latlon(input, max_value))
            .prompt()?;

        let value: f32 = new_input
            .parse()
            .expect("input should have been validated to be parseable");
        Ok(value)
    }

    /// Like [`Self::parse_latlon`], but always prompts, pre-filled with `current`.
    fn edit_latlon(
        current: f32,
        field: &str,
        max_value: f32,
    ) -> Result<f32, inquire::InquireError> {
        let initial = current.to_string();
        let new_input = inquire::Text::new(&format!("Enter the {field}"))
            .with_initial_value(&initial)
            .with_validator(move |input: &str| Self::validate_latlon(input, max_value))
            .prompt()?;

        let value: f32 = new_input
            .parse()
            .expect("input should have been validated to be parseable");
        Ok(value)
    }

    /// Prompt for a date in YYYY-MM-DD format, pre-filled with `current`.
    ///
    /// If `optional` is true, an empty input is accepted and returns `None`.
    /// If `after` is given, the date entered must be later than that date.
    fn edit_date(
        message: &str,
        current: Option<NaiveDate>,
        optional: bool,
        after: Option<NaiveDate>,
    ) -> Result<Option<NaiveDate>, inquire::InquireError> {
        let initial = current.map(|d| d.to_string()).unwrap_or_default();
        let new_input = inquire::Text::new(message)
            .with_initial_value(&initial)
            .with_validator(move |input: &str| Self::validate_date(input, optional, after))
            .prompt()?;

        let new_input = new_input.trim();
        if new_input.is_empty() {
            return Ok(None);
        }

        let date = NaiveDate::parse_from_str(new_input, "%Y-%m-%d")
            .expect("input should have been validated as a date");
        Ok(Some(date))
    }

    fn validate_date(
        input: &str,
        optional: bool,
        after: Option<NaiveDate>,
    ) -> Result<Validation, inquire::CustomUserError> {
        let input = input.trim();
        if input.is_empty() {
            if optional {
                return Ok(Validation::Valid);
            } else {
                return Ok(Validation::Invalid("A date is required".into()));
            }
        }

        let date = if let Ok(d) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
            d
        } else {
            return Ok(Validation::Invalid(
                "Date must be in YYYY-MM-DD format".into(),
            ));
        };

        if let Some(min_date) = after {
            if date <= min_date {
                return Ok(Validation::Invalid(
                    format!("Date must be after {min_date}").into(),
                ));
            }
        }

        Ok(Validation::Valid)
    }

    fn validate_latlon(
        input: &str,
        max_value: f32,
    ) -> Result<Validation, inquire::CustomUserError> {
        let val = if let Ok(x) = input.parse::<f32>() {
            x
        } else {
            return Ok(Validation::Invalid(
                "Input must be parseable as a float".into(),
            ));
        };

        if val.abs() > max_value {
            return Ok(Validation::Invalid(
                "Input must be between -{max_value} and +{max_value}".into(),
            ));
        }

        Ok(Validation::Valid)
    }

    fn get_site_id(
        request_value: &str,
        existing_site_ids: &HashMap<String, String>,
    ) -> Result<String, inquire::InquireError> {
        let existing_site_name = existing_site_ids.get(request_value);
        if request_value.len() == 2 && existing_site_name.is_none() {
            Ok(request_value.to_string())
        } else {
            let site_id = inquire::Text::new("Enter site ID (2 characters)")
                .with_validator(|inp: &str| Self::validate_site_id(inp, existing_site_ids))
                .prompt()?;
            Ok(site_id)
        }
    }

    fn validate_site_id(
        input: &str,
        existing_site_ids: &HashMap<String, String>,
    ) -> Result<Validation, inquire::CustomUserError> {
        if input.len() != 2 {
            return Ok(Validation::Invalid("Site ID must be 2 characters".into()));
        } else if let Some(name) = existing_site_ids.get(input) {
            return Ok(Validation::Invalid(
                format!("'{input}' is already used by site '{name}'").into(),
            ));
        } else {
            return Ok(Validation::Valid);
        }
    }

    fn validate_no_quotes(input: &str) -> Result<Validation, inquire::CustomUserError> {
        if input.starts_with(&['\'', '"']) || input.ends_with(&['\'', '"']) {
            Ok(Validation::Invalid(
                "Do not enclose the value in quotes".into(),
            ))
        } else {
            Ok(Validation::Valid)
        }
    }
}

impl Display for SiteToAdd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "  {} ({}), {} site",
            self.site_name, self.site_id, self.site_type
        )?;
        writeln!(
            f,
            "  lat = {:.4}, lon = {:.4} ({})",
            self.latitude, self.longitude, self.location
        )?;
        // Do not end with a newline, so whichever line might be last
        // uses write! instead of writeln!
        if let Some(end) = self.end_date {
            write!(f, "  From {} to {}", self.start_date, end)?;
        } else {
            write!(f, "  Starts on {} (open-ended)", self.start_date)?;
        }
        if let Some(cmt) = self.comment.as_deref() {
            write!(f, "\n  Comment: {cmt}")?;
        }
        Ok(())
    }
}
