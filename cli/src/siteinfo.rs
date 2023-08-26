use std::str::FromStr;
use anyhow;
use chrono::NaiveDate;
use clap::{self,Args, Subcommand};
use log::warn;
use orm::{self, siteinfo::{SiteType, StdSite, SiteInfo, StdOutputStructure}, MySqlConn};
use sqlx::Connection;

/// Manage definition of standard sites and their locations
#[derive(Debug, Args)]
pub struct StdSiteCli {
    #[clap(subcommand)]
    pub command: StdSiteActions
}

#[derive(Debug, Subcommand)]
pub enum StdSiteActions {
    AddSite(AddNewStdSiteCli),
    Edit(EditSiteCli),
    Print(PrintSitesCli),
    AddInfo(AddSiteInfoCli),
    PrintInfo(PrintLocsCli),
    Json(InfoJsonCli),
}

#[derive(Debug)]
enum JsonType {
    Flat,
    Grouped,
}

impl FromStr for JsonType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "flat" => Ok(Self::Flat),
            "grouped" => Ok(Self::Grouped),
            _ => Err(format!("Unknown variant of JsonType: {s}"))
        }
    }
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
    inactive: bool
}


pub async fn site_info_json(db: &mut orm::MySqlConn, clargs: &InfoJsonCli) -> anyhow::Result<()> {
    let infos = if clargs.date.is_some() {
        orm::siteinfo::SiteInfo::get_site_info_for_date(db, clargs.date.unwrap(), !clargs.inactive).await?
    }else {
        orm::siteinfo::SiteInfo::get_all_site_info(db).await?
    };

    let json = match clargs.json_type {
        JsonType::Flat => orm::siteinfo::SiteInfo::to_flat_json(&infos, !clargs.minified)?,
        JsonType::Grouped => orm::siteinfo::SiteInfo::to_grouped_json(&infos, !clargs.minified)?
    };
    
    if clargs.minified {
        print!("{json}");
    }else{
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
    site_type: SiteType
}

pub async fn add_new_std_site_cli(conn: &mut MySqlConn, args: AddNewStdSiteCli) -> anyhow::Result<()> {
    add_new_std_site(conn, &args.site_id, &args.site_name, args.site_type).await
}


pub async fn add_new_std_site(conn: &mut MySqlConn, site_id: &str, site_name: &str, site_type: SiteType) -> anyhow::Result<()> {
    StdSite::create(conn, site_id, site_name, site_type).await?;
    Ok(())
}

/// Modify an existing standard site
#[derive(Debug, Args)]
pub struct EditSiteCli {
    /// The current two-letter ID for the site
    site_id: String,

    /// A new two-letter ID for the site - must be unique among all sites
    #[clap(long="site-id")]
    new_site_id: Option<String>,

    /// If given, the new name to assign for this site
    #[clap(long="name")]
    site_name: Option<String>,

    /// If given, the new type (TCCON or EM27) for this site
    #[clap(long="type")]
    site_type: Option<SiteType>,

    /// If given, the new output structure ("FlatModVmr", "FlatAll", "TreeModVmr", or "TreeAll")
    /// for this site. The "Flat" structures will put all the files in the root of the tarball,
    /// while the "Tree" structure retain ginputs `fpit/xx/*` directory structure. The "ModVmr"
    /// options only keep the `.mod` and `.vmr` files, while the "All" structures include the 
    /// `.map` files as well.
    #[clap(long="output")]
    output_structure: Option<StdOutputStructure>
}

pub async fn edit_std_site_cli(conn: &mut MySqlConn, args: EditSiteCli) -> anyhow::Result<()> {
    edit_std_site(conn, &args.site_id, args.new_site_id, args.site_name, args.site_type, args.output_structure).await
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

pub async fn add_std_site_info_range_cli(conn: &mut MySqlConn, args: AddSiteInfoCli) -> anyhow::Result<()> {
    add_std_site_info_range(
        conn,
        &args.site_id,
        args.start_date,
        args.end_date,
        args.location,
        args.longitude,
        args.latitude,
        args.comment.as_deref()
    ).await
}

pub async fn add_std_site_info_range(
    conn: &mut MySqlConn, 
    site_id: &str, 
    start_date: NaiveDate, 
    end_date: Option<NaiveDate>, 
    location: Option<String>, 
    longitude: Option<f32>, 
    latitude: Option<f32>, 
    comment: Option<&str>
) -> anyhow::Result<()> {
    SiteInfo::set_site_info_for_dates(
        conn, 
        site_id, 
        start_date, 
        end_date, 
        location, 
        longitude, 
        latitude, 
        comment
    ).await
}


/// Print out a table of defined standard sites
#[derive(Debug, Args)]
pub struct PrintSitesCli {
    /// Limit to only sites of a certain type
    #[clap(short = 't', long = "type")]
    site_type: Option<SiteType>
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
    /// The two-letter ID for the site to print information about
    site_id: String
}

pub async fn print_locations_for_site_cli(conn: &mut MySqlConn, args: PrintLocsCli) -> anyhow::Result<()> {
    print_locations_for_site(conn, &args.site_id).await
}

pub async fn print_locations_for_site(
    conn: &mut MySqlConn,
    site_id: &str
) -> anyhow::Result<()> {
    let infos = SiteInfo::get_site_locations(conn, site_id).await?;
    let table = orm::utils::to_std_table(infos);
    println!("{table}");

    Ok(())
}