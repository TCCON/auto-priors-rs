use std::str::FromStr;
use anyhow;
use chrono::NaiveDate;
use clap::{self,Args, Subcommand};
use orm::{self, siteinfo::{SiteType, StdSite, SiteInfo}, MySqlConn};
use sqlx::Connection;


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

#[derive(Debug, Args)]
pub struct StdSiteCli {
    #[clap(subcommand)]
    pub command: Actions
}

#[derive(Debug, Subcommand)]
pub enum Actions {
    AddSite(AddNewStdSiteCli),
    EditSite(EditSiteCli),
    AddInfo(AddSiteInfoCli)
}

#[derive(Debug, Args)]
pub struct AddNewStdSiteCli {
    site_id: String,
    site_name: String,
    site_type: SiteType
}

pub async fn add_new_std_site_cli(conn: &mut MySqlConn, args: AddNewStdSiteCli) -> anyhow::Result<()> {
    add_new_std_site(conn, &args.site_id, &args.site_name, args.site_type).await
}


pub async fn add_new_std_site(conn: &mut MySqlConn, site_id: &str, site_name: &str, site_type: SiteType) -> anyhow::Result<()> {
    StdSite::create(conn, site_id, site_name, site_type).await?;
    Ok(())
}


#[derive(Debug, Args)]
pub struct EditSiteCli {
    site_id: String,
    #[clap(long="name")]
    site_name: Option<String>,
    #[clap(long="type")]
    site_type: Option<SiteType>
}

pub async fn edit_std_site_cli(conn: &mut MySqlConn, args: EditSiteCli) -> anyhow::Result<()> {
    edit_std_site(conn, &args.site_id, args.site_name, args.site_type).await
}

pub async fn edit_std_site(conn: &mut MySqlConn, site_id: &str, site_name: Option<String>, site_type: Option<SiteType>) -> anyhow::Result<()> {
    let mut trans = conn.begin().await?;

    let mut site = if let Some(s) = StdSite::get_by_site_id(&mut trans, site_id).await? {
        s
    } else {
        anyhow::bail!("No site with site ID '{site_id}'");
    };

    if let Some(name) = site_name {
        site.set_name(&mut trans, name).await?;
    }

    if let Some(typ) = site_type {
        site.set_type(&mut trans, typ).await?;
    }

    trans.commit().await?;
    Ok(())
}


#[derive(Debug, Args)]
pub struct AddSiteInfoCli {
    site_id: String,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    #[clap(short = 'l', long)]
    location: Option<String>,
    #[clap(short = 'x', long)]
    longitude: Option<f32>,
    #[clap(short = 'y', long)]
    latitude: Option<f32>,
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