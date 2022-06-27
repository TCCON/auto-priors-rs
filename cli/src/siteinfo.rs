use std::str::FromStr;
use anyhow;
use chrono::NaiveDate;
use clap::{self,Args};
use orm;


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


pub async fn site_info_json(db: &mut orm::MySqlPC, clargs: &InfoJsonCli) -> anyhow::Result<()> {
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