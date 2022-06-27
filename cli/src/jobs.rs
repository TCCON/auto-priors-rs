use std::path::PathBuf;

use chrono::{NaiveDate, NaiveDateTime};
use clap::{self, Args};
use orm::jobs::{Job, ModFmt, VmrFmt, MapFmt, TarChoice};

fn str_to_vec_str(arg: &str) -> Result<Vec<String>, anyhow::Error> {
    Ok(arg.split(",").map(|x| x.to_owned()).collect())
}

fn str_to_vec_float(arg: &str) -> Result<Vec<Option<f32>>, anyhow::Error> {
    let mut floats = vec![];
    for el in arg.split(",") {
        let v = el.parse::<f32>()?;
        floats.push(Some(v));
    }

    return Ok(floats);
}

#[derive(Debug, Args)]
pub struct AddJobCli {
    
    #[clap(long="mod-fmt")]
    mod_fmt: Option<ModFmt>,
    #[clap(long="vmr-fmt")]
    vmr_fmt: Option<VmrFmt>,
    #[clap(long="map-fmt")]
    map_fmt: Option<MapFmt>,
    #[clap(short='p', long="priority")]
    priority: Option<i32>,
    #[clap(long="no-delete")]
    no_delete: bool,
    #[clap(short='t', long="to-tarball")]
    to_tarball: bool,

    site_id: String,
    start_date: NaiveDate,
    end_date: NaiveDate,
    email: String,
    lat: Option<String>,
    lon: Option<String>
}

pub struct AddJobArgs {
    site_id: Vec<String>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    email: String,
    lat: Vec<Option<f32>>,
    lon: Vec<Option<f32>>,
    mod_fmt: Option<ModFmt>,
    vmr_fmt: Option<VmrFmt>,
    map_fmt: Option<MapFmt>,
    priority: Option<i32>,
    delete_time: Option<NaiveDateTime>,
    save_dir: PathBuf,
    save_tarball: TarChoice
}

#[derive(Debug, Args)]
pub struct DeleteJobCli {
    id: i32,
}

impl TryFrom<AddJobCli> for AddJobArgs {
    type Error = anyhow::Error;

    fn try_from(clargs: AddJobCli) -> Result<Self, anyhow::Error> {
        let site_ids = str_to_vec_str(&clargs.site_id)?;
        let (lat, lon) = if clargs.lat.is_some() && clargs.lon.is_some() {
            let x = str_to_vec_float(&clargs.lon.unwrap())?;
            let y = str_to_vec_float(&clargs.lat.unwrap())?;
            (y, x)
        }else if clargs.lat.is_none() && clargs.lon.is_none() {
            let n = site_ids.len();
            let x = vec![None; n];
            let y = vec![None; n];
            (y, x)
        }else{
            anyhow::bail!("lat and lon must be both given or neither given");
        };

        let delete_time = if clargs.no_delete {
            None
        }else{
            // TODO: use configuration for default time to keep
            let now = chrono::Local::now().naive_local();
            Some(now + chrono::Duration::days(7))
        };

        let save_tarball = if clargs.to_tarball {
            TarChoice::Yes
        }else{
            TarChoice::No
        };

        // TODO: take from config
        let save_dir = PathBuf::from(".");
        
        Ok(Self { 
            site_id: site_ids,
            start_date: clargs.start_date,
            end_date: clargs.end_date,
            email: clargs.email,
            lat: lat,
            lon: lon,
            mod_fmt: clargs.mod_fmt,
            vmr_fmt: clargs.vmr_fmt,
            map_fmt: clargs.map_fmt,
            priority: clargs.priority,
            delete_time: delete_time,
            save_dir: save_dir,
            save_tarball: save_tarball
        })
    }
}

pub async fn add_job(db: &mut orm::MySqlPC, clargs: AddJobCli) -> anyhow::Result<()> {
    dbg!(&clargs);
    let args = AddJobArgs::try_from(clargs)?;
    let id = Job::add_job_from_args(db, 
        args.site_id,
        args.start_date, 
        args.end_date, 
        args.save_dir, 
        Some(args.email), 
        args.lat,
        args.lon,
        args.mod_fmt, 
        args.vmr_fmt, 
        args.map_fmt, 
        args.priority, 
        args.delete_time, 
        args.save_tarball)
    .await?;
    println!("Added new job, ID = {id}");
    Ok(())
}

pub async fn delete_job(db: &mut orm::MySqlPool, clargs: DeleteJobCli) -> anyhow::Result<()> {
    let n_deleted = orm::jobs::Job::delete_job_with_id(db, clargs.id).await?;
    println!("Deleted {n_deleted} job(s)");
    Ok(())
}