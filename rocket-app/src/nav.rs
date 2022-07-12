use rocket::http::uri::Origin;
use serde::Serialize;
use tera::Function;

use crate::jobs::*;
use crate::stdsites::*;

/// A structure that holds the URLs needed for the navigation bar
/// 
/// If additional links are desired for the navigation bar, add them as
/// additional fields to this class, use the [`rocket::uri!`] macro to
/// look them up in the `new` method, then add a name for them to the 
/// [`tera::Function`] implementation's `call` method. 
/// 
/// Because this struct implements [`tera::Function`] it can be registered
/// in Rocket's `attach` method with:
/// 
/// ```
/// Template::custom(|engines| {
/// engines.tera.register_function("nav_url_for", nav::NavBarUrls::new());
/// }
/// ```
/// 
/// so that it can be called in templates as `{{ nav_url_for(name='submit_job') }}`,
/// for example. This allows these URLs to be available automatically to all pages
/// without having to include them in individual context objects.
#[derive(Debug, Serialize)]
pub struct NavBarUrls<'a> {
    pub submit_job: Origin<'a>,
    pub check_jobs: Origin<'a>,
    pub check_std_stds: Origin<'a>
}

impl<'a> NavBarUrls<'a> {
    pub fn new() -> Self {
        return Self { 
            submit_job: rocket::uri!(submit_job),
            check_jobs: rocket::uri!(check_jobs), 
            check_std_stds: rocket::uri!(check_std_sites)
        }
    }
}

impl<'a> Function for NavBarUrls<'a> {
    fn call(&self, args: &std::collections::HashMap<String, tera::Value>) -> tera::Result<tera::Value> {
        let url_name = match args.get("name") {
            Some(val) => match tera::from_value::<String>(val.clone()) {
                Ok(name) => name,
                Err(_) => return Err("Unable to convert the 'name' parameter into a string".into())
            },
            None => return Err("Must provide a 'name' paramter, e.g. name='index'".into())
        };

        let url = match url_name.as_ref() {
            "submit_job" => &self.submit_job,
            "check_jobs" => &self.check_jobs,
            "check_std_sites" => &self.check_std_stds,
            _ => return Err(format!("The url name {url_name} is not known").into())
        };

        Ok(tera::to_value(url)?)
    }
}

#[derive(Debug)]
pub struct StaticUrls {
    static_root: String
}

impl StaticUrls {
    pub fn new(static_root: &str) -> Self {
        let root = static_root.trim_end_matches(&[' ', '/']);
        return Self { static_root: root.to_owned() }
    }
}

impl Function for StaticUrls {
    fn call(&self, args: &std::collections::HashMap<String, tera::Value>) -> tera::Result<tera::Value> {
        let file_path = match args.get("file") {
            Some(val) => match tera::from_value::<String>(val.clone()) {
                Ok(p) => p,
                Err(_) => return Err("Unable to convert the 'file' parameter into a string".into())
            },
            None => return Err("Must provide a 'file' parameter, e.g. file='main.css'".into())
        };

        let url = format!("{}/{}", self.static_root, file_path);
        Ok(tera::to_value(url)?)
    }
}