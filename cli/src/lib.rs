pub mod api;
pub mod auth;
pub mod config;
pub mod dbexport;
pub mod email;
pub mod input_files;
pub mod jobs;
pub mod met_download;
pub mod siteinfo;
pub mod stdsites;

pub fn get_user_input(prompt: &str) -> std::io::Result<String> {
    use std::io::Write;

    print!("{prompt}");
    std::io::stdout().flush()?;
    let mut ans = String::new();
    std::io::stdin().read_line(&mut ans)?;
    Ok(ans.trim().to_owned())
}
