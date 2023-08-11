pub mod utils;
pub mod config;
pub mod met_download;
pub mod input_files;
pub mod jobs;
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