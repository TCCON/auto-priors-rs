use std::path::PathBuf;

#[derive(Debug, clap::Args)]
pub struct SiteRequetFormComponent {
    /// Path to the .csv of the priors requests, downloaded from Google sheets. If omitted,
    /// will be fetched based on the sheet ID configured or passed to the --sheet-id option.
    #[clap(short = 'r', long)]
    pub request_csv: Option<PathBuf>,

    /// The google sheet ID (normally the part after "/d/" in the sharing URL) to fetch.
    #[clap(long)]
    pub sheet_id: Option<String>,
}
