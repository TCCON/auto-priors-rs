use std::io::Read;

use clap::{Args, Subcommand};
use orm::auth::api::{authenticate_refresh_token, generate_refresh_token};
use orm::auth::{load_jwt_hmac_secret, User};
use orm::config::Config;
use orm::MySqlConn;

/// Manage aspects of the web API
#[derive(Debug, Args)]
pub struct ApiCli {
    #[clap(subcommand)]
    pub command: ApiActions,
}

#[derive(Debug, Subcommand)]
pub enum ApiActions {
    CreateToken(CreateTokenCli),
    ValidateToken(ValidateTokenCli),
}

/// Create an API token for a user
#[derive(Debug, Args)]
pub struct CreateTokenCli {
    /// The username to create the token for
    username: String,

    /// The nickname to give the token. If omitted, a generic description will
    /// be created.
    #[clap(short = 'n', long)]
    nickname: Option<String>,

    /// Set the token to expire in this many hours from now. If not given, the
    /// default (usually 365 days) will be used.
    #[clap(short = 'e', long = "expire-after")]
    expire_after_hours: Option<i64>,
}

pub async fn generate_api_key_cli(
    conn: &mut MySqlConn,
    config: &Config,
    cli: CreateTokenCli,
) -> anyhow::Result<()> {
    let expire_after = cli.expire_after_hours.map(|h| chrono::TimeDelta::hours(h));
    let token = generate_api_key(conn, config, &cli.username, cli.nickname, expire_after).await?;
    println!("{token}");
    Ok(())
}

pub async fn generate_api_key(
    conn: &mut MySqlConn,
    config: &Config,
    username: &str,
    nickname: Option<String>,
    expire_after: Option<chrono::Duration>,
) -> anyhow::Result<String> {
    let (encode_key, _) = load_jwt_hmac_secret(&config.auth.hmac_secret_file)?;
    let user = User::load_from_db(conn, username)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No user named '{username}' found"))?;
    let nickname: String = nickname.unwrap_or_else(|| {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S %Z");
        format!("Key created from the CLI at {now}")
    });
    let token = generate_refresh_token(conn, user, &nickname, &encode_key, expire_after).await?;
    Ok(token)
}

/// Check that a JWT given is valid
#[derive(Debug, Args)]
pub struct ValidateTokenCli {
    /// The full JSON web token to validate, or a path to a file to read it from if --file is given
    token: String,

    /// If this flag is given, then TOKEN will be intepreted as a path to read from, rather than the
    /// token itself
    #[clap(short = 'f', long)]
    file: bool,
}

pub async fn validate_api_key_cli(
    conn: &mut MySqlConn,
    config: &Config,
    cli: ValidateTokenCli,
) -> anyhow::Result<()> {
    if cli.file {
        let mut f = std::fs::File::open(&cli.token)?;
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        // .trim() is needed - guess there must be whitespace at the end of some files
        validate_api_key(conn, config, s.trim().to_string()).await
    } else {
        validate_api_key(conn, config, cli.token).await
    }
}

pub async fn validate_api_key(
    conn: &mut MySqlConn,
    config: &Config,
    token: String,
) -> anyhow::Result<()> {
    let (_, decode_key) = load_jwt_hmac_secret(&config.auth.hmac_secret_file)?;
    authenticate_refresh_token(conn, &token, &decode_key).await?;
    println!("Token is valid.");
    Ok(())
}
