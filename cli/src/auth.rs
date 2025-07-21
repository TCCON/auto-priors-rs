use clap::{Args, Subcommand};
use orm::{
    auth::{api::add_user_permission, PermSet, Permission, User},
    MySqlConn,
};

/// Manage authentication and authorization (i.e., permissions)
#[derive(Debug, Args)]
pub struct AuthCli {
    #[clap(subcommand)]
    pub command: AuthActions,
}

#[derive(Debug, Subcommand)]
pub enum AuthActions {
    MakeAdmin(MakeUserAdminCli),
    ShowPerms(ShowUserPermsCli),
}

/// Give a user the ADMIN permissions
#[derive(Debug, Args)]
pub struct MakeUserAdminCli {
    /// The username to give ADMIN permissions to
    username: String,
}

pub async fn make_user_admin_cli(
    conn: &mut MySqlConn,
    cli: MakeUserAdminCli,
) -> anyhow::Result<()> {
    make_user_admin(conn, &cli.username).await?;
    println!(
        "User '{}' successfully given {} permissions",
        cli.username,
        Permission::Admin
    );
    Ok(())
}

pub async fn make_user_admin(conn: &mut MySqlConn, username: &str) -> anyhow::Result<()> {
    let user = User::load_from_db(conn, username)
        .await?
        .ok_or_else(|| anyhow::anyhow!("User {username} not found"))?;
    add_user_permission(conn, &user, &Permission::Admin).await?;
    Ok(())
}

/// Show permissions assigned to users
#[derive(Debug, Args)]
pub struct ShowUserPermsCli {
    /// The username(s) to show permissions for
    usernames: Vec<String>,
}

pub async fn show_user_perms_cli(
    conn: &mut MySqlConn,
    cli: ShowUserPermsCli,
) -> anyhow::Result<()> {
    show_user_perms(conn, &cli.usernames).await
}

pub async fn show_user_perms<S: AsRef<str>>(
    conn: &mut MySqlConn,
    usernames: &[S],
) -> anyhow::Result<()> {
    for name in usernames {
        let opt_user = User::load_from_db(conn, name.as_ref()).await?;
        if let Some(user) = opt_user {
            let perms = PermSet::load_from_db(conn, &user).await?;
            println!("{}: {perms}", user.username);
        } else {
            println!("User {} not found", name.as_ref());
        }
    }
    Ok(())
}
