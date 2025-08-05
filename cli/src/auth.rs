use std::str::FromStr;

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
    EditPerms(ModifyUserPermissionsCli),
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

/// Add or remove web permissions for a user
#[derive(Debug, Args)]
pub struct ModifyUserPermissionsCli {
    /// The user to modify
    username: String,
    /// Which permission to add or remove: ADMIN, QUERY, SUBMIT, or DOWNLOAD
    permission_tag: String,
    /// Use this flag to delete the permission instead of add it
    #[clap(short, long)]
    delete: bool,
}

pub async fn modify_user_permissions_cli(
    conn: &mut MySqlConn,
    cli: ModifyUserPermissionsCli,
) -> anyhow::Result<()> {
    let perm = Permission::from_str(&cli.permission_tag)
        .map_err(|_| anyhow::anyhow!("Unknown permission tag '{}'", cli.permission_tag))?;
    modify_user_permissions(conn, &cli.username, &perm, cli.delete).await
}

pub async fn modify_user_permissions(
    conn: &mut MySqlConn,
    username: &str,
    perm: &Permission,
    delete: bool,
) -> anyhow::Result<()> {
    let user = User::load_from_db(conn, username)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No user named '{username}' found"))?;
    if delete {
        let had_perm = orm::auth::api::delete_user_permission(conn, &user, perm).await?;
        if had_perm {
            log::info!("Removed {perm} from user {username}");
        } else {
            log::info!("User {username} did not have permission {perm}, no change to database");
        }
    } else {
        let added_perm = orm::auth::api::add_user_permission(conn, &user, perm).await?;
        if added_perm {
            log::info!("Added {perm} to user {username}");
        } else {
            log::info!("User {username} already had permission {perm}, no change to database");
        }
    }
    Ok(())
}
