use clap::{Args, Subcommand};
use orm::{
    auth::{
        api::{add_user_permission, Permission},
        User,
    },
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
