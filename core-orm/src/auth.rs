use std::{collections::HashSet, fmt::Display, io::Read, path::Path, str::FromStr};

use anyhow::Context;
use axum_login::{AuthUser, AuthnBackend, UserId};
use jsonwebtoken::{DecodingKey, EncodingKey};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::{MySqlConn, PoolWrapper};

pub mod api;

pub type AuthSession = axum_login::AuthSession<WebBackend>;

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub email: String,
    password: String,
}

impl User {
    pub async fn load_from_db(
        conn: &mut MySqlConn,
        username: &str,
    ) -> Result<Option<Self>, WebAuthError> {
        let opt_user = sqlx::query_as!(
            Self,
            "SELECT id,username,email,password FROM v_auth_user WHERE username = ?",
            username
        )
        .fetch_optional(&mut *conn)
        .await?;
        Ok(opt_user)
    }

    pub async fn all_associated_emails(
        &self,
        _conn: &mut MySqlConn,
    ) -> anyhow::Result<Vec<String>> {
        // TODO: this should query the database to find other emails associated with this user
        Ok(vec![self.email.clone()])
    }
}

impl std::fmt::Debug for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Per the axum-login example (https://github.com/maxcountryman/axum-login/blob/main/examples/sqlite/src/users.rs)
        // manually implemented to not log passwords
        f.debug_struct("User")
            .field("id", &self.id)
            .field("username", &self.username)
            .field("email", &self.email)
            .field("password", &"[redacted]")
            .finish()
    }
}

impl AuthUser for User {
    type Id = i64;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn session_auth_hash(&self) -> &[u8] {
        // At least for now, we'll use the password hash like the axum-login
        // example shows.
        self.password.as_bytes()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Credentials {
    pub username: String,
    pub password: String,
    pub next: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WebBackend {
    pool: PoolWrapper,
}

impl WebBackend {
    pub fn new(pool: PoolWrapper) -> Self {
        WebBackend { pool }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WebAuthError {
    #[error(transparent)]
    SqlError(#[from] sqlx::Error),
    #[error(transparent)]
    TaskError(#[from] tokio::task::JoinError),
    #[error("Error checking password hash: {0:?}")]
    HashError(String),
}

impl From<djangohashers::HasherError> for WebAuthError {
    fn from(value: djangohashers::HasherError) -> Self {
        let s = format!("{value:?}");
        Self::HashError(s)
    }
}

impl AuthnBackend for WebBackend {
    type User = User;
    type Credentials = Credentials;
    type Error = WebAuthError;

    async fn authenticate(
        &self,
        creds: Self::Credentials,
    ) -> Result<Option<Self::User>, Self::Error> {
        let mut conn = self.pool.get_connection().await?;
        let user = User::load_from_db(&mut conn, &creds.username).await?;

        if user.is_none() {
            return Ok(None);
        }

        let user = user.unwrap();
        let user_pw = user.password.clone();
        let form_pw = creds.password.clone();
        let pw_valid = tokio::task::spawn_blocking(move || {
            // The axum-login example suggests doing password validation in a separate
            // thread since it can be slow.
            djangohashers::check_password(&form_pw, &user_pw)
        })
        .await??;

        if pw_valid {
            Ok(Some(user))
        } else {
            Ok(None)
        }
    }

    async fn get_user(&self, user_id: &UserId<Self>) -> Result<Option<Self::User>, Self::Error> {
        let mut conn = self.pool.get_connection().await?;
        let user: Option<Self::User> = sqlx::query_as!(
            User,
            "SELECT id,username,email,password FROM v_auth_user WHERE id = ?",
            user_id
        )
        .fetch_optional(&mut *conn)
        .await?;

        Ok(user)
    }
}

pub fn load_jwt_hmac_secret(file: &Path) -> anyhow::Result<(EncodingKey, DecodingKey)> {
    let mut f = std::fs::File::open(file).with_context(|| {
        format!(
            "An error occurred opening the HMAC secrets file, {}",
            file.display()
        )
    })?;
    let mut buf = Vec::with_capacity(256 / 8);
    f.read_to_end(&mut buf).with_context(|| {
        format!(
            "An error occurred reading the HMAC secrets file, {}",
            file.display()
        )
    })?;

    let encoding = EncodingKey::from_secret(&buf);
    let decoding = DecodingKey::from_secret(&buf);
    Ok((encoding, decoding))
}

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, strum::Display, strum::EnumString, strum::IntoStaticStr,
)]
#[strum(serialize_all = "UPPERCASE")]
pub enum Permission {
    Admin,
    Query,
    Submit,
    Download,
}

impl Permission {
    pub(crate) async fn get_id(&self, conn: &mut MySqlConn) -> sqlx::Result<Option<i32>> {
        let s: &'static str = self.into();
        let id = sqlx::query!("SELECT id FROM auth_prior_permissions WHERE tag = ?", s)
            .fetch_optional(conn)
            .await?
            .map(|rec| rec.id);
        Ok(id)
    }
}

#[derive(Debug, Clone)]
pub struct PermSet(HashSet<Permission>);

impl PermSet {
    pub fn has_perm(&self, perm: &Permission) -> bool {
        self.0.contains(perm)
    }

    pub async fn load_from_db(conn: &mut MySqlConn, user: &User) -> anyhow::Result<Self> {
        let perm_rows = sqlx::query!(
            "SELECT perm_id,tag FROM auth_prior_user_permissions LEFT JOIN auth_prior_permissions ON auth_prior_user_permissions.perm_id = auth_prior_permissions.id WHERE user_id = ?",
            user.id
        ).fetch_all(conn)
        .await?;

        let mut perm_set = HashSet::new();
        for row in perm_rows {
            // let tag = row.tag.ok_or_else(|| anyhow::anyhow!("Permission ID {} did not have an associated tag", row.perm_id))?;
            let tag = if let Some(tag) = row.tag {
                tag
            } else {
                log::error!(
                    "Permission ID {} did not have an associated tag",
                    row.perm_id
                );
                continue;
            };

            let perm = Permission::from_str(&tag).with_context(|| {
                format!("Could not convert permission tag '{tag}' to a concrete permission")
            })?;
            perm_set.insert(perm);
        }

        Ok(Self(perm_set))
    }
}

impl Display for PermSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut tags: Vec<&'static str> = self.0.iter().map(|perm| perm.into()).collect();
        tags.sort();
        let joined_tags = tags.join(", ");
        write!(f, "{joined_tags}")
    }
}
