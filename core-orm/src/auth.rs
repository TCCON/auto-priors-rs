use async_trait::async_trait;
use axum_login::{AuthUser, AuthnBackend, UserId};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::{MySqlConn, PoolWrapper};

pub mod api;

pub type AuthSession = axum_login::AuthSession<Backend>;

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub email: String,
    password: String,
}

impl User {
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
pub struct Backend {
    pool: PoolWrapper,
}

impl Backend {
    pub fn new(pool: PoolWrapper) -> Self {
        Backend { pool }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error(transparent)]
    SqlError(#[from] sqlx::Error),
    #[error(transparent)]
    TaskError(#[from] tokio::task::JoinError),
    #[error("Error checking password hash: {0:?}")]
    HashError(String),
}

impl From<djangohashers::HasherError> for AuthError {
    fn from(value: djangohashers::HasherError) -> Self {
        let s = format!("{value:?}");
        Self::HashError(s)
    }
}

#[async_trait]
impl AuthnBackend for Backend {
    type User = User;
    type Credentials = Credentials;
    type Error = AuthError;

    async fn authenticate(
        &self,
        creds: Self::Credentials,
    ) -> Result<Option<Self::User>, Self::Error> {
        let mut conn = self.pool.get_connection().await?;
        let user: Option<Self::User> = sqlx::query_as!(
            User,
            "SELECT id,username,email,password FROM v_auth_user WHERE username = ?",
            creds.username
        )
        .fetch_optional(&mut *conn)
        .await?;

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
