use crate::{
    auth::{PermSet, Permission},
    error::ApiAuthError,
    MySqlConn,
};
use anyhow::Context;
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use jsonwebtoken::{DecodingKey, EncodingKey, Header};
use rand::{rng, RngCore};
use serde::{Deserialize, Serialize};
use sqlx::{prelude::FromRow, Connection};

use crate::auth::User;

// From what I've read, JSON web tokens seem to be the way to go, and there is a
// crate to use them: https://docs.rs/jsonwebtoken/latest/jsonwebtoken/index.html
// I've also thought it through, and I don't think the token needs encrypted.
// The risk is if a bad actor acquires the token, they can impersonate the user,
// but since the server would decrypt it, just the bad actor having the encrypted
// token would let them log in.
//
// I've also read a bit on the concept of a refresh token, as it's never made sense
// to me to have this long lived refresh token that's used to get a short lived access
// token.  The point is that the access token can allow authentication without querying
// the database - i.e., if a valid signed token is presented, that user has access.
// Then those tokens have short lives so that they expire quickly enough to limit the
// damage if they are compromised. The refresh token causes the API to hit the DB to
// confirm the user still has permission to access resources, so if access is revoked,
// they won't be able to get that token.
//
// For this, I'm going to start with the refresh token only. I don't think we're at the
// level where I need to worry about heavy use of the DB. I will implement rate limiting
// - https://github.com/benwis/tower-governor looks like a good, widely-used option, so
// that should protect against DoS to a degree. In the future, if I need to move to a
// refresh+access model, I will - and will update EGI-RS to handle that.

// Steps to implement API authorization:
//
// 1. Get JWT generation working through the CLI
// 2. Get JWT validation and authentication vs. the database working
// 3. Implement (potentially via middleware) route protection.
// 4. Add a web page for users to get tokens. Likely I will allow multiple tokens,
//    but they will expire after ~1 yr (that seems to be a security best practice too,
//    force people to update their tokens in case they have been compromised). Will
//    need to be able to send emails warning of expired tokens.

#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshClaims {
    /// The token issuer (this will be us)
    iss: String,

    /// The subject of the token (this will be the username)
    sub: String,

    /// UTC expiration date & time as a Unix timestamp
    exp: i64,

    /// A sufficiently random unique ID for this token, essentially the user's API key.
    jti: String,
}

impl RefreshClaims {
    fn new(username: String) -> Self {
        let exp = chrono::Utc::now() + chrono::Days::new(365);
        // TODO: decide if I want the issuer to be set in the config
        Self {
            iss: "ggg2020-priors".to_string(),
            sub: username,
            exp: exp.timestamp(),
            jti: generate_api_key(),
        }
    }

    fn new_expire_after(username: String, expire_after: chrono::Duration) -> Self {
        let exp = chrono::Utc::now() + expire_after;
        // TODO: decide if I want the issuer to be set in the config
        Self {
            iss: "ggg2020-priors".to_string(),
            sub: username,
            exp: exp.timestamp(),
            jti: generate_api_key(),
        }
    }

    fn sql_exp(&self) -> chrono::NaiveDateTime {
        let exp = chrono::DateTime::from_timestamp_nanos(self.exp * 1_000_000_000);
        exp.naive_utc()
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct RefreshTokenDb {
    id: i64,
    user_id: i64,
    api_key: String,
    expires: chrono::NaiveDateTime,
    nickname: String,
}

impl RefreshTokenDb {
    pub(crate) async fn load_from_db(
        conn: &mut MySqlConn,
        user_id: i64,
        api_key: &str,
    ) -> Result<Option<RefreshTokenDb>, sqlx::Error> {
        sqlx::query_as!(
            RefreshTokenDb,
            "SELECT * FROM auth_api_user WHERE user_id = ? AND api_key = ?",
            user_id,
            api_key
        )
        .fetch_optional(conn)
        .await
    }
}

pub async fn generate_refresh_token(
    conn: &mut MySqlConn,
    user: User,
    nickname: &str,
    key: &EncodingKey,
    expire_after: Option<chrono::Duration>,
) -> anyhow::Result<String> {
    let header = Header::default();
    let claims = if let Some(after) = expire_after {
        RefreshClaims::new_expire_after(user.username, after)
    } else {
        RefreshClaims::new(user.username)
    };

    // Insert an entry for this token into the database, then encode and return it.
    // Insert the entry in a transaction so that we only finalize the database entry
    // if the token encoding succeeds.
    let mut trans = conn.begin().await?;
    sqlx::query!(
        "INSERT INTO auth_api_user(user_id, api_key, expires, nickname) VALUES (?,?,?,?)",
        user.id,
        claims.jti,
        claims.sql_exp(),
        nickname
    )
    .execute(&mut *trans)
    .await
    .with_context(|| {
        format!("An error occurred while adding the API key '{nickname}' to the database")
    })?;

    let jwt = jsonwebtoken::encode(&header, &claims, key)
        .with_context(|| "An error occurred while encoding the JWT")?;

    trans.commit().await.with_context(|| {
        format!("An error occurred while committing the transaction to add API key '{nickname}'")
    })?;

    Ok(jwt)
}

pub async fn authenticate_refresh_token(
    conn: &mut MySqlConn,
    token: &str,
    decode_key: &DecodingKey,
) -> Result<(User, PermSet), ApiAuthError> {
    // First validate the token itself - if not signed or otherwise invalid, auth fails
    let val = jsonwebtoken::Validation::default();
    let res = jsonwebtoken::decode::<RefreshClaims>(token, decode_key, &val);
    let claims = match res {
        Ok(tok) => tok.claims,
        Err(e) => match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                return Err(ApiAuthError::TokenInvalidExpired)
            }
            _ => return Err(ApiAuthError::TokenInvalidOther(e)),
        },
    };

    // Then check against the database to confirm the token is not expired and is still
    // allowed. If not, auth fails
    let user = User::load_from_db(conn, &claims.sub)
        .await
        .map_err(|e| ApiAuthError::Other(e.to_string()))?
        .ok_or_else(|| ApiAuthError::TokenNotFound)?;

    let _api_key_opt = RefreshTokenDb::load_from_db(conn, user.id, &claims.jti)
        .await?
        .ok_or_else(|| ApiAuthError::TokenNotFound)?;

    let perms = PermSet::load_from_db(conn, &user)
        .await
        .map_err(|e| ApiAuthError::Other(e.to_string()))?;
    Ok((user, perms))
}

/// Add a new permission for the given user to the database.
///
/// Returns an error if the permission was not in the database (which is a bug)
/// or if adding the permission failed.
pub async fn add_user_permission(
    conn: &mut MySqlConn,
    user: &User,
    perm: &Permission,
) -> anyhow::Result<bool> {
    let perm_id = perm
        .get_id(conn)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Permission {perm} not found in database"))?;

    if !user_has_perm(conn, user, perm_id).await? {
        add_permission(conn, user, perm_id).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

async fn user_has_perm(conn: &mut MySqlConn, user: &User, perm_id: i32) -> sqlx::Result<bool> {
    let opt = sqlx::query!(
        "SELECT * FROM auth_prior_user_permissions WHERE user_id = ? AND perm_id = ?",
        user.id,
        perm_id
    )
    .fetch_optional(conn)
    .await?;
    Ok(opt.is_some())
}

async fn add_permission(conn: &mut MySqlConn, user: &User, perm_id: i32) -> sqlx::Result<()> {
    sqlx::query!(
        "INSERT INTO auth_prior_user_permissions(user_id, perm_id) VALUES (?, ?)",
        user.id,
        perm_id
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Remove a permission from the user.
///
/// Returns `Ok(true)` if the user had that permission and it was successfully removed.
/// Returns `Ok(false)` if the user did not have that permission.
/// Returns an error if any of the calls to the database failed.
pub async fn delete_user_permission(
    conn: &mut MySqlConn,
    user: &User,
    perm: &Permission,
) -> anyhow::Result<bool> {
    let perm_id = perm
        .get_id(conn)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Permission {perm} not found in database"))?;

    if user_has_perm(conn, user, perm_id).await? {
        delete_permission(conn, user, perm_id).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

async fn delete_permission(conn: &mut MySqlConn, user: &User, perm_id: i32) -> sqlx::Result<()> {
    sqlx::query!(
        "DELETE FROM auth_prior_user_permissions WHERE user_id = ? AND perm_id = ?",
        user.id,
        perm_id
    )
    .execute(conn)
    .await?;
    Ok(())
}

pub(crate) fn generate_api_key() -> String {
    let mut r = rng();
    let mut bytes = [0; 32];
    r.fill_bytes(&mut bytes);
    URL_SAFE.encode(bytes)
}
