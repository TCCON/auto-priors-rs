use anyhow::Context;
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use jsonwebtoken::{EncodingKey, Header};
use orm::MySqlConn;
use rand::{rng, RngCore};
use serde::{Deserialize, Serialize};
use sqlx::Connection;

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
pub(crate) struct RefreshClaims {
    /// The token issuer (this will be us)
    iss: String,

    /// The subject of the token (this will be the user, identified by an email usually)
    sub: String,

    /// UTC expiration date & time as a Unix timestamp
    exp: i64,

    /// A sufficiently random unique ID for this token, essentially the user's API key.
    jti: String,
}

impl RefreshClaims {
    fn new(user_email: String) -> Self {
        let exp = chrono::Utc::now() + chrono::Days::new(365);
        // TODO: decide if I want the issuer to be set in the config
        // TODO: generate a cryptographically random ID to serve as the API token
        Self {
            iss: "ggg2020-priors".to_string(),
            sub: user_email,
            exp: exp.timestamp(),
            jti: generate_api_key(),
        }
    }
}

pub(crate) async fn generate_refresh_token(
    conn: &mut MySqlConn,
    user: User,
    nickname: &str,
    key: &EncodingKey,
) -> anyhow::Result<String> {
    let header = Header::default();
    let claims = RefreshClaims::new(user.email);

    // Insert an entry for this token into the database, then encode and return it.
    // Insert the entry in a transaction so that we only finalize the database entry
    // if the token encoding succeeds.
    let mut trans = conn.begin().await?;
    let x = sqlx::query!(
        "INSERT INTO auth_api_user(user_id, api_key, expires) VALUES (?,?,?)",
        user.id,
        claims.jti,
        claims.exp
    );
    let jwt = jsonwebtoken::encode(&header, &claims, key)
        .with_context(|| "An error occurred while encoding the JWT")?;
    todo!()
}

pub(crate) async fn authenticate_refresh_token(token: String) -> anyhow::Result<()> {
    // First validate the token itself - if not signed or otherwise invalid, auth fails
    // Then check against the database to confirm the token is not expired and is still
    // allowed. If not, auth fails
    // If auth succeeds, return permissions associated with this user.
    anyhow::bail!("Authentication failed")
}

pub(crate) fn generate_api_key() -> String {
    let mut r = rng();
    let mut bytes = [0; 32];
    r.fill_bytes(&mut bytes);
    URL_SAFE.encode(bytes)
}
