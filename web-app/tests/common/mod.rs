use djangohashers;
use orm::MySqlConn;

pub(crate) async fn add_test_user(conn: &mut MySqlConn) -> anyhow::Result<()> {
    let pw_hash = djangohashers::make_password("qwerty");
    sqlx::query!(
        "INSERT INTO v_auth_user(password,username,email) VALUES(?,?,?)",
        pw_hash,
        "testuser",
        "test@test.net"
    )
    .execute(conn)
    .await?;
    Ok(())
}
