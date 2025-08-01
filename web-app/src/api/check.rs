pub(crate) mod get {
    use axum::response::IntoResponse;

    pub(crate) async fn check_api_access() -> impl IntoResponse {
        "Access confirmed"
    }
}
