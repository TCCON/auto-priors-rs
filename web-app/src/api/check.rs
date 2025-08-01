pub(crate) mod get {
    use askama_axum::IntoResponse;

    pub(crate) async fn check_api_access() -> impl IntoResponse {
        "Access confirmed"
    }
}
