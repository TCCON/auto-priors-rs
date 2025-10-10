pub(crate) fn strip_p<T: std::fmt::Display>(
    s: T,
    _: &dyn askama::Values,
) -> askama::Result<String> {
    let s = s.to_string();
    Ok(s.trim_start_matches("<p>")
        .trim_end_matches("</p>")
        .to_string())
}
