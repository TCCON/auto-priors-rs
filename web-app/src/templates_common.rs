pub(crate) trait BaseContext {
    fn subtitle(&self) -> &str;
    fn page_id(&self) -> &str;
    fn root_uri(&self) -> &str;
    fn username(&self) -> Option<&str>;
    fn uri(&self, resource: &str) -> String {
        format!(
            "{}/{}",
            self.root_uri().trim_end_matches("/"),
            resource.trim_start_matches("/")
        )
    }
}

pub(crate) struct Sblink {
    pub(crate) classes: &'static str,
    pub(crate) url: String,
    pub(crate) text: String,
}

pub(crate) trait ContextWithSidebar {
    fn sblink(
        &self,
        resource_uri: &str,
        text: &str,
        curr_page_id: &str,
        link_page_id: &str,
    ) -> Sblink;
    fn sblink_default_id(&self, resource_uri: &str, text: &str, link_page_id: &str) -> Sblink {
        let curr_page_id = resource_uri.trim_start_matches("/");
        self.sblink(resource_uri, text, curr_page_id, link_page_id)
    }
}

pub(crate) fn sblink_inner(
    root_uri: &str,
    resource_uri: &str,
    text: &str,
    curr_page_id: &str,
    link_page_id: &str,
) -> Sblink {
    let classes = if curr_page_id == link_page_id {
        "sidebar-current-page"
    } else {
        ""
    };

    let url = format!(
        "{}/{}",
        root_uri.trim_end_matches("/"),
        resource_uri.trim_start_matches("/")
    );
    // format!(r#"<div class="{classes}"><a href="{url}">{text}</a></div>"#)
    // {{ Self::sblink(self, "/", "Home", page_id, "home") | safe }}
    // {{ Self::sblink_default_id(self, "/job-statuses", "Your jobs statuses", page_id)}}
    // {{ Self::sblink_default_id(self, "/submit-job", "Submit job", page_id)}}
    // {{ Self::sblink_default_id(self, "/job-queue", "Job queue", page_id)}}
    // {{ Self::sblink_default_id(self, "/std-sites", "Standard sites", page_id)}}
    // {{ Self::sblink_default_id(self, "/met-data", "Met data", page_id)}}
    Sblink {
        classes,
        url,
        text: text.to_string(),
    }
}
