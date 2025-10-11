use serde_json::{json, Value};

pub(crate) fn active_jobs_output_example() -> Value {
    json!([
        {
            "job_id": 56676,
            "state": "Pending",
            "sites": [
                {
                    "site_id": "aa",
                    "lat": 12.34,
                    "lon": -56.78
                }
            ],
            "start_date": "2025-01-01",
            "end_date": "2025-01-02",
            "email": "test@example.com",
            "met_key": null,
            "ginput_key": null,
            "mod_fmt": "Text",
            "vmr_fmt": "Text",
            "map_fmt": "Text",
            "submit_time": "2025-08-04T23:08:01",
            "complete_time": null,
            "download_url": null
        },
        {
            "job_id": 56677,
            "state": "Pending",
            "sites": [
                {
                    "site_id": "wg",
                    "lat": null,
                    "lon": null
                }
            ],
            "start_date": "2023-01-01",
            "end_date": "2023-01-02",
            "email": "test@example.com",
            "met_key": null,
            "ginput_key": null,
            "mod_fmt": "Text",
            "vmr_fmt": "Text",
            "map_fmt": "Text",
            "submit_time": "2025-08-07T09:46:12",
            "complete_time": null,
            "download_url": null
        }
    ])
}
