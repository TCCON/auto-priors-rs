-- Insert the standard and processing configurations (only one on June 1st)
-- for Park Fall (site = 1) and Lamont (site = 2) for June 1st, set
-- to complete (state = 2)
INSERT INTO
    StdSiteJobs (
        site,
        date,
        state,
        processing_key
    )
VALUES (
        1,
        "2023-06-01",
        2,
        "std-geosit"
    ),
    (
        2,
        "2023-06-01",
        2,
        "std-geosit"
    );