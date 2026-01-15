-- Defines two test sites for the standard site jobs along with periods that they are "active"
-- so that we can request jobs for them.
-- Manually specify the ID so that it's easier to create preexisting job rows for tests.
INSERT INTO
    StdSiteList (
        id,
        site_id,
        name,
        site_type,
        output_structure
    )
VALUES (
        1,
        "ci",
        "Caltech",
        "TCCON",
        "FlatModVmr"
    ),
    (
        2,
        "oc",
        "Lamont",
        "TCCON",
        "FlatModVmr"
    );

-- We will use the true locations for Caltech and Lamont, since this will
-- make it easier to compare to past runs of ginput. We want to include
-- Caltech as it is an urban site, so it will be easy to check that the
-- alternate FPIT runs using IT chemistry are clearly different.

INSERT INTO
    StdSiteInfo (
        site,
        location,
        latitude,
        longitude,
        start_date
    )
VALUES (
        1,
        "California, USA",
        34.1362,
        -118.1269,
        "2012-09-01"
    ),
    (
        2,
        "Oklahoma, USA",
        36.604,
        -97.486,
        "2008-07-01"
    );