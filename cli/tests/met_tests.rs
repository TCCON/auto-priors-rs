use std::path::PathBuf;

use chrono::{Duration, NaiveDate};
use itertools::Itertools;
use orm::{
    config::{MetCfgKey, ProcCfgKey},
    downloading::{Downloader, WgetDownloader},
    met::MetFile,
    test_utils::{
        init_logging, make_dummy_config, make_dummy_config_with_temp_dirs, multiline_sql,
        multiline_sql_init, open_test_database,
    },
};
use tccon_priors_cli::met_download::{self, check_one_config_set_files_for_dates};
mod common;

static EXPECTED_GEOSFPIT_FILES_20180102: [&'static str; 24] = [
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0000.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0300.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0000.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0300.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0000.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0300.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0600.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0900.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1200.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1500.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1800.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_2100.V01.nc4",
];

static EXPECTED_GEOSIT_FILES_20180102: [&'static str; 8] = [
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0000.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0300.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0600.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0900.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T1200.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T1500.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T1800.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T2100.V01.nc4",
];

static EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_A: [&'static str; 24] = [
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0600.V01.nc4", // FPIT 3d met
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0600.V01.nc4", // FPIT 3d chm
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0600.V01.nc4", // FPIT 2d met
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0900.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1200.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1500.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1800.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_2100.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0600.V01.nc4", // IT 3d chm
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0900.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T1200.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T1500.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T1800.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T2100.V01.nc4",
];

static EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_B: [&'static str; 24] = [
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0000.V01.nc4", // FPIT 3d met
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0000.V01.nc4", // FPIT 3d chm
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20180102_2100.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0000.V01.nc4", // FPIT 2d met
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0600.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_0900.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1500.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_1800.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20180102_2100.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0000.V01.nc4", // IT 3d chm
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0600.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T0900.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T1500.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T1800.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2018-01-02T2100.V01.nc4",
];

static EXPECTED_GEOSIT_FILES_20180702: [&'static str; 24] = [
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0000.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0300.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0600.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0900.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1200.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1500.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1800.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T2100.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0000.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0300.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0600.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T0900.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1200.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1500.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T1800.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-07-02T2100.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T0000.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T0300.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T0600.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T0900.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T1200.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T1500.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T1800.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-07-02T2100.V01.nc4",
];

static EXPECTED_GEOS_TRANSITION_FILES: [&'static str; 56] = [
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_0000.V01.nc4", // FPIT 3d met
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_0300.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_asm_Nv.GEOS5124.20230531_2100.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_0000.V01.nc4", // FPIT 3d chm
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_0300.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_0600.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_0900.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_1200.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_1500.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_1800.V01.nc4",
    "./Nv/GEOS.fpit.asm.inst3_3d_chm_Nv.GEOS5124.20230531_2100.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-05-31T0000.V01.nc4", // IT 3d chm - since we use this
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-05-31T0300.V01.nc4", // in the alternate configuration
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-05-31T0600.V01.nc4", // before the transition, these
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-05-31T0900.V01.nc4", // are needed
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-05-31T1200.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-05-31T1500.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-05-31T1800.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-05-31T2100.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0000.V01.nc4", // IT 3d met (post transition)
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0300.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0600.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0900.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1200.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1500.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1800.V01.nc4",
    "./Nv/GEOS.it.asm.asm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T2100.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0000.V01.nc4", // IT 3d chm (post transition)
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0300.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0600.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T0900.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1200.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1500.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T1800.V01.nc4",
    "./Nv/GEOS.it.asm.chm_inst_3hr_glo_L576x361_v72.GEOS5294.2023-06-01T2100.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_0000.V01.nc4", // FPIT 2d met
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_0300.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_0600.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_0900.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_1200.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_1500.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_1800.V01.nc4",
    "./Nx/GEOS.fpit.asm.inst3_2d_asm_Nx.GEOS5124.20230531_2100.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T0000.V01.nc4", // IT 2d met (post transition)
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T0300.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T0600.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T0900.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T1200.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T1500.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T1800.V01.nc4",
    "./Nx/GEOS.it.asm.asm_inst_1hr_glo_L576x361_slv.GEOS5294.2023-06-01T2100.V01.nc4",
];

// Because the tests use a database, if we're not using testcontainers to give each test its
// own database, we have to call the tests as `$ cargo test -- --test-threads=1` to ensure only
// one test runs at a time.

/// Test that a single processing configuration's files are correctly marked as complete, incomplete,
/// and missing for different combinations of files (i.e., all present, all missing, one file from one
/// type missing, all files of one type missing).
#[tokio::test]
async fn test_check_met() {
    init_logging();
    let (mut conn, _test_db) = multiline_sql_init!("sql/check_met.sql");
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let stat_map = check_one_config_set_files_for_dates(
        &mut conn,
        &config,
        &common::test_proc_key(),
        NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()),
    )
    .await
    .unwrap();

    let stat = stat_map
        .get(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap())
        .unwrap();
    assert!(
        stat.is_complete(),
        "2020-01-01 expected to be complete, was not (state = {stat:?})"
    );

    // Should be marked as incomplete because they are each missing one of one type of file

    let stat_map = check_one_config_set_files_for_dates(
        &mut conn,
        &config,
        &common::test_proc_key(),
        NaiveDate::from_ymd_opt(2020, 2, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2020, 2, 4).unwrap()),
    )
    .await
    .unwrap();

    let stat = stat_map
        .get(&NaiveDate::from_ymd_opt(2020, 2, 1).unwrap())
        .unwrap();
    assert!(
        stat.is_incomplete(),
        "Day missing one surface met file (2020-02-01) was not marked as incomplete (state = {stat:?})"
    );
    assert_eq!(
        stat.n_found, 23,
        "Day missing one surface file (2020-02-01) has the wrong number of files found (state = {stat:?})"
    );
    assert_eq!(
        stat.n_expected, 24,
        "Day missing one surface file (2020-02-01) has the wrong number of files expected (state = {stat:?})"
    );

    let stat = stat_map
        .get(&NaiveDate::from_ymd_opt(2020, 2, 2).unwrap())
        .unwrap();
    assert!(
        stat.is_incomplete(),
        "Day missing one eta met file (2020-02-02) was not marked as incomplete (state = {stat:?})"
    );
    assert_eq!(
        stat.n_found, 23,
        "Day missing one eta met file (2020-02-02) has the wrong number of files found (state = {stat:?})"
    );
    assert_eq!(
        stat.n_expected, 24,
        "Day missing one eta met file (2020-02-02) has the wrong number of files expected (state = {stat:?})"
    );

    let stat = stat_map
        .get(&NaiveDate::from_ymd_opt(2020, 2, 3).unwrap())
        .unwrap();
    assert!(
        stat.is_incomplete(),
        "Day missing one eta chem file (2020-02-03) not marked as incomplete or has the wrong number of files (state = {stat:?})"
    );
    assert_eq!(
        stat.n_found, 23,
        "Day missing one eta chem file (2020-02-03) has the wrong number of files found (state = {stat:?})"
    );
    assert_eq!(
        stat.n_expected, 24,
        "Day missing one eta chem file (2020-02-03) has the wrong number of files expected (state = {stat:?})"
    );

    // Should also be marked as incomplete - missing all of one type of file
    let stat_map = check_one_config_set_files_for_dates(
        &mut conn,
        &config,
        &common::test_proc_key(),
        NaiveDate::from_ymd_opt(2020, 3, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2020, 3, 4).unwrap()),
    )
    .await
    .unwrap();

    let stat = stat_map
        .get(&NaiveDate::from_ymd_opt(2020, 3, 1).unwrap())
        .unwrap();
    assert!(
        stat.is_incomplete(),
        "Day missing all surface met files (2020-03-01) not marked as incomplete (state = {stat:?})"
    );
    assert_eq!(
        stat.n_found, 16,
        "Day missing all surface met files (2020-03-01) has the wrong number of files found (state = {stat:?})"
    );
    assert_eq!(
        stat.n_expected, 24,
        "Day missing all surface met files (2020-03-01) has the wrong number of files expected (state = {stat:?})"
    );

    let stat = stat_map
        .get(&NaiveDate::from_ymd_opt(2020, 3, 2).unwrap())
        .unwrap();
    assert!(
        stat.is_incomplete(),
        "Day missing all eta met files (2020-03-02) not marked as incomplete (state = {stat:?})"
    );
    assert_eq!(
        stat.n_found, 16,
        "Day missing all eta met files (2020-03-02) has the wrong number of files found (state = {stat:?})"
    );
    assert_eq!(
        stat.n_expected, 24,
        "Day missing all eta met files (2020-03-02) has the wrong number of files expected (state = {stat:?})"
    );

    let stat = stat_map
        .get(&NaiveDate::from_ymd_opt(2020, 3, 3).unwrap())
        .unwrap();
    assert!(
        stat.is_incomplete(),
        "Day missing all eta chem files (2020-03-03) not marked as incomplete or has the wrong number of files (state = {stat:?})"
    );
    assert_eq!(
        stat.n_found, 16,
        "Day missing all eta chem files (2020-03-03) has the wrong number of files found (state = {stat:?})"
    );
    assert_eq!(
        stat.n_expected, 24,
        "Day missing all eta chem files (2020-03-03) has the wrong number of files expected (state = {stat:?})"
    );

    // This day isn't in the database at all, should be marked as missing
    let stat_map = check_one_config_set_files_for_dates(
        &mut conn,
        &config,
        &common::test_proc_key(),
        NaiveDate::from_ymd_opt(2020, 4, 1).unwrap(),
        Some(NaiveDate::from_ymd_opt(2020, 4, 2).unwrap()),
    )
    .await
    .unwrap();
    let stat = stat_map
        .get(&NaiveDate::from_ymd_opt(2020, 4, 1).unwrap())
        .unwrap();
    assert!(
        stat.is_missing(),
        "Day missing all files (2020-04-01) not marked as missing (state = {stat:?})"
    );
}

/// Test that "downloading" a particular set of met files for a particular date
/// correctly stores them to disk and records them in the database. Specifically,
/// tests GEOS FP-IT for a period it should be available. (This does not actually
/// download from Goddard, only mocks that, so the test is based only on the
/// configured date ranges.)
#[tokio::test]
async fn test_geosfpit_download_by_dates() {
    init_logging();

    // Don't need any initial values in the database, just a connection to a blank database
    let (pool, _test_db) = open_test_database(true)
        .await
        .expect("Failed to open test database");
    let mut conn = pool
        .get_connection()
        .await
        .expect("Failed to acquire connection to database");

    // 2018 should be GEOS FP-IT in the default configuration
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("default_fpit")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_files_for_dates(
        &mut conn,
        &common::test_geosfpit_met_keys().iter().collect_vec(),
        NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(),
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSFPIT_FILES_20180102)
        .expect("Not all 'FP-IT' files for 2018-01-02 downloaded");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102)
        .await
        .expect("Not all 'FP-IT' files for 2018-01-02 stored in the database");
}

/// Test that "downloading" a particular set of met files for a particular date
/// correctly stores them to disk and records them in the database. Specifically,
/// tests GEOS IT for a period it should be available. (This does not actually
/// download from Goddard, only mocks that, so the test is based only on the
/// configured date ranges.)
#[tokio::test]
async fn test_geosit_download_by_dates() {
    init_logging();

    // Don't need any initial values in the database, just a connection to a blank database
    let (pool, _test_db) = open_test_database(true)
        .await
        .expect("Failed to open test database");
    let mut conn = pool
        .get_connection()
        .await
        .expect("Failed to acquire connection to database");

    // 2023 after June should be GEOS IT in the default configuration
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("default_it")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_files_for_dates(
        &mut conn,
        &common::test_geosit_met_keys().iter().collect_vec(),
        NaiveDate::from_ymd_opt(2023, 7, 2).unwrap(),
        Some(NaiveDate::from_ymd_opt(2023, 7, 3).unwrap()),
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSIT_FILES_20180702)
        .expect("Not all 'IT' files for 2023-07-02 downloaded");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSIT_FILES_20180702)
        .await
        .expect("Not all 'IT' files for 2023-07-02 stored in the database");
}

/// Tests that a transition between required met files for automatic processing
/// correctly handles the transition for a specific set of dates. This mimics
/// how one might use the `met download-missing` CLI with --start-date and
/// --end-date flags.
#[tokio::test]
async fn test_transition_in_automatic_required_mets() {
    init_logging();

    // Don't need any initial values in the database, just a connection to a blank database
    let (pool, _test_db) = open_test_database(true)
        .await
        .expect("Failed to open test database");
    let mut conn = pool
        .get_connection()
        .await
        .expect("Failed to acquire connection to database");

    // 2023 after June should be GEOS IT in the default configuration
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("default_fpit_to_it")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()),
        Some(NaiveDate::from_ymd_opt(2023, 6, 2).unwrap()),
        None,
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOS_TRANSITION_FILES)
        .expect("Not all 'GEOS FP-IT/IT' files in the 'transition period' downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOS_TRANSITION_FILES)
        .await
        .expect(
            "Not all 'GEOS FP-IT/IT' files in the 'transition period' entered in the database.",
        );
}

/// Tests that downloading a specific processing configuration's missing met files
/// successfully adds them to disk and the database. This mimics how one might use
/// the `met download-missing` CLI with a --proc-key flag.
#[tokio::test]
async fn test_download_one_proc_cfg_missing() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_geos_fpit_next_date.sql");
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("missing_fpit")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        Some(&ProcCfgKey("std-geosfpit".to_string())),
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' missing files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSFPIT_FILES_20180102)
        .expect("Not all missing 'GEOS FP-IT' files for 2018-01-02 downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102)
        .await
        .expect("Not all missing 'GEOS FP-IT' files for 2018-01-02 entered in the database.");

    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(
        res, 0,
        "Files before 2018-01-01 should not have been added to the database, but were."
    );

    // Also check that the other processing configuration was not downloaded for this day,
    // but was present in the database for the previous day (as a check we have the right
    // product key).
    let res_jan01 = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE DATE(filedate) = ? AND product_key LIKE 'geosit%'",
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    let res_jan02 = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE DATE(filedate) = ? AND product_key LIKE 'geosit%'",
        NaiveDate::from_ymd_opt(2018, 1, 2).unwrap()
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(res_jan01, 8, "There should be 8 Jan 01 GEOS IT files in the database from the test initial SQL. (This may mean the key needs updated in the check within this test.)");
    assert_eq!(
        res_jan02, 0,
        "No GEOS IT files should have been downloaded for Jan 02"
    );
}

/// Tests that downloading the missing files before the test transition gets the expected
/// sets of files, accounting for both the standard and alternate processing configuration.
/// This mimics both the service acting before the GEOS IT transition and calling the
/// `met download-missing` CLI with an --end-date flag.
#[tokio::test]
async fn test_download_pre_transition_missing() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_geos_fpit_next_date.sql");
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("missing_pre_transition")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        None,
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' missing files did not complete successfully");

    // Check for both the full set of GEOS FP-IT files and the chemistry IT files - doing this
    // separately only so I can reuse the expected FPIT files list
    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSFPIT_FILES_20180102)
        .expect("Not all missing GEOS FP-IT files for 2018-01-02 downloaded");
    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSIT_FILES_20180102)
        .expect("Not all missing GEOS IT files for 2018-01-02 downloaded");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102)
        .await
        .expect("Not all missing GEOS FP-IT files for 2018-01-02 entered in the database");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSIT_FILES_20180102)
        .await
        .expect("Not all missing GEOS IT files for 2018-01-02 entered in the database");

    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(
        res, 0,
        "Files before 2018-01-01 should not have been added to the database, but were."
    );
}

/// Tests that downloading the missing files after the test transition gets the expected
/// sets of files. This mimics both the service acting after the GEOS IT transition and calling the
/// `met download-missing` CLI with --start-date and --end-date flags.
#[tokio::test]
async fn test_download_post_transition_missing() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_geos_it_next_date.sql");
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("missing_post_transition")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    // We set the start date to 2023-07-01 to exclude the pre-transition processing configurations.
    // Otherwise, it does what it is supposed to, which is finds that all of the files needed for
    // the pre-transition period are missing and tries to download them. We can't set it to 2023-06-01
    // because then it will (again correctly) see that June is missing and "download" it.
    met_download::download_missing_files(
        &mut conn,
        Some(NaiveDate::from_ymd_opt(2023, 7, 1).unwrap()),
        Some(NaiveDate::from_ymd_opt(2023, 7, 3).unwrap()),
        None,
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' missing files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOSIT_FILES_20180702)
        .expect("Not all missing 'GEOS IT' files for 2023-07-02 downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSIT_FILES_20180702)
        .await
        .expect("Not all missing 'GEOS IT' files for 2023-07-02 entered in the database.");

    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2023, 7, 1).unwrap()
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(
        res, 0,
        "Files before 2023-07-01 should not have been added to the database, but were."
    );
}

/// Tests that the download correctly handles a transition between two different sets
/// of auto-required met files, basing the start date off of existing files, rather
/// than it being specified (as in `test_transition_in_automatic_required_mets`).
///
/// This test should mimic the most complicated use of the service's met download or
/// calling the `met download-missing` CLI with no arguments, where it needs to correctly
/// figure out which met files to download across a transition from one processing
/// configuration to another with files from the first set already present.
#[tokio::test]
async fn test_transition_in_automatic_required_mets_missing() {
    init_logging();

    let (mut conn, _test_db) =
        multiline_sql_init!("sql/check_geos_fpit_to_it_transition_next_date.sql");
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("missing_fpit_and_it")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2023, 6, 2).unwrap()),
        None,
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' missing files did not complete successfully");

    common::are_met_files_present_on_disk(tmp_dir.path(), &EXPECTED_GEOS_TRANSITION_FILES)
        .expect("Not all missing 'GEOS FP-IT/IT' files for the 'transition period' downloaded.");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOS_TRANSITION_FILES)
        .await
        .expect("Not all missing 'GEOS FP-IT/IT' files for the 'transition period' entered in the database.");

    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2023, 5, 30).unwrap()
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(
        res, 0,
        "Files before 2023-05-30 should not have been added to the database, but were."
    );
}

/// Tests that the right files are automatically downloaded when the last day
/// for that automatic set of met files was partially complete.
#[tokio::test]
async fn test_download_partial_day_from_start() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_geos_fpit_next_partial.sql");
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("missing_fpit_partial")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        None,
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' missing files did not complete successfully");

    // Because of how we're testing the download, only the "missing" files will actually be downloaded
    // so we can't check for all the 2018-01-02 files (hence the "partial" arrays)
    common::are_met_files_present_on_disk(
        tmp_dir.path(),
        &EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_A,
    )
    .expect("Not all missing 'GEOS FP-IT' files for the partial day downloaded.");
    common::are_met_file_present_in_database(
        &mut conn,
        &EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_A,
    )
    .await
    .expect("Not all missing 'GEOS FP-IT' files for the partial entered in the database.");

    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(
        res, 0,
        "Files before 2018-01-01 should not have been added to the database, but were."
    );
}

/// Tests that the right files are automatically downloaded when the last day
/// for that automatic set of met files was partially complete with the files
/// being more scattered in time, whereas `test_download_partial_day_from_start`
/// gives it a day that just cut off partway through.
#[tokio::test]
async fn test_download_partial_day_scattered() {
    init_logging();

    let (mut conn, _test_db) =
        multiline_sql_init!("sql/check_geos_fpit_next_partial_scattered.sql");
    let (config, tmp_dir) = make_dummy_config_with_temp_dirs("missing_fpit_partial_scattered")
        .expect("Failed to set up test config and temp directories");
    let downloader = common::TestDownloader::new();

    met_download::download_missing_files(
        &mut conn,
        None, // should pick up the start date from the existing files
        Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
        None,
        &config,
        downloader,
        false,
    )
    .await
    .expect("'Downloading' missing files did not complete successfully");

    // Because of how we're testing the download, only the "missing" files will actually be downloaded
    // so we can't check for all the 2018-01-02 files (hence the "partial" arrays)
    common::are_met_files_present_on_disk(
        tmp_dir.path(),
        &EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_B,
    )
    .expect("Not all missing 'GEOS FP-IT' files for the partial day downloaded.");
    common::are_met_file_present_in_database(
        &mut conn,
        &EXPECTED_GEOSFPIT_FILES_20180102_PARTIAL_B,
    )
    .await
    .expect("Not all missing 'GEOS FP-IT' files for the partial entered in the database.");

    // Double check that no files before the previously available day were downloaded
    let res = sqlx::query!(
        "SELECT COUNT(*) as count FROM MetFiles WHERE filedate < ?",
        NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()
    )
    .fetch_one(&mut *conn)
    .await
    .expect("Query to check for too-early files failed")
    .count;

    assert_eq!(
        res, 0,
        "Files before 2018-01-01 should not have been added to the database, but were."
    );
}

/// Tests the ability to add already-downloaded met files to the database.
#[tokio::test]
async fn test_met_rescanning() {
    init_logging();

    // Don't need any initial values in the database, just a connection to a blank database
    let (pool, _test_db) = open_test_database(true)
        .await
        .expect("Failed to open test database");
    let mut conn = pool
        .get_connection()
        .await
        .expect("Failed to acquire connection to database");
    let (config, _tmp_dir) = make_dummy_config_with_temp_dirs("met_rescan")
        .expect("Failed to set up test config and temp directories");

    // Let's use the TestDownloader to "download" some files without adding them to the database
    // This should only download the 3 FPIT file types.
    let test_date = NaiveDate::from_ymd_opt(2018, 1, 2).unwrap();
    let geos_fpit_cfgs = config
        .get_mets_for_processing_config(&ProcCfgKey("std-geosfpit".to_string()))
        .expect("geosfpit key missing from test download configs");
    for dl_cfg in geos_fpit_cfgs {
        let mut downloader = common::TestDownloader::new();
        for file_time in dl_cfg.cfg.times_on_day(test_date) {
            let file_url = file_time.format(&dl_cfg.cfg.url_pattern).to_string();
            downloader.add_file_to_download(file_url).unwrap();
        }
        downloader
            .download_files(&dl_cfg.cfg.download_dir)
            .expect("Downloading files failed");
    }

    // Now we should have all the files from 2018-01-02
    orm::met::rescan_met_files(
        &mut conn,
        Some(test_date),
        Some(test_date + Duration::days(1)),
        &config,
        None,
        false,
    )
    .await
    .expect("Rescanning for 2018-01-02 failed");
    common::are_met_file_present_in_database(&mut conn, &EXPECTED_GEOSFPIT_FILES_20180102)
        .await
        .expect("Rescanning 2018-01-02 failed to find all expected files");
}

/// Test that met_download::get_date_iter_for_specified_met prioritizes
/// input start and end dates over anything in the database.
#[tokio::test]
async fn test_single_met_dates_user_override() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd_opt(2014, 5, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2016, 3, 1).unwrap();

    let mut date_iter = met_download::get_date_iter_for_specified_met(
        &mut conn,
        Some(start),
        Some(end),
        &config,
        &common::test_geosfpit_met_keys()[0],
        false,
    )
    .await
    .unwrap();

    // should ignore everything in the database
    assert_eq!(
        date_iter.next(),
        Some(start),
        "First date in iterator was incorrect"
    );
    assert_eq!(
        date_iter.last(),
        Some(end - Duration::days(1)),
        "Final date in iterator was incorrect"
    );
}

/// Tests that met_download::get_date_iter_for_specified_met can correctly pick up
/// the last day a single type of met files was downloaded and figure out to end
/// when it is no longer required by any of the processing.
#[tokio::test]
async fn test_single_met_dates_start_from_db() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let mut date_iter = met_download::get_date_iter_for_specified_met(
        &mut conn,
        None,
        None,
        &config,
        &common::test_geosfpit_met_keys()[0],
        true,
    )
    .await
    .unwrap();

    // should start on the day after we have FPIT met data and stop on the last day before the pure GEOS IT
    // processing starts.
    assert_eq!(
        date_iter.next(),
        Some(NaiveDate::from_ymd_opt(2018, 1, 2).unwrap()),
        "First date in iterator was incorrect"
    );
    assert_eq!(
        date_iter.last(),
        Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()),
        "Final date in iterator was incorrect"
    );
}

/// Tests that met_download::get_date_iter_for_specified_met can correctly pick up
/// the first day needed across the processing configurations and figure out to end
/// when it is no longer required by any of the processing.
#[tokio::test]
async fn test_single_met_start_from_dl_config() {
    init_logging();

    // Don't need any initial values in the database, just a connection to a blank database
    let (pool, _test_db) = open_test_database(true)
        .await
        .expect("Failed to open test database");
    let mut conn = pool
        .get_connection()
        .await
        .expect("Failed to acquire connection to database");
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let geos_fpit_key = common::test_geosfpit_met_keys()[0].clone();

    let mut date_iter = met_download::get_date_iter_for_specified_met(
        &mut conn,
        None,
        None,
        &config,
        &geos_fpit_key,
        true, // we need to respect the defaults to get the iterator to end at the last date the processing wants it, rather than today
    )
    .await
    .unwrap();

    // should start on the day defined as the earliest day FPIT is needed and stop on the last day before geos-it is set to start
    assert_eq!(
        date_iter.next(),
        Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
        "First date in iterator was incorrect when all files have the same earliest_date values"
    );
    assert_eq!(
        date_iter.last(),
        Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()),
        "Final date in iterator was incorrect when all files have the same earliest_date values"
    );
}

/// Tests that met_download::get_date_iter_for_specified_met can correctly pick up
/// the first day needed across the processing configurations and figure out to end
/// when it is no longer required by any of the processing.
#[tokio::test]
async fn test_single_met_cross_boundary_with_defaults() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd_opt(2023, 5, 20).unwrap();
    let end = NaiveDate::from_ymd_opt(2023, 8, 1).unwrap();
    let mut date_iter = met_download::get_date_iter_for_specified_met(
        &mut conn,
        Some(start),
        Some(end),
        &config,
        &common::test_geosfpit_met_keys()[0],
        true,
    )
    .await
    .unwrap();

    // should start on the day we requested and stop on the day before geos-it starts
    assert_eq!(
        date_iter.next(),
        Some(start),
        "First date in iterator was incorrect"
    );
    assert_eq!(
        date_iter.last(),
        Some(NaiveDate::from_ymd_opt(2023, 5, 31).unwrap()),
        "Final date in iterator was incorrect"
    );
}

/// Tests that met_download::get_date_iter_for_specified_met can correctly pick up
/// the first day needed across the processing configurations and ignore the set
/// processing configuration dates.
#[tokio::test]
async fn test_single_met_cross_boundary_ignoring_defaults() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_geos_fpit_plus_it_single_met.sql");
    let config = make_dummy_config(PathBuf::from(".")).expect("Failed to make test configuration");

    let start = NaiveDate::from_ymd_opt(2023, 5, 20).unwrap();
    let end = NaiveDate::from_ymd_opt(2023, 8, 1).unwrap();
    let mut date_iter = met_download::get_date_iter_for_specified_met(
        &mut conn,
        Some(start),
        Some(end),
        &config,
        &common::test_geosfpit_met_keys()[0],
        false,
    )
    .await
    .unwrap();

    // should start on the day we requested and stop on the day before our end
    assert_eq!(
        date_iter.next(),
        Some(start),
        "First date in iterator was incorrect"
    );
    assert_eq!(
        date_iter.last(),
        Some(end - Duration::days(1)),
        "Final date in iterator was incorrect"
    );
}

/// Tests the function `MetFile::get_file_by_name`, which must retrieve a single
/// file from the database given its basename.
#[tokio::test]
async fn test_find_met_file_by_name() {
    init_logging();

    let (mut conn, _test_db) = multiline_sql_init!("sql/check_finding_met_file.sql");
    let check_some = MetFile::get_file_by_name(&mut conn, "geos_surf_test_20200101_0000.nc")
        .await
        .expect("Database query failed or returned >1 file");

    assert!(
        check_some.is_some(),
        "Failed to find file geos_surf_test_20200101_0000.nc"
    );
    assert_eq!(
        check_some.unwrap().file_path,
        PathBuf::from("/data/met/Nx/geos_surf_test_20200101_0000.nc")
    );

    let check_none = MetFile::get_file_by_name(&mut conn, "bob")
        .await
        .expect("Database query failed or returned >1 file");

    assert!(
        check_none.is_none(),
        "Erroneously matched the file name 'bob'"
    );
}

/// Tests the function `MetFile::get_file_by_full_path`, which must retrieve a single
/// file from the database given its full path.
#[tokio::test]
async fn test_find_met_file_by_path() {
    init_logging();

    let test_path = PathBuf::from("/data/met/Nx/geos_surf_test_20200101_0000.nc");
    let (mut conn, _test_db) = multiline_sql_init!("sql/check_finding_met_path.sql");
    let check_some = MetFile::get_file_by_full_path(&mut conn, &test_path)
        .await
        .expect("Database query for full path failed or returned >1 file");

    assert!(
        check_some.is_some(),
        "Failed to find file /data/met/Nx/geos_surf_test_20200101_0000.nc"
    );
    assert_eq!(check_some.unwrap().file_path, test_path);

    let check_none = MetFile::get_file_by_full_path(
        &mut conn,
        &PathBuf::from("geos_surf_test_20200101_0000.nc"),
    )
    .await
    .expect("Database query for base name failed or returned >1 file");
    assert!(
        check_none.is_none(),
        "Erroneously matched the basename when checking for full path"
    );
}

/// Tests truly downloading a file, specifically one of the freely-available GEOS FP surface files.
#[test]
#[ignore = "requires downloading a file"]
fn test_geosfp_download() {
    init_logging();

    let tmp_dir =
        tempdir::TempDir::new("test_geosfp_download").expect("Failed to make temporary directory");
    let config =
        make_dummy_config(tmp_dir.path().to_owned()).expect("Failed to make test configuration");

    let fp_dl_cfg = config
        .data
        .met_download
        .get(&MetCfgKey("geosfp-surf-met".to_string()))
        .expect("Test config should define the 2D GEOS-FP met file type for download");

    std::fs::create_dir(&fp_dl_cfg.download_dir)
        .expect("Could not create temporary download directory for GEOS FP");

    let test_datetime = NaiveDate::from_ymd_opt(2018, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let test_url = test_datetime.format(&fp_dl_cfg.url_pattern).to_string();
    let mut downloader = WgetDownloader::new_with_verbosity(0);
    downloader.add_file_to_download(test_url).unwrap();
    downloader
        .download_files(&fp_dl_cfg.download_dir)
        .expect("Failed to download GEOS FP file");

    let expected_file = fp_dl_cfg
        .download_dir
        .join("GEOS.fp.asm.inst3_2d_asm_Nx.20180101_0000.V01.nc4");
    println!("Expected file = {}", expected_file.display());
    assert!(
        expected_file.exists(),
        "Download succeeded, but expected file is not present"
    );

    let expected_checksum = hex_literal::hex!("ade5e528d45f55b9eb37e1676e782ec3");
    let actual_checksum =
        common::md5sum(&expected_file).expect("Could not compute checksum on downloaded file");
    assert_eq!(
        actual_checksum, expected_checksum,
        "GEOS FP file checksum did not match"
    );
}
