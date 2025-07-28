# Setup

## Initial setup

Installing Rust and the SQL database server, setting up the database, compiling the priors automation, and generating the initial configuration are all described in the [README](https://github.com/TCCON/auto-priors-rs/blob/main/README.md).

## Installing ginput

This program calls [`ginput`](https://github.com/TCCON/py-ginput) to generate GGG2020 priors.
Currently, it can only do so by using the `ginput` command line interface, through the `auto` subcommand.
This will require `ginput` v1.2.0 or greater _or_ using the `lts/v1.0.6` branch.
Where `ginput` is installed is configurable; however, we recommend placing it in a `python` subdirectory
of this repository and creating the conda/mamba environment for it within the `ginput` directory.
That is, use the [manual installation](https://ginput.readthedocs.io/en/latest/ginput_usage/installation_and_first_steps.html#manual-installation)
method in the ginput docs and create a directory structure like so:

```
.
├── Cargo.lock
├── Cargo.toml
├── cli
├── core-orm
├── Makefile
├── python
│   └── ginput
│       ├── .condaenv
│       ├── (other ginput code)
│       └── run_ginput.py
├── README.md
├── service
├── target
├── testing
└── var
```

where `.condaenv` contains the conda or mamba environment used to run ginput.

## Downloading met data

GGG2020 uses GEOS FP-IT or GEOS IT met data, specifically three products from each:

- `asm_inst_3hr_glo_L576x361_v72` (assimilated 3D meteorological variables output every 3 hours, instantaneous on 72 hybrid levels),
- `chm_inst_3hr_glo_L576x361_v72` (assimilated 3D chemical variables output every 3 hours, instantaneous on 72 hybrid levels), and
- `asm_inst_1hr_glo_L576x361_slv` (assimilated 2D meteorological variables output every hour, though only the same 3-hourly cadence as the 3D variables is required).

GEOS FP-IT is the predecessor, and in the standard TCCON GGG2020 priors, it is used as priors for all data through 31 Mar 2024.
GEOS IT is used for all data from 1 Apr 2024 on.
Note that GEOS FP-IT will no longer be produced sometime in 2025, and may not be available for download after that.

Both GEOS FP-IT and IT require a data subscription to access.
As of 27 Jul 2025, contact Robert Lucchesi (robert.a.lucchesi@nasa.gov) to arrange that access.
