# Setup

## Initial setup

Installing Rust and the SQL database server, setting up the database, compiling the priors automation, and generating the initial configuration are all described in the [README](https://github.com/TCCON/auto-priors-rs/blob/main/README.md).
Note that, for proper deployment, you should compile with `cargo build --release` and use the programs placed in the `target/release` subdirectory, as the optimizations turned on in release mode make the code noticeably faster.

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

## Running the service

The `tccon-priors-service` program is intended to be run in the background on the host system.
Our approach is to create a [systemd](https://systemd.io/) service, but any approach that ensures
that a single instance of `tccon-priors-service` is running at all times.
If you want to use `systemd` and you have `sudo` priviledges, then you would create a file similar
to the following example in `/etc/systemd/system`, named `automodrust.service` or similar:

```
[Unit]
Description=Priors generation automation
# Since the service requires network access, we must wait for the network
# services to be brought up.
After=network.target

[Service]
# Not strictly necessary, but good to keep any files created relative to the
# current directory contained - though there should not be any normally.
WorkingDirectory=/run/automodust

# This assumes that you moved/copied the executable into /usr/local/bin - otherwise,
# point to the appropriate path. You can also change the debugging level for the file
# if you want less logging, and the path to the log files (this will use a rolling log
# system).
ExecStart=/usr/local/bin/tccon-priors-service --file-level=DEBUG --log-file=/var/log/auto-mod-rust.log

# In principle, this should make a "reload" command reload the configuration and a "stop" command
# shut things down gracefully.
ExecReload=/bin/kill -HUP $MAINPID
ExecStop=/bin/kill -USR2 $MAINPID

# Ensure that the DATABASE_URL has the correct username, password, host, and database name in it, and
# that PRIOR_CONFIG_FILE points to your configuration file.
Environment="DATABASE_URL=mysql://USER:PASSWORD@HOST/DATABASE" "PRIOR_CONFIG_FILE=/etc/auto-priors-config.toml"

# Make sure that we always try to keep this service running.
Restart=always

[Install]
WantedBy=default.target
```

```admonish info
Make sure that you use a version of `tccon-priors-service` compiled in release mode, i.e. with `cargo build --release`,
to maximize performance.
```

Once this file is in place, and assuming it is named `automodrust.service`, then the following commands should
suffice to have it start automatically, even if your server reboots.

```
# Usually needed when you add a new system file for systemd to become aware of it
systemctl daemon-reload

# Tell systemd it should start this service automatically
systemctl enable automodrust.service

# Start the service running this first time
systemctl start automodrust.service
```

Now, running `systemctl status automodrust.service` should show that the service is running.

### Running the service as a non-root user

It is possible to run a `systemd` service as a regular user.
To do so, you put the `.service` file under `~/.config/systemd/user`, then run the same `systemctl` commands as above with the `--user` flag added.
However, we have found that this does not always start the service automatically when the server reboots.
The issue seems to occur if your user's home directory is on a drive that is not mounted when `systemd` tries to start its child processes.
To get around this, we use a "kickstart" script that runs every hour via a cron job:

```bash
#!/bin/bash

status=$(systemctl --user is-active automodrust.service)
if [[ $status == "inactive" ]]; then
  # So we need to do daemon-reload first to find the user unit...
  systemctl --user daemon-reload
  # ...then we can start it...
  systemctl --user start automodrust.service
  # ...finally email me so I know it started successfully
  isok=$(systemctl --user is-active automodrust.service)
  echo "Kickstarted automodrust, new automodrust status = ${isok}" | mail -s 'AutoModRust: kickstart' you@test.com
elif [[ $status != "active" ]]; then
  echo "automodrust status = '$status', this is an unexpected status, please address" | mail -s 'AutoModRust: unknown status' you@test.com
fi
```

As a double check, we also run a cron job that uses the following script to email us the automation's status each morning:

```bash
#!/bin/bash
# Email me so I know if the priors service is running
isok=$(systemctl --user is-active automodrust.service)
echo "Automodrust status as of $(date) = ${isok}" | mail -s 'AutoModRust status' you@test.com
```

