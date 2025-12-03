# Automatic TCCON priors

Now rewritten in Rust!

## Notice

Copyright 2025, by the California Institute of Technology. ALL RIGHTS RESERVED.
United States Government Sponsorship acknowledged.
Any commercial use must be negotiated with the Office of Technology Transfer at the California Institute of Technology.
 
This software may be subject to U.S. export control laws.
By accepting this software, the user agrees to comply with all applicable U.S. export laws and regulations.
User has the responsibility to obtain export licenses, or other export authority as may be required before exporting such information to foreign countries or providing access to foreign persons.

## Setting up a development copy

1. Ensure you have a Rust toolchain with minimum version 1.71.1 installed:
    - Run `rustup show` from a terminal. If this prints something like:

    ```
    Default host: x86_64-unknown-linux-gnu
    rustup home:  /home/tccon/.rustup

    stable-x86_64-unknown-linux-gnu (default)
    rustc 1.71.1 (eb26296b5 2023-08-03)
    ```

    then you should be set. If you get any message to the effect of "no program `rustup` found", then follow the instructions at https://rustup.rs/ to install a toolchain.

    - If the `rustc` version shown is less than 1.71.1, use `rustup update` to update to a more recent version.
2. Ensure you have a MySQL server installed. MariaDB is the open source implementation of MySQL. On Mac, this can be installed with [Homebrew](https://brew.sh/) with the command `brew install mariadb`. On Linux systems, it should be available via the package manager.
    - Note that depending on how it is configured, the server may or may not be started automatically on boot. Typically, Linux distros will start it automatically, but Mac will not. To start the server manually, use the command `mysql.server start`.
3. Set up your database. We will assume you use 'tccon' for the database user and 'priors' for the database name. You can change either one if you wish, just replace the corresponding text in the commands below. This also assumes that you have root or admin control of the database; if not, you'll probably need to contact whoever does.
    - Enter the MySQL prompt with `mysql` at the terminal. (Linux users may need to do `sudo mysql` to access it as root.)
    - Create the 'tccon' user with `CREATE USER 'tccon'@'localhost' IDENTIFIED BY '*****';` - replace the `*`s with a real password.
    - Grant all permissions to the 'tccon' user on the 'priors' database with `GRANT ALL PRIVILEGES ON 'priors'.* TO 'tccon'@'localhost';`. If you get an error about the 'priors' database not existing, create it first with `CREATE DATABASE 'priors';`
    - To work with the web app, this assumes that you have the Django site status/metadata portal database on the same server.
      Grant the same user read access to those databases with ``GRANT SELECT ON `djopstat`.* TO `tccon`@`localhost` ``.
4. Ensure you have the `sqlx-cli` Cargo extension installed and set up to work with MySQL.
    - Run `cargo sqlx --help`, if that produces an error, you need to install it
    - To install, run `cargo install sqlx-cli --features native-tls,mysql`. (This will install it with support for only MySQL databases, which avoids errors from SqLite or Postgress libraries not being installed. See the [sqlx-cli docs](https://crates.io/crates/sqlx-cli) if you want to include support for other database types.)
5. Clone this repo (`TCCON/auto-priors-rs`) to your computer.
6. Create a `.env` file with the following contents in the root of the cloned repo (i.e. in the same directory as this README). Replace the `*`s with the database password created in step 3, and if you created a different user or database name, replace 'tccon' with the user and 'priors' with the database name:

```
DATABASE_URL="mysql://tccon:****@localhost/priors"
```

7. Initialize the database. From the repo root, run `cargo sqlx database create` then `cargo sqlx migrate run --source core-orm/migrations/`.
    - The `database create` command may fail if the user specified in the `DATABASE_URL` does not have database creation privileges.
    - In that case, you can create the database manually from within `mysql` with `CREATE DATABASE priors` using a root or administrator account.
8. Compile the project; from the repo root, run `cargo build`. If successful, the `tccon-priors-cli` and `tccon-priors-service` executables will be produced in `./target/debug`.
    - Note, for deployment, you should use `cargo build --release` and find the executables under `./target/release` instead, as their will have much better optimization.
9. Create a default configuration file. Assuming you want the file written to `auto-priors.toml`, the command to run from the repo root is `./target/debug/tccon-priors-cli config gen auto-priors.toml`. Modify this file as needed.
10. Add the path to your new config file to the `.env` file. Assuming the config file created in step 9 is at the path `/home/tccon/auto-priors-rs/auto-priors.toml`, add the following line to the `.env` file:

```
PRIOR_CONFIG_FILE=/home/tccon/auto-priors-rs/auto-priors.toml
```

11. (optional) If you want to copy the values from an existing database, then:
    - On the computer running the database to copy, call `tccon-priors-cli db export priors-db.json` (note that you can replace `priors-db.json` with any valid file path, but it will be overwritten).
    - Copy the JSON file produced to your computer.
    - On your computer, run `tccon-priors-cli db import priors-db.json` (assuming the JSON file is named `priors-db.json`).
    - This process will be slower than using `mysqldump`; however, it avoids issues with subtly different MySQL configurations.

## Further documention

More documentation is available in the `book` subdirectory.
You can browse the markdown in `book/src` directly, or use `mdbook` to render it.
To render it:

1. Install [mdbook](https://crates.io/crates/mdbook)
2. Install [mdbook-admonish](https://crates.io/crates/mdbook-admonish)
3. In the `book` subdirectory, run `mdbook serve`. It will provide a localhost link where you can view the rendered book.

Unfortunately, GitHub pages requires that a repository be made public or be hosted by a GitHub Enterprise account.
Until/unless we make this repository public, the best way to view the book is locally.

## Notes during development

This project uses [sqlx](https://crates.io/crates/sqlx) to interact with the database.
Most of the queries are checked at compile time with its `query!` and `query_as!` macros.
This requires that the MySQL server be running on your computer, and that the database specified in the `.env` file's `DATABASE_URL` variable exist with the proper tables.
The `cargo sqlx` commands in step 7 of the setup will take care of the second requirement.
If your MySQL server doesn't start automatically, then you will get many errors about "unable to connect to database" when compiling or in your code editor.
If that happens, just start the MySQL server.
For Macs, the command is probably either `mysql.server start` or `mariadbd`.
For other systems, consult the documentation for your MySQL server.

