# Component crates of the automation

Rust calls its packages "crates".
The automation has several top levels components, each written as its own crate, and tied together in a [Cargo workspace](https://doc.rust-lang.org/book/ch14-03-cargo-workspaces.html).
The crates are:

- `core-orm`: this crate provides several core components: the interface to the database, the configuration structure, and other shared utilities.
- `cli`: this crate provides a command line interface for an admin to check on the system and make changes to the database.
- `service`: this crate compiles to a program intented to run as a systemd service to handle the routine operations of the automation.

## Interacting with the database

One of the primary purpose sof the `core-orm` crate is to serve as the interface to the database.
The "orm" part of its name stands for ["object-relational mapping"](https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping), a fancy way of referring to an interface to a database that maps tables in the database to structures in the programming language.
None of the other crates should make direct SQL calls to the database; instead, if a new type of query is needed, a new function should be added to the `core-orm` crate (which does the query) and that function should be called by the other crates.
`core-orm` should use the compile-time checked query functions from [`sqlx`](https://docs.rs/sqlx/latest/sqlx/): [`query!`](https://docs.rs/sqlx/latest/sqlx/macro.query.html) and [`query_as!`](https://docs.rs/sqlx/latest/sqlx/macro.query_as.html) whenever possible.
These ensure that all queries are valid without needing to write a test for them.

`core-orm` also stores the migration SQL files for the database.
These contain SQL commands that set up the necessary tables and views in the database.

Many of the different modules within `core-orm` map to tables in the database; for example, `met` has mappings to the table storing the met model files and `jobs` has mappings to the list of ginput jobs to run.
Within these modules, there will be one or more structures that directly map to a row from the corresponding table.
For example, the `Job` in `jobs` represents one row of the "Jobs" table, with each field on the struct mapping to a column of the table.
In some cases, we need an intermediate struct for the `sqlx` crate to deserialize the database fields into first for the `query_as!` macro - this is the case if we want to use custom types in the struct that `sqlx` doesn't know how to map to SQL types.
Such intermediate structs follow the naming convention of "Q" (for "query") + the final structure name, e.g., `QJob` for `Job`.
We then implement the [`TryFrom`](https://doc.rust-lang.org/std/convert/trait.TryFrom.html) or [`From`](https://doc.rust-lang.org/std/convert/trait.From.html) traits to convert both ways (e.g., `QJob` to `Job` and `Job` to `QJob`) as needed.

