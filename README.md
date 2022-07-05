# Automatic TCCON priors

Now rewritten in Rust!

## Installation

Clone this repo with `git clone --recursive`. The [Rocket](rocket.rs) dependency is currently checked out as a submodule
rather than managed through crates.io. 

To build, the MySQL database must be specified as the `DATABASE_URL` environmental variable, e.g.:

```
DATABASE_URL="mysql://user:password@host/tccon_priors"
```

The database name at the end *must* be `tccon_priors`. This variable can be stored in a `.env` file for development,
but will probably need defined in the shell for deployment.

