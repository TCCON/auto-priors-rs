# Running tests locally

## SQLx prepared queries

In order to compile without a database active, SQLx must prepare its queries.
You can check if the queries are up-to-date by using `cargo sqlx prepare --workspace --check -- --all-targets` in the workspace root.
(The `-- --all-targets` is necessary to tell cargo to include the test queries _and_ regular queries.
It seems that the prepared queries are actually for each `query!` or `query_as!` macro in the program,
not a summarization of the database.)

### Example for Mac

1. Start database with `mariadbd` (note the extra "d" at the end)
2. Run `cargo sqlx prepare --workspace -- --all-targets`
3. Stop `mariadbd`. (Ctrl+C doesn't seem to work, so I just `pkill mariadbd` in another shell.)

## Running tests with Podman

Start podman with `podman machine start`.
It should provide a `DOCKER_HOST` variable to use - copy or export that, as it's difficult to get after the fact.


Ensure that podman is running by confirming that `podman info` produces output.
If not, `podman machine start` should handle that.
Although the test code should default to using the test containers, running local tests without mariadb running is safest.

```
SQLX_OFFLINE=true cargo test -- --test-threads=1
```

If you haven't exported `DOCKER_HOST` as given in the `podman machine start` output, include it as a one-off environmental variable here just like `SQLX_OFFLINE`.
`SQLX_OFFLINE=true` is needed to tell SQLx to ignore the `DATABASE_URL` variable in a `.env` file in the workspace root, and use the prepared queries from step 1.
The `--test-threads=1` option after the `--` seems to be necessary to avoid tests failing because containers failed to start quickly enough.
It's likely that this value can be greater than one; for a set of 3 tests, they ran successfully without the thread limit while a set of 26 did not.
However, the more threads the longer it seems to take for Podman to start the containers, so there may not be much advantage to increasing that above 1.

This will take time, so likely it is best to run all tests once to find the failing ones, then try individual tests after trying to fix them.

### Alternate approach

Rust's `testcontainers` package is meant to work with docker, not podman, so to get it to use podman requires a workaround described [here](https://medium.com/twodigits/testcontainers-on-podman-a090c348b9d8).
The first step is to get connection information with `podman system connection list`.
This should print a table like:

```text
Name                         URI                                                         Identity                                                        Default     ReadWrite
podman-machine-default       ssh://core@127.0.0.1:55151/run/user/503/podman/podman.sock  /Users/laughner/.local/share/containers/podman/machine/machine  false       true
podman-machine-default-root  ssh://root@127.0.0.1:55151/run/podman/podman.sock           /Users/laughner/.local/share/containers/podman/machine/machine  true        true
```

We're interested in the line with "core" as the SSH user.
Note the file under identity, the port in the URI, and the path after the port in the URI, then run the following SSH command:

```bash
ssh -i IDENTITY_FILE -p PORT -N core@127.0.0.1 -L'/tmp/podman.sock:PATH'
```

for example, given the table above:

```bash
ssh -i /Users/laughner/.local/share/containers/podman/machine/machine -p 55151 -N core@127.0.0.1 -L'/tmp/podman.sock:/run/user/503/podman/podman.sock'
```

Then, in the workspace root, run the tests like so:

```
DOCKER_HOST=unix:///tmp/podman.sock SQLX_OFFLINE=true cargo test -- --test-threads=1
```

The `DOCKER_HOST` variable gets set to the socket our SSH tunnel created on our side, under `/tmp`.
`SQLX_OFFLINE=true` is needed to tell SQLx to ignore the `DATABASE_URL` variable in a `.env` file in the workspace root, and use the prepared queries from step 1.
The `--test-threads=1` option after the `--` seems to be necessary to avoid tests failing because containers failed to start quickly enough.
It's likely that this value can be greater than one; for a set of 3 tests, they ran successfully without the thread limit while a set of 26 did not.
However, the more threads the longer it seems to take for Podman to start the containers, so there may not be much advantage to increasing that above 1.

This will take time, so likely it is best to run all tests once to find the failing ones, then try individual tests after trying to fix them.
