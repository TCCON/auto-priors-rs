cargo-build:
	cargo build --release

sqlx-offline:
	cargo sqlx prepare --workspace -- --all-targets

test:
	SQLX_OFFLINE=true cargo test --features=container-tests -- --test-threads=1 $(TEST_LIST)
