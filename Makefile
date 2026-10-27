.PHONY: contracts contracts-check

contracts:
	cargo run --locked --bin ocg-rs-ts

contracts-check:
	cargo run --locked --bin ocg-rs-ts -- --check
