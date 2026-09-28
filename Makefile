# OCG
#
# `make check` runs the formatting, lint, and contract gates.
# `make validate` validates the shipped configuration.
# `make package` builds the current platform artifact and SHA256SUMS locally.
# `make contracts` regenerates the TypeScript projection of the Rust wire
# contract; `make contracts-check` fails when it is stale.

CARGO ?= cargo

.PHONY: all build check fmt fmt-check clippy \
	validate package clean contracts contracts-check

all: check

build:
	$(CARGO) build

check: fmt-check contracts-check clippy

# Rust is the single source of truth for the loopback control wire contract;
# this projects it into the TypeScript PWA.
contracts:
	$(CARGO) run --quiet --bin ocg-rs-ts

contracts-check:
	$(CARGO) run --quiet --bin ocg-rs-ts -- --check

fmt:
	$(CARGO) fmt

fmt-check:
	$(CARGO) fmt --check

clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

validate:
	$(CARGO) run --quiet -- validate

package:
	sh scripts/package-release.sh

clean:
	$(CARGO) clean
