# OCG

OCG is a project-agnostic orchestration Core with a PWA control surface.
The Core owns durable contracts, missions, execution state, verification, and
the loopback control API. The PWA presents that state and sends control-plane
commands through the Rust-owned wire contract.
