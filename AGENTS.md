# Agent brief — chronicle-js

The TypeScript SDK for chronicle (`@impire-io/chronicle`). Orientation lives
in `../chronicle-hq`: decisions 0036 (the SDK contract) and 0040 (license
and npm name), design 12 (the contract), design 13 (the panel, this SDK's
first consumer). The Go `client` package of `../chronicle` is the reference
implementation; port its behavior, never invent wire.

Non-negotiables:

- **The quality gate before "done"**: `make check` — prettier, type-checked
  eslint and tsc, every test, the build. All green, nothing skipped.
- **Tested against real NATS**: the conformance suite runs against the
  declared chronicle release's `chronicle up` over its websocket; the bridge
  runs against the managed fleet with `make test-managed`. No mocked NATS.
- **Generated code is generated**: `src/generated/` comes from
  `contract/sdk-contract.json` (`npm run generate`); the support matrix in
  the README comes from `scripts/matrix.mjs`.
- **nats.js v3 (`@nats-io/*`) only** — never the deprecated `nats.ws`.
- **Commits are signed and signed off** (`git commit -S -s`, DCO);
  `.claude/settings.local.json` is never committed.
- **Work follows playbook 07** (`../chronicle-hq/00-META/process/07-parallel-work.md`):
  one work ID as branch, workspace and PR label; draft PR from the first
  push; marked ready and merged by whoever did the work once the gate is
  green and every predecessor has merged, in `lands` order.
