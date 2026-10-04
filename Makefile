.PHONY: fmt test test-managed lint build check generate release-fetch

# Format everything (prettier).
fmt:
	npx prettier --write .

# The chronicle release the conformance suite comes from (package.json chronicle.conformance).
release-fetch:
	node scripts/fetch-release.mjs

# Regenerate src/generated/contract.ts from contract/sdk-contract.json.
generate:
	node scripts/generate.mjs

# Unit tests and the conformance suite against a real `chronicle up` - no mocked NATS.
test: release-fetch
	npx vitest run

# eslint (type-checked) and tsc, and formatting checked.
# The bridge against the managed fleet (chronicle-service, private): CHRONICLE_MANAGED_BIN=path/to/chronicle.
test-managed:
	npx vitest run --config vitest.managed.config.ts

lint:
	npx eslint .
	npx tsc -p tsconfig.json --noEmit
	npx prettier --check .

build:
	npx tsc -p tsconfig.build.json

# The one gate to run before every commit.
check: fmt lint test build
