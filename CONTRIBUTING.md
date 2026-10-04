# Contributing

`chronicle-js` is Apache-2.0 and accepts contributions under the
[Developer Certificate of Origin](https://developercertificate.org/): sign
off every commit (`git commit -s`), which adds

```
Signed-off-by: Your Name <you@example.com>
```

CI refuses a pull request with a commit that lacks the trailer.

## The gate

```sh
make check   # prettier, eslint (type-checked) + tsc, the tests, the build
```

`make test` fetches the chronicle release the SDK declares conformance
against (`package.json` → `chronicle.conformance`) and runs the
conformance suite against its real `chronicle up`, over the websocket.
Mocking the NATS client is not accepted.

`src/generated/` is generated from `contract/sdk-contract.json` by
`npm run generate` — never edit it by hand. The README's support matrix is
rendered by `scripts/matrix.mjs` from the suite's result.

## Where things are decided

The contract this SDK implements, and every rule behind it, is decided in
chronicle's design records (design 12, the SDK contract) and implemented
first in the Go client of [`chronicle`](https://github.com/impire-io/chronicle).
A change to the wire belongs there; this repository follows the contract
version it declares.
