# chronicle-js

The TypeScript SDK for [chronicle](https://github.com/impire-io/chronicle):
`@impire-io/chronicle`. It speaks chronicle's SDK contract over
[nats.js](https://github.com/nats-io/nats.js), in the browser through a
websocket and in Node.

```sh
npm install @impire-io/chronicle
```

## Support

An SDK supports chronicle when it passes the conformance suite of the
contract version it declares (chronicle design 12). This table is rendered
from the suite's last result, never edited by hand:

<!-- support-matrix:begin (rendered by scripts/matrix.mjs — do not edit) -->

| SDK                          | Contract | Suite (chronicle release) | Fixtures     | Scenarios   | Supports |
| ---------------------------- | -------- | ------------------------- | ------------ | ----------- | -------- |
| `@impire-io/chronicle` 0.1.0 | 1.0.0    | v0.2.4                    | ✅ 75 passed | ✅ 2 passed | yes      |

<!-- support-matrix:end -->

## Connecting

To a local `chronicle up --websocket-port 9222`, as its one user:

```ts
import { Client } from "@impire-io/chronicle";
import { readFileSync } from "node:fs";

const dir = `${process.env.HOME}/.chronicle/dev`;
const c = await Client.connect({
  servers: readFileSync(`${dir}/websocket.url`, "utf8"),
  nkeySeed: readFileSync(`${dir}/user.nk`, "utf8"),
  author: "admin",
});
```

With a creds file the principal is the creds' name:
`Client.connect({ servers, creds })`. Any other transport — a Node TCP
connection from `@nats-io/transport-node` — is adopted with
`Client.wrap(nc, principal)`.

To the hosted platform at chronicle.impire.dev, through the GitHub identity
bridge — the install profile names the websocket and the sentinel:

```ts
import { connectBridge, connectIdentity, fetchInstallProfile } from "@impire-io/chronicle";

const profile = await fetchInstallProfile();
const auth = { servers: profile.websocket_url!, sentinel: profile.sentinel, githubToken };

const id = await connectIdentity(auth); // the identity plane
const { memberships } = await id.memberships();
const account = memberships[0]?.account ?? (await id.createAccount()).name;
await id.close();

const c = await connectBridge({ ...auth, account });
```

## The sentences

Collections are async iterables; single values are replies
(design 12 § the surface rule).

```ts
await c.createLog("orders");
await c.defineType("orders", "invoice", {
  schema: { type: "object", required: ["total"] },
  operations: {
    create: { schema: { type: "object" }, effect: "merge" },
    send: { schema: { type: "object" }, effect: "merge" },
  },
});

await c.createWith("orders", "invoice.inv-1", "create", { total: 120 });
const { seq, state } = await c.state("orders", "invoice.inv-1");
await c.append("orders", "invoice.inv-1", "send", { to: "x" }, { expectedSeq: seq }); // guarded

for await (const op of c.replay("orders", "invoice.inv-1")) console.log(op.seq, op.type);

const ctl = new AbortController();
for await (const sv of c.watch("orders", "invoice.inv-1", { signal: ctl.signal })) render(sv.state);

await c.declareIndex("orders", "text", "search");
const hits = c.queryIndex("orders", "text", "widgets");
for await (const hit of hits) console.log(hit.thing, hit.score);
console.log(hits.trailer?.total);
```

Writes are checked before they are sent: a payload that fails its
operation's schema is a `SchemaViolationError`, an operation the type does
not define an `UndefinedOperationError`. A guard that finds the thing moved
is a `ThingMovedError`; a service's refusal is a `ServiceError` carrying a
code from the contract's error catalog.

## Developing

`make check` is the gate — see [CONTRIBUTING.md](CONTRIBUTING.md). Licensed
under the [Apache License 2.0](LICENSE).
