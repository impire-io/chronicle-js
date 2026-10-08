# chronicle-js

The TypeScript SDK for [chronicle](https://github.com/impire-io/chronicle):
`@impire-io/chronicle`. It speaks chronicle's SDK contract over
[nats.js](https://github.com/nats-io/nats.js), in the browser through a
websocket and in Node, in the words of chronicle's
[vocabulary](https://github.com/impire-io/chronicle-hq/blob/main/00-META/vocabulary.md):
a **store** holds **types**; you create **instances** of a type, named by a
**path** (`type/id`, then `name/id` for each **child**); every change is an
**operation** applied to an instance and kept in its **history**; the
current **state** is one read away; the **indexes** you declare are kept
current from the history.

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
| `@impire-io/chronicle` 0.2.0 | 2.0.0    | v0.4.0                    | ✅ 83 passed | ✅ 2 passed | yes      |

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

With a credential file the principal is the file's name:
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
(design 12 § the surface rule). Instances are named by path.

```ts
await c.createStore("orders");
await c.defineType("orders", "invoice", {
  schema: { type: "object", required: ["total"] },
  children: { comments: "comment" },
  operations: {
    create: { schema: { type: "object" }, effect: "merge" },
    send: { schema: { type: "object", required: ["to"] }, effect: "merge" },
    note: { schema: { type: "object" }, effect: "none" },
  },
});

await c.create("orders", "invoice/inv-1", "create", { total: 120 });
const { seq, state } = await c.state("orders", "invoice/inv-1");
await c.apply("orders", "invoice/inv-1", "send", { to: "x" }, { expectedSeq: seq });

for await (const op of c.history("orders", "invoice/inv-1")) console.log(op.seq, op.type);
for await (const i of c.listInstances("orders", { type: "invoice", where: { to: "x" } }))
  console.log(i.path, i.state);
for await (const i of c.listInstances("orders", { under: "invoice/inv-1" })) console.log(i.path); // its children

const ctl = new AbortController();
for await (const sv of c.watch("orders", "invoice/inv-1", { signal: ctl.signal })) render(sv.state);

await c.snapshot("orders", "invoice/inv-1"); // the current state as one entry; the history before it compacted

await c.declareIndex("orders", "text", "search");
const hits = c.queryIndex("orders", "text", "widgets");
for await (const hit of hits) console.log(hit.instance, hit.score);
console.log(hits.trailer?.total);

await c.addMember("jordan", "reader", { githubId: 4242 });
await c.addServiceAccount("billing-svc", "writer");
```

Writes are checked before they are sent: data that fails its operation's
schema is a `SchemaViolationError`, an operation the type does not define
an `UndefinedOperationError`, a child the parent type does not declare an
`UndeclaredChildError`. A write whose expected sequence the instance moved
past is an `InstanceMovedError`; a create of an instance that exists is an
`InstanceExistsError`; a service's refusal is a `ServiceError` carrying a
code from the contract's error catalog.

## Developing

`make check` is the gate — see [CONTRIBUTING.md](CONTRIBUTING.md). Licensed
under the [Apache License 2.0](LICENSE).
