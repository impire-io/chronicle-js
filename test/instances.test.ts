// A listing under a parent names each child by its type, the one the
// parent's children map declares for the child's name — what the
// contract's list.instances item calls the type its path names — and by
// the name itself where the parent is untyped. The type option still
// narrows to the children declared as it. The quick start's node folds
// the state the listing reads; nothing is mocked. A name the parent's
// type does not declare falls back the same way, but never reaches the
// state: the node marks such an op undeclared and moves nothing, and a
// redefinition of the parent rebuilds the state under what it declares.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { up, type Up } from "./conformance/up.js";

const STORE = "invoices";

async function listed(u: Up, opts: Parameters<Up["client"]["listInstances"]>[1]) {
  const items: { path: string; type: string }[] = [];
  for await (const i of u.client.listInstances(STORE, opts)) {
    items.push({ path: i.path, type: i.type });
  }
  return items.sort((a, b) => a.path.localeCompare(b.path));
}

describe("listInstances under a parent", () => {
  let u: Up;
  beforeAll(async () => {
    u = await up();
    const c = u.client;
    await c.createStore(STORE);
    await c.defineType(STORE, "comment", {
      schema: {},
      operations: { create: { schema: {}, effect: "merge" } },
    });
    await c.defineType(STORE, "attachment", {
      schema: {},
      operations: { create: { schema: {}, effect: "merge" } },
    });
    await c.defineType(STORE, "invoice", {
      schema: {},
      children: { comments: "comment", attachments: "attachment" },
      operations: { create: { schema: {}, effect: "merge" } },
    });
    await c.create(STORE, "invoice/inv-1", "create", { total: 10 });
    await c.create(STORE, "invoice/inv-1/comments/c-1", "create", { body: "first" });
    await c.create(STORE, "invoice/inv-1/comments/c-2", "create", { body: "second" });
    await c.create(STORE, "invoice/inv-1/attachments/a-1", "create", { name: "scan.pdf" });
    // An untyped parent, and its children, exist by snapshot alone.
    await c.createFromSnapshot(STORE, "note/n-1", { body: "loose" });
    await c.createFromSnapshot(STORE, "note/n-1/replies/r-1", { body: "loose reply" });
  });
  afterAll(async () => {
    await u.stop();
  });

  it("names a child by the type the parent's children map declares, not by its segment", async () => {
    expect(await listed(u, { under: "invoice/inv-1" })).toEqual([
      { path: "invoice/inv-1/attachments/a-1", type: "attachment" },
      { path: "invoice/inv-1/comments/c-1", type: "comment" },
      { path: "invoice/inv-1/comments/c-2", type: "comment" },
    ]);
  });

  it("falls back to the segment name under an untyped parent", async () => {
    expect(await listed(u, { under: "note/n-1" })).toEqual([
      { path: "note/n-1/replies/r-1", type: "replies" },
    ]);
  });

  it("still narrows to the children declared as the type asked for", async () => {
    expect(await listed(u, { under: "invoice/inv-1", type: "comment" })).toEqual([
      { path: "invoice/inv-1/comments/c-1", type: "comment" },
      { path: "invoice/inv-1/comments/c-2", type: "comment" },
    ]);
    expect(await listed(u, { under: "invoice/inv-1", type: "attachment" })).toEqual([
      { path: "invoice/inv-1/attachments/a-1", type: "attachment" },
    ]);
    expect(await listed(u, { under: "invoice/inv-1", type: "invoice" })).toEqual([]);
  });

  it("names top-level instances by their type, as before", async () => {
    expect(await listed(u, { type: "invoice" })).toEqual([{ path: "invoice/inv-1", type: "invoice" }]);
  });
});
