// store create's options reach the wire as the contract names them. The
// quick start's node answers the request; an observer beside it — the
// node's own user, since a member may not subscribe to the verbs
// (decision 0057) — sees what was asked, on the member's own subject.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { nodeConnection, up, type Up } from "./conformance/up.js";

describe("store create's wire", () => {
  let u: Up;
  beforeAll(async () => {
    u = await up();
  });
  afterAll(async () => {
    await u.stop();
  });

  it("carries schedules and verified authors when asked, on the caller's subject (decisions 0050, 0057)", async () => {
    const nc = await nodeConnection(u);
    const seen: Record<string, unknown>[] = [];
    const sub = nc.subscribe("CHRON.API.*.LOG.CREATE", {
      callback: (_err, msg) => {
        seen.push({
          subject: msg.subject,
          ...(JSON.parse(new TextDecoder().decode(msg.data)) as Record<string, unknown>),
        });
      },
    });
    try {
      await u.client.createStore("ticks", { schedules: true, description: "the tick store" });
      await u.client.createStore("rooms", { verifiedAuthors: true });
      await u.client.createStore("orders");
    } finally {
      sub.unsubscribe();
      await nc.close();
    }
    const ticks = seen.find((b) => b["store"] === "ticks");
    const rooms = seen.find((b) => b["store"] === "rooms");
    const orders = seen.find((b) => b["store"] === "orders");
    expect(ticks).toMatchObject({
      subject: "CHRON.API.admin.LOG.CREATE",
      store: "ticks",
      schedules: true,
      description: "the tick store",
    });
    expect(rooms).toMatchObject({ store: "rooms", verified_authors: true });
    expect(orders).toBeDefined();
    expect(orders).not.toHaveProperty("schedules");
    expect(orders).not.toHaveProperty("verified_authors");
    expect(orders).not.toHaveProperty("principal");
  });
});
