// store create's options reach the wire as the contract names them. The
// quick start's node answers the request; an observer beside it sees what
// was asked (the declared release predates schedules, so the node's reply
// cannot prove the flag — the request can).
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { up, type Up } from "./conformance/up.js";

describe("store create's wire", () => {
  let u: Up;
  beforeAll(async () => {
    u = await up();
  });
  afterAll(async () => {
    await u.stop();
  });

  it("carries schedules when asked, and not otherwise (decision 0050)", async () => {
    const nc = u.client.connection;
    const seen: Record<string, unknown>[] = [];
    const sub = nc.subscribe("CHRON.API.LOG.CREATE", {
      callback: (_err, msg) => {
        seen.push(JSON.parse(new TextDecoder().decode(msg.data)) as Record<string, unknown>);
      },
    });
    try {
      await u.client.createStore("ticks", { schedules: true, description: "the tick store" });
      await u.client.createStore("rooms");
    } finally {
      sub.unsubscribe();
    }
    const ticks = seen.find((b) => b["store"] === "ticks");
    const rooms = seen.find((b) => b["store"] === "rooms");
    expect(ticks).toMatchObject({ store: "ticks", schedules: true, description: "the tick store" });
    expect(rooms).toBeDefined();
    expect(rooms).not.toHaveProperty("schedules");
  });
});
