// The bridge, live: the managed `chronicle up` (chronicle-service, private)
// with its websocket listener and a fake GitHub, and the console's first
// session through this SDK — a GitHub identity that has never seen the
// install is placed in the identity plane over the websocket, creates its
// account, is placed there and writes; a token GitHub does not know is
// refused. The managed binary is not public, so this runs where one is
// built: `CHRONICLE_MANAGED_BIN=… make test-managed`.
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { connectBridge, connectIdentity, type InstallProfile } from "../../src/index.js";

const bin = process.env.CHRONICLE_MANAGED_BIN ?? "";

describe("the bridge over the websocket, against the managed fleet", () => {
  let github: Server;
  let child: ChildProcess;
  let dir: string;
  let profile: InstallProfile;

  beforeAll(async () => {
    if (bin === "") {
      throw new Error("CHRONICLE_MANAGED_BIN names no managed chronicle binary");
    }
    github = createServer((req, res) => {
      if (req.url === "/user" && req.headers.authorization === "Bearer tok-erin") {
        res.end(JSON.stringify({ id: 4242, login: "Erin" }));
        return;
      }
      res.statusCode = 401;
      res.end();
    });
    await new Promise<void>((resolve) => github.listen(0, "127.0.0.1", resolve));
    const gh = `http://127.0.0.1:${(github.address() as AddressInfo).port}`;

    dir = mkdtempSync(join(tmpdir(), "chronicle-js-managed-"));
    child = spawn(
      bin,
      [
        "up",
        "--dir",
        dir,
        "--port",
        "-1",
        "--websocket-port",
        "-1",
        "--github-client-id",
        "Iv1.test",
        "--github-api-base",
        gh,
        "--github-oauth-base",
        gh,
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    let output = "";
    child.stdout?.on("data", (d: Buffer) => (output += d.toString()));
    child.stderr?.on("data", (d: Buffer) => (output += d.toString()));
    const file = join(dir, "bridge.json");
    const deadline = Date.now() + 90_000;
    while (!existsSync(file) || !output.includes("ws:")) {
      if (child.exitCode !== null || Date.now() > deadline) {
        throw new Error(`managed chronicle up did not come up:\n${output}`);
      }
      await sleep(200);
    }
    profile = JSON.parse(readFileSync(file, "utf8")) as InstallProfile;
  });

  afterAll(async () => {
    child.kill("SIGTERM");
    await Promise.race([new Promise((resolve) => child.once("exit", resolve)), sleep(15_000)]);
    github.close();
    rmSync(dir, { recursive: true, force: true });
  });

  it("the console's first session", async () => {
    const servers = profile.websocket_url ?? "";
    expect(servers).toMatch(/^ws:\/\/127\.0\.0\.1:/);

    const id = await connectIdentity({ servers, sentinel: profile.sentinel, githubToken: "tok-erin" });
    try {
      expect(id.githubId).toBe(4242);
      expect(id.login).toBe("erin");
      expect((await id.memberships()).memberships).toEqual([]);
      const created = await id.createAccount();
      expect(created.name).toBe("erin");

      const c = await connectBridge({
        servers,
        sentinel: profile.sentinel,
        githubToken: () => "tok-erin",
        account: created.name,
      });
      try {
        expect(c.author).toBe(created.admin);
        await c.createLog("panel", { description: "written from a browser's transport" });
        const logs: string[] = [];
        for await (const log of c.listLogs()) logs.push(log);
        expect(logs).toContain("panel");
      } finally {
        await c.close();
      }
      expect((await id.memberships()).memberships.map((m) => m.account)).toEqual(["erin"]);
    } finally {
      await id.close();
    }

    await expect(
      connectBridge({ servers, sentinel: profile.sentinel, githubToken: "tok-nobody", account: "erin" }),
    ).rejects.toThrow();
  });
});
