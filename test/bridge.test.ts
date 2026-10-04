// The bridge's pure rules and the profile fetch; whoami against a real
// quick start. The callout itself lives in the managed service: its live
// path is test/managed/bridge.managed.ts, run with `make test-managed`.
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  bridgeToken,
  fetchInstallProfile,
  identityAccountCreateSubject,
  identityFromSubject,
  identityMembershipsSubject,
  principalFromCreds,
  whoami,
} from "../src/index.js";
import { up, type Up } from "./conformance/up.js";

describe("the bridge's wire", () => {
  it("the CONNECT token is selector:token", () => {
    expect(bridgeToken("acme", "gho_x")).toBe("acme:gho_x");
    expect(bridgeToken("+", "gho_x")).toBe("+:gho_x");
  });

  it("identity-plane subjects round-trip through the placement's permissions", () => {
    const s = identityMembershipsSubject(4242, "erin");
    expect(s).toBe("CHRON.CTRL.IDENTITY.MEMBERSHIPS.4242.erin");
    expect(identityAccountCreateSubject(4242, "erin")).toBe("CHRON.CTRL.IDENTITY.ACCOUNT.CREATE.4242.erin");
    expect(identityFromSubject(s)).toEqual({ githubId: 4242, login: "erin" });
    expect(identityFromSubject("CHRON.API.LOG.CREATE")).toBeUndefined();
    expect(identityFromSubject("CHRON.CTRL.IDENTITY.MEMBERSHIPS.x.erin")).toBeUndefined();
    expect(identityFromSubject("CHRON.CTRL.IDENTITY.MEMBERSHIPS.0.erin")).toBeUndefined();
    expect(identityFromSubject("CHRON.CTRL.IDENTITY.MEMBERSHIPS.4242.Erin")).toBeUndefined();
  });
});

describe("principalFromCreds", () => {
  const jwt = (claims: object) =>
    [
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ",
      Buffer.from(JSON.stringify(claims)).toString("base64url"),
      "sig",
    ].join(".");
  const creds = (token: string) =>
    `-----BEGIN NATS USER JWT-----\n${token}\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\nSUAX\n------END USER NKEY SEED------\n`;

  it("reads the user JWT's name", () => {
    expect(principalFromCreds(creds(jwt({ name: "dana", sub: "UABC" })))).toBe("dana");
  });
  it("refuses creds whose JWT names no principal", () => {
    expect(() => principalFromCreds(creds(jwt({ sub: "UABC" })))).toThrow(/names no principal/);
    expect(() => principalFromCreds("not a creds file")).toThrow(/no user JWT/);
  });
});

describe("fetchInstallProfile", () => {
  let server: Server;
  let base: string;
  beforeAll(async () => {
    server = createServer((req, res) => {
      if (req.url === "/.well-known/chronicle/profile.json") {
        res.setHeader("Content-Type", "application/json");
        res.end(
          JSON.stringify({
            url: "tls://connect.example:4222",
            websocket_url: "wss://connect.example",
            github_client_id: "Iv1.x",
            sentinel: "-----BEGIN NATS USER JWT-----",
          }),
        );
        return;
      }
      if (req.url === "/partial.json") {
        res.end(JSON.stringify({ url: "tls://x" }));
        return;
      }
      res.statusCode = 404;
      res.end();
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });
  afterAll(() => {
    server.close();
  });

  it("reads the profile, websocket url included", async () => {
    const p = await fetchInstallProfile(`${base}/.well-known/chronicle/profile.json`);
    expect(p.websocket_url).toBe("wss://connect.example");
    expect(p.github_client_id).toBe("Iv1.x");
  });
  it("refuses a missing or partial profile", async () => {
    await expect(fetchInstallProfile(`${base}/nope.json`)).rejects.toThrow(/404/);
    await expect(fetchInstallProfile(`${base}/partial.json`)).rejects.toThrow(/missing/);
  });
});

describe("whoami", () => {
  let u: Up;
  beforeAll(async () => {
    u = await up();
  });
  afterAll(async () => {
    await u.stop();
  });

  it("refuses a placement the server names only by its nkey", async () => {
    // The quick start's user is an nkey with no name: whoami must not
    // mistake the key for a principal.
    await expect(whoami(u.client.connection)).rejects.toThrow(/no principal name/);
  });
});
