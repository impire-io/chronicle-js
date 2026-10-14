// The bridge's pure rules and the profile fetch; whoami against a real
// quick start. The callout itself lives in the managed service: its live
// path is test/managed/bridge.managed.ts, run with `make test-managed`.
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { headers } from "@nats-io/nats-core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  bridgeToken,
  fetchInstallManifest,
  fetchInstallProfile,
  Identity,
  identityAccountCreateSubject,
  identityFromSubject,
  identityMembershipsSubject,
  identityServiceAccountIssueSubject,
  identityServiceAccountRevokeSubject,
  identityServiceAccountRotateSubject,
  principalFromCreds,
  whoami,
} from "../src/index.js";
import { nodeConnection, up, type Up } from "./conformance/up.js";

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

  it("the service-account verbs carry the admin's identity in the subject (decision 0052)", () => {
    const issue = identityServiceAccountIssueSubject(4242, "erin");
    expect(issue).toBe("CHRON.CTRL.IDENTITY.SERVICE_ACCOUNT.ISSUE.4242.erin");
    expect(identityServiceAccountRotateSubject(4242, "erin")).toBe(
      "CHRON.CTRL.IDENTITY.SERVICE_ACCOUNT.ROTATE.4242.erin",
    );
    expect(identityServiceAccountRevokeSubject(4242, "erin")).toBe(
      "CHRON.CTRL.IDENTITY.SERVICE_ACCOUNT.REVOKE.4242.erin",
    );
    expect(identityFromSubject(issue)).toEqual({ githubId: 4242, login: "erin" });
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
            solutions: [{ name: "chronicle-agents", manifest: "https://agents.example/install.json" }],
          }),
        );
        return;
      }
      if (req.url === "/partial.json") {
        res.end(JSON.stringify({ url: "tls://x" }));
        return;
      }
      if (req.url === "/install.json") {
        res.setHeader("Content-Type", "application/json");
        res.end(
          JSON.stringify({
            manifest: 1,
            solution: {
              name: "chronicle-agents",
              description: "rooms",
              version: "0.1.1",
              home: "https://github.com/impire-io/chronicle-agents",
              url: "https://agents.example",
            },
            stores: [
              {
                name: "rooms",
                description: "the rooms",
                types: [{ name: "message", definition: { schema: { type: "object" }, history: "full" } }],
              },
            ],
            announce: { store: "rooms", path: "solution/chronicle-agents" },
          }),
        );
        return;
      }
      if (req.url === "/install-old.json") {
        res.end(
          JSON.stringify({
            manifest: 0,
            solution: { name: "x" },
            stores: [],
            announce: { store: "s", path: "p" },
          }),
        );
        return;
      }
      if (req.url === "/install-partial.json") {
        res.end(JSON.stringify({ manifest: 1, solution: { name: "x" } }));
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
  it("passes the hosted solutions through, manifest address included", async () => {
    const p = await fetchInstallProfile(`${base}/.well-known/chronicle/profile.json`);
    expect(p.solutions).toEqual([
      { name: "chronicle-agents", manifest: "https://agents.example/install.json" },
    ]);
  });
  it("refuses a missing or partial profile", async () => {
    await expect(fetchInstallProfile(`${base}/nope.json`)).rejects.toThrow(/404/);
    await expect(fetchInstallProfile(`${base}/partial.json`)).rejects.toThrow(/missing/);
  });

  it("reads an install manifest of the shape it knows", async () => {
    const m = await fetchInstallManifest(`${base}/install.json`);
    expect(m.manifest).toBe(1);
    expect(m.solution.name).toBe("chronicle-agents");
    expect(m.stores[0]?.types[0]?.definition.history).toBe("full");
    expect(m.announce).toEqual({ store: "rooms", path: "solution/chronicle-agents" });
  });
  it("refuses a manifest of another shape, a partial one, or none", async () => {
    await expect(fetchInstallManifest(`${base}/install-old.json`)).rejects.toThrow(/shape 0 is not 1/);
    await expect(fetchInstallManifest(`${base}/install-partial.json`)).rejects.toThrow(/missing/);
    await expect(fetchInstallManifest(`${base}/nope.json`)).rejects.toThrow(/404/);
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

describe("the service-account verbs' wire (decision 0052)", () => {
  // The responders are the managed service's; here a stand-in on a real
  // server — the quick start's node user, which the baseline does not bind —
  // server answers them, so what the test proves is the request each
  // method sends and the reply it hands back untouched.
  let u: Up;
  beforeAll(async () => {
    u = await up();
  });
  afterAll(async () => {
    await u.stop();
  });

  it("issue, rotate and revoke carry the account and the name; issue the role too", async () => {
    const nc = await nodeConnection(u);
    const seen: { subject: string; body: unknown }[] = [];
    const sub = nc.subscribe("CHRON.CTRL.IDENTITY.SERVICE_ACCOUNT.>", {
      callback: (_err, msg) => {
        const body = JSON.parse(new TextDecoder().decode(msg.data)) as { account: string; name: string };
        seen.push({ subject: msg.subject, body });
        const verb = msg.subject.split(".")[4];
        const reply =
          verb === "REVOKE"
            ? { account: body.account, name: body.name, public_key: "UOLD" }
            : {
                account: body.account,
                name: body.name,
                role: "writer",
                public_key: "UNEW",
                creds: "-----BEGIN NATS USER JWT-----\n",
              };
        msg.respond(new TextEncoder().encode(JSON.stringify(reply)));
      },
    });
    try {
      const id = new Identity(nc, 4242, "erin");
      const issued = await id.issueServiceAccount("acme", "billing-svc", "writer");
      expect(issued).toEqual({
        account: "acme",
        name: "billing-svc",
        role: "writer",
        public_key: "UNEW",
        creds: "-----BEGIN NATS USER JWT-----\n",
      });
      const rotated = await id.rotateServiceAccount("acme", "billing-svc");
      expect(rotated.public_key).toBe("UNEW");
      const revoked = await id.revokeServiceAccount("acme", "billing-svc");
      expect(revoked).toEqual({ account: "acme", name: "billing-svc", public_key: "UOLD" });
    } finally {
      sub.unsubscribe();
      await nc.close();
    }
    expect(seen).toEqual([
      {
        subject: "CHRON.CTRL.IDENTITY.SERVICE_ACCOUNT.ISSUE.4242.erin",
        body: { account: "acme", name: "billing-svc", role: "writer" },
      },
      {
        subject: "CHRON.CTRL.IDENTITY.SERVICE_ACCOUNT.ROTATE.4242.erin",
        body: { account: "acme", name: "billing-svc" },
      },
      {
        subject: "CHRON.CTRL.IDENTITY.SERVICE_ACCOUNT.REVOKE.4242.erin",
        body: { account: "acme", name: "billing-svc" },
      },
    ]);
  });

  it("a refusal is the service's own, by code", async () => {
    const nc = await nodeConnection(u);
    const sub = nc.subscribe("CHRON.CTRL.IDENTITY.SERVICE_ACCOUNT.ISSUE.4242.erin", {
      callback: (_err, msg) => {
        msg.respond(new Uint8Array(), {
          headers: (() => {
            const h = headers();
            h.set("Nats-Service-Error-Code", "credential-exists");
            h.set("Nats-Service-Error", "service account billing-svc already has a credential; rotate it");
            return h;
          })(),
        });
      },
    });
    try {
      const id = new Identity(nc, 4242, "erin");
      await expect(id.issueServiceAccount("acme", "billing-svc", "writer")).rejects.toMatchObject({
        code: "credential-exists",
      });
    } finally {
      sub.unsubscribe();
      await nc.close();
    }
  });
});
