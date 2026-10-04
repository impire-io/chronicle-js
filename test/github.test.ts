// The web flow's browser half (decision 0041): PKCE, the authorize URL,
// and the two exchanges against a token endpoint that answers as
// control's does.
import { createHash } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  exchangeGithubCode,
  githubAuthorizeURL,
  GithubSignInError,
  pkcePair,
  randomToken,
  refreshGithubToken,
} from "../src/index.js";

describe("PKCE and the authorize URL", () => {
  it("the challenge is the verifier's S256", async () => {
    const { verifier, challenge } = await pkcePair();
    expect(verifier).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(challenge).toBe(createHash("sha256").update(verifier).digest("base64url"));
  });

  it("states are random and URL-safe", () => {
    const a = randomToken();
    expect(a).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(a).not.toBe(randomToken());
  });

  it("the authorize URL names the App, the return, the state and the challenge", () => {
    const u = new URL(
      githubAuthorizeURL(
        { github_client_id: "Iv1.app" },
        { redirectUri: "https://chronicle.impire.dev/console/", state: "st", codeChallenge: "ch" },
      ),
    );
    expect(u.origin + u.pathname).toBe("https://github.com/login/oauth/authorize");
    expect(Object.fromEntries(u.searchParams)).toEqual({
      client_id: "Iv1.app",
      redirect_uri: "https://chronicle.impire.dev/console/",
      state: "st",
      code_challenge: "ch",
      code_challenge_method: "S256",
    });
    expect(
      githubAuthorizeURL(
        { github_client_id: "x", github_oauth_base: "http://fake" },
        { redirectUri: "r", state: "s" },
      ),
    ).toMatch(/^http:\/\/fake\/login\/oauth\/authorize\?/);
  });
});

describe("the exchanges", () => {
  let server: Server;
  let url: string;
  const seen: unknown[] = [];
  beforeAll(async () => {
    server = createServer((req, res) => {
      let body = "";
      req.on("data", (d: Buffer) => (body += d.toString()));
      req.on("end", () => {
        const b = JSON.parse(body) as Record<string, string>;
        seen.push(b);
        res.setHeader("Content-Type", "application/json");
        if (b.grant_type === "authorization_code" && b.code === "code-1") {
          res.end(JSON.stringify({ access_token: "ghu_new", refresh_token: "ghr_new", expires_in: 28800 }));
        } else if (b.grant_type === "refresh_token" && b.refresh_token === "ghr_new") {
          res.end(JSON.stringify({ access_token: "ghu_fresh", expires_in: 28800 }));
        } else {
          res.statusCode = 400;
          res.end(JSON.stringify({ error: "github: grant refused: bad_verification_code" }));
        }
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/auth/github/token`;
  });
  afterAll(() => {
    server.close();
  });

  it("trades a code, then a refresh token", async () => {
    const tok = await exchangeGithubCode(
      { github_token_url: url },
      { code: "code-1", redirectUri: "https://x/console/", codeVerifier: "v" },
    );
    expect(tok.access_token).toBe("ghu_new");
    expect(seen[0]).toEqual({
      grant_type: "authorization_code",
      code: "code-1",
      redirect_uri: "https://x/console/",
      code_verifier: "v",
    });
    const fresh = await refreshGithubToken({ github_token_url: url }, tok.refresh_token ?? "");
    expect(fresh.access_token).toBe("ghu_fresh");
  });

  it("a refused grant says why", async () => {
    const err: unknown = await exchangeGithubCode(
      { github_token_url: url },
      { code: "used", redirectUri: "r" },
    ).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(GithubSignInError);
    expect((err as GithubSignInError).status).toBe(400);
    expect((err as Error).message).toMatch(/bad_verification_code/);
  });

  it("an install without web-flow sign-in says so", async () => {
    await expect(exchangeGithubCode({}, { code: "c", redirectUri: "r" })).rejects.toThrow(
      /no github_token_url/,
    );
  });
});
