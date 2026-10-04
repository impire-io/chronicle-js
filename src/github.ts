// The browser's GitHub sign-in (decision 0041): the web flow's redirect
// with a state and PKCE, and the two exchanges — the authorization code,
// later the refresh token — made by the install's control at the
// profile's github_token_url, since the App's client secret is not the
// browser's to hold. The tokens come back to the caller, which keeps them
// client-side and hands the access token to the bridge.
import type { InstallProfile } from "./bridge.js";

/** GitHub's grant, as control passes it on. */
export interface GithubTokens {
  access_token: string;
  refresh_token?: string;
  /** Seconds the access token lives. */
  expires_in?: number;
  /** Seconds the refresh token lives. */
  refresh_token_expires_in?: number;
}

/** A refusal from the token endpoint: the code or refresh token was not accepted. */
export class GithubSignInError extends Error {
  constructor(
    message: string,
    /** The endpoint's HTTP status. */
    readonly status: number,
  ) {
    super(message);
    this.name = "GithubSignInError";
  }
}

const base64url = (bytes: Uint8Array): string =>
  btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/, "");

/** A random URL-safe string: a state, or a PKCE verifier. */
export function randomToken(bytes = 32): string {
  return base64url(crypto.getRandomValues(new Uint8Array(bytes)));
}

/** A PKCE pair: keep the verifier, send the challenge (S256). */
export async function pkcePair(): Promise<{ verifier: string; challenge: string }> {
  const verifier = randomToken(32);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
  return { verifier, challenge: base64url(new Uint8Array(digest)) };
}

/** Where to send the browser to sign in: GitHub's authorize page for the install's App. */
export function githubAuthorizeURL(
  profile: Pick<InstallProfile, "github_client_id" | "github_oauth_base">,
  opts: { redirectUri: string; state: string; codeChallenge?: string },
): string {
  const u = new URL("/login/oauth/authorize", profile.github_oauth_base ?? "https://github.com");
  u.searchParams.set("client_id", profile.github_client_id);
  u.searchParams.set("redirect_uri", opts.redirectUri);
  u.searchParams.set("state", opts.state);
  if (opts.codeChallenge) {
    u.searchParams.set("code_challenge", opts.codeChallenge);
    u.searchParams.set("code_challenge_method", "S256");
  }
  return u.toString();
}

async function tokenCall(url: string, body: Record<string, string>): Promise<GithubTokens> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = (await res.json().catch(() => ({}))) as Partial<GithubTokens> & { error?: string };
  if (!res.ok || !data.access_token) {
    throw new GithubSignInError(
      `GitHub sign-in: ${data.error ?? `the token endpoint answered ${res.status}`}`,
      res.status,
    );
  }
  return data as GithubTokens;
}

function tokenURL(profile: Pick<InstallProfile, "github_token_url">): string {
  if (!profile.github_token_url) {
    throw new Error("this install has no web-flow sign-in: its profile names no github_token_url");
  }
  return profile.github_token_url;
}

/** Trades the code GitHub returned to the console for tokens, through the install's control. */
export async function exchangeGithubCode(
  profile: Pick<InstallProfile, "github_token_url">,
  opts: { code: string; redirectUri: string; codeVerifier?: string },
): Promise<GithubTokens> {
  return tokenCall(tokenURL(profile), {
    grant_type: "authorization_code",
    code: opts.code,
    redirect_uri: opts.redirectUri,
    ...(opts.codeVerifier ? { code_verifier: opts.codeVerifier } : {}),
  });
}

/** Trades a refresh token for fresh tokens, through the install's control. */
export async function refreshGithubToken(
  profile: Pick<InstallProfile, "github_token_url">,
  refreshToken: string,
): Promise<GithubTokens> {
  return tokenCall(tokenURL(profile), { grant_type: "refresh_token", refresh_token: refreshToken });
}
