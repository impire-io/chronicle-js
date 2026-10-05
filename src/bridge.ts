// The managed platform's front door (decisions 0026, 0035, 0038): the
// install profile a client reads before it can connect, and the browser
// identity bridge — the sentinel triggers the callout, a GitHub token
// rides the CONNECT as `<selector>:<token>`, and the server places the
// connection in an account, or in the identity plane where an identity
// learns its memberships and creates its first account. This wire is the
// open contract's client half of the bridge (decision 0043) — the same one
// the Go module's bridge package speaks; the callout that answers it is
// the managed service's, and an install without one connects with creds.
import type { NatsConnection } from "@nats-io/nats-core";
import { Client, dial } from "./client.js";
import { request, type CallOptions } from "./rpc.js";

/** Where the hosted environment publishes its install profile. */
export const HOSTED_PROFILE_URL = "https://chronicle.impire.dev/.well-known/chronicle/profile.json";

/** The install profile: what a client needs to know about an install to log in (decision 0038). */
export interface InstallProfile {
  /** The NATS URL the CLI connects to. */
  url: string;
  /** The URL a browser dials: the websocket listener; absent when the install has none. */
  websocket_url?: string;
  /** The install's GitHub App. */
  github_client_id: string;
  /** The sentinel creds that trigger the callout: public by design, worthless without an identity. */
  sentinel: string;
  /** GitHub's API endpoint, when it is not github.com's. */
  github_api_base?: string;
  /** GitHub's OAuth endpoint, when it is not github.com's. */
  github_oauth_base?: string;
  /** Where the web flow's code and refresh token are traded for tokens: control's exchange (decision 0041). */
  github_token_url?: string;
}

/** Fetches an install's profile; integrity comes from the HTTPS name it is served at. */
export async function fetchInstallProfile(url: string = HOSTED_PROFILE_URL): Promise<InstallProfile> {
  const res = await fetch(url, { cache: "no-cache" });
  if (!res.ok) {
    throw new Error(`install profile ${url}: ${res.status} ${res.statusText}`);
  }
  const p = (await res.json()) as Partial<InstallProfile>;
  if (!p.url || !p.sentinel || !p.github_client_id) {
    throw new Error(`install profile ${url}: missing url, sentinel or github_client_id`);
  }
  return p as InstallProfile;
}

/** The identity plane's selector: a placement into it, not into an account. */
export const IDENTITY_SELECTOR = "+";
/** The account the identity plane lives in. */
export const IDENTITY_PLANE_ACCOUNT = "CONTROL";
const IDENTITY_ROOT = "CHRON.CTRL.IDENTITY.";
const NAME = /^[a-z0-9-]+$/;

/** The CONNECT token the bridge reads: `<selector>:<github-token>`. */
export function bridgeToken(selector: string, githubToken: string): string {
  return `${selector}:${githubToken}`;
}

/** The subject an identity asks for its memberships on. */
export const identityMembershipsSubject = (githubId: number, login: string): string =>
  `${IDENTITY_ROOT}MEMBERSHIPS.${githubId}.${login}`;

/** The subject an identity creates an account on. */
export const identityAccountCreateSubject = (githubId: number, login: string): string =>
  `${IDENTITY_ROOT}ACCOUNT.CREATE.${githubId}.${login}`;

/** Recovers the identity a placement's publish permission names; undefined when it names none. */
export function identityFromSubject(subject: string): { githubId: number; login: string } | undefined {
  if (!subject.startsWith(IDENTITY_ROOT)) {
    return undefined;
  }
  const toks = subject.split(".");
  const login = toks.at(-1) ?? "";
  const githubId = Number(toks.at(-2));
  if (!Number.isSafeInteger(githubId) || githubId <= 0 || !NAME.test(login)) {
    return undefined;
  }
  return { githubId, login };
}

/** What the server says about a placed connection. */
export interface Placement {
  /** The principal: the user JWT's name. */
  user: string;
  /** The account it was placed in. */
  account: string;
  /** The subjects it may publish on. */
  publish: string[];
}

const looksLikeNkey = (s: string): boolean => /^U[A-Z2-7]{55}$/.test(s);

/** Asks the server who this connection is ($SYS.REQ.USER.INFO). */
export async function whoami(nc: NatsConnection): Promise<Placement> {
  const resp = await request<{
    data?: { user?: string; account_name?: string; permissions?: { publish?: { allow?: string[] } } };
  }>(nc, "$SYS.REQ.USER.INFO", {}, { timeout: 5_000 });
  const user = resp.data?.user ?? "";
  if (user === "" || looksLikeNkey(user)) {
    throw new Error(`whoami: the server reported no principal name (got ${JSON.stringify(user)})`);
  }
  return {
    user,
    account: resp.data?.account_name ?? "",
    publish: resp.data?.permissions?.publish?.allow ?? [],
  };
}

/** How to reach the bridge. */
export interface BridgeOptions {
  /** The websocket URL: the profile's websocket_url. */
  servers: string | string[];
  /** The profile's sentinel creds. */
  sentinel: string | Uint8Array;
  /** The GitHub token, or a function that returns a fresh one at every (re)connect. */
  githubToken: string | (() => string);
  name?: string;
  timeout?: number;
}

function dialBridge(opts: BridgeOptions, selector: string): Promise<NatsConnection> {
  const token = opts.githubToken;
  return dial({
    servers: opts.servers,
    creds: opts.sentinel,
    token: typeof token === "function" ? () => bridgeToken(selector, token()) : bridgeToken(selector, token),
    ...(opts.name ? { name: opts.name } : {}),
    ...(opts.timeout ? { timeout: opts.timeout } : {}),
  });
}

/**
 * Connects through the bridge into an account the identity is a member
 * of. The principal comes from the server's own answer, so Op-Author
 * names an identity the bridge actually resolved.
 */
export async function connectBridge(opts: BridgeOptions & { account: string }): Promise<Client> {
  if (opts.account === "" || opts.account === IDENTITY_SELECTOR) {
    throw new Error("bridge connect: an account is required (connectIdentity is the identity plane)");
  }
  const nc = await dialBridge(opts, opts.account);
  try {
    const who = await whoami(nc);
    if (who.account !== opts.account) {
      throw new Error(
        `whoami: placed in account ${JSON.stringify(who.account)}, expected ${JSON.stringify(opts.account)}`,
      );
    }
    return Client.wrap(nc, who.user);
  } catch (err) {
    await nc.close();
    throw err;
  }
}

/** One of an identity's memberships. */
export interface IdentityMembership {
  account: string;
  principal: string;
  role: string;
  created?: boolean;
}

/** What the identity plane says about an identity. */
export interface IdentityMemberships {
  memberships: IdentityMembership[];
  accounts_created: number;
  accounts_per_identity: number;
  plan: string;
}

/** An account the identity plane created. */
export interface IdentityAccountCreated {
  name: string;
  account: string;
  admin: string;
  plan: string;
}

/** A placement in the identity plane: where a login learns its accounts and creates its first. */
export class Identity {
  readonly #nc: NatsConnection;

  constructor(
    nc: NatsConnection,
    /** The GitHub user's numeric id. */
    readonly githubId: number,
    /** The GitHub login, lowercased. */
    readonly login: string,
  ) {
    this.#nc = nc;
  }

  /** The accounts this identity may log into, and what its plan allows. */
  async memberships(opts?: CallOptions): Promise<IdentityMemberships> {
    // The service encodes an identity with no memberships as null; a collection is never null here.
    const r = await request<
      Omit<IdentityMemberships, "memberships"> & { memberships: IdentityMembership[] | null }
    >(this.#nc, identityMembershipsSubject(this.githubId, this.login), {}, opts);
    return { ...r, memberships: r.memberships ?? [] };
  }

  /** Creates an account for the identity, its admin; the name defaults to the login. Waits for the account's node. */
  createAccount(name?: string, opts: CallOptions = {}): Promise<IdentityAccountCreated> {
    return request<IdentityAccountCreated>(
      this.#nc,
      identityAccountCreateSubject(this.githubId, this.login),
      name ? { name } : {},
      {
        timeout: 60_000,
        ...opts,
      },
    );
  }

  /** Closes the placement. */
  close(): Promise<void> {
    return this.#nc.close();
  }
}

/** Connects through the bridge into the identity plane. */
export async function connectIdentity(opts: BridgeOptions): Promise<Identity> {
  const nc = await dialBridge(opts, IDENTITY_SELECTOR);
  try {
    const who = await whoami(nc);
    if (who.account !== IDENTITY_PLANE_ACCOUNT) {
      throw new Error(
        `whoami: placed in account ${JSON.stringify(who.account)}, expected the identity plane`,
      );
    }
    for (const subject of who.publish) {
      const id = identityFromSubject(subject);
      if (id) {
        return new Identity(nc, id.githubId, id.login);
      }
    }
    throw new Error(`whoami: the placement names no identity (permissions ${who.publish.join(", ")})`);
  } catch (err) {
    await nc.close();
    throw err;
  }
}
