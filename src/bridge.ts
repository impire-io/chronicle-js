// The managed platform's front door (decisions 0026, 0035, 0038): the
// install profile a client reads before it can connect, and the browser
// identity bridge — the sentinel triggers the callout, a GitHub token
// rides the CONNECT as `<selector>:<token>`, and the server places the
// connection in an account, or in the identity plane where an identity
// learns its memberships and creates its first account. This wire is the
// open contract's client half of the bridge (decision 0043) — the same one
// the Go module's bridge package speaks; the callout that answers it is
// the managed service's, and an install without one connects with creds.
// An admin of an account issues, rotates and revokes a service account's
// credential on the same plane (chronicle-hq decision
// 03-DECISIONS/0052-an-admin-issues-and-rotates-service-account-credentials-through-the-identity-plane.md,
// design 02-DESIGN/08-browser-identity.md § the identity plane, at
// chronicle-hq commit 5c1d258): the console speaks these from its Members
// page on a placement opened for the one request and closed after.
// The profile also lists the solutions the environment hosts, each with
// the address of its install manifest, and the manifest's shape is here
// too (chronicle-hq decision
// 03-DECISIONS/0054-a-solution-is-enabled-from-the-console-by-its-install-manifest.md,
// designs 02-DESIGN/08-browser-identity.md and 02-DESIGN/14-solutions.md
// § the install manifest, at chronicle-hq commit 88fa386): the console
// enables a solution for an account from what the manifest declares.
import type { NatsConnection } from "@nats-io/nats-core";
import { Client, dial, type TypeDefinition } from "./client.js";
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
  /** The solutions the environment hosts, each with its install manifest's address (decision 0054); absent when it hosts none. */
  solutions?: InstallProfileSolution[];
}

/** A solution the environment hosts: its name and where its install manifest is. */
export interface InstallProfileSolution {
  name: string;
  /** The address of the solution's install manifest. */
  manifest: string;
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

/** The shape version of the install manifest this SDK reads. */
export const INSTALL_MANIFEST_VERSION = 1;

/**
 * A solution's install manifest (chronicle-hq design 14 § the install
 * manifest): what enabling the solution in an account means — the stores to
 * create, the types to define in each, in order, and the announcement to
 * write. Published by the solution at its own address; names and
 * definitions only, never data and never a credential.
 */
export interface InstallManifest {
  /** The shape's version: 1. */
  manifest: number;
  /** The announcement's fields: the solution as the console shows it. */
  solution: InstallManifestSolution;
  /** The stores to create, in order, each with its types in order. */
  stores: InstallManifestStore[];
  /** Where the announcement is created: the store and the path `solution/<name>`. */
  announce: { store: string; path: string };
}

/** The solution's announcement, as the manifest states it. */
export interface InstallManifestSolution {
  name: string;
  description: string;
  version: string;
  /** Its source of truth: a repository or documentation URL. */
  home: string;
  /** The hosted address of its interface. */
  url: string;
}

/** One store the manifest creates, with the types it defines there in order. */
export interface InstallManifestStore {
  name: string;
  description: string;
  /** The store must allow schedules (decision 0050); absent means not. */
  schedules?: boolean;
  types: InstallManifestType[];
}

/** One type the manifest defines: its name and its definition in the SDK contract's shape. */
export interface InstallManifestType {
  name: string;
  definition: TypeDefinition;
}

/**
 * Fetches a solution's install manifest from its address, fresh, and refuses
 * anything that is not a manifest of the shape this SDK reads.
 */
export async function fetchInstallManifest(
  url: string,
  opts: { signal?: AbortSignal } = {},
): Promise<InstallManifest> {
  const res = await fetch(url, { cache: "no-store", signal: opts.signal ?? null });
  if (!res.ok) {
    throw new Error(`install manifest ${url}: ${res.status} ${res.statusText}`);
  }
  const m = (await res.json()) as Partial<InstallManifest>;
  if (m.manifest !== INSTALL_MANIFEST_VERSION) {
    throw new Error(
      `install manifest ${url}: shape ${String(m.manifest)} is not ${INSTALL_MANIFEST_VERSION}, the one this client reads`,
    );
  }
  if (!m.solution?.name || !Array.isArray(m.stores) || !m.announce?.store || !m.announce.path) {
    throw new Error(`install manifest ${url}: missing solution, stores or announce`);
  }
  return m as InstallManifest;
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

/** The subject an admin issues a service account's credential on. */
export const identityServiceAccountIssueSubject = (githubId: number, login: string): string =>
  `${IDENTITY_ROOT}SERVICE_ACCOUNT.ISSUE.${githubId}.${login}`;

/** The subject an admin rotates a service account's credential on. */
export const identityServiceAccountRotateSubject = (githubId: number, login: string): string =>
  `${IDENTITY_ROOT}SERVICE_ACCOUNT.ROTATE.${githubId}.${login}`;

/** The subject an admin revokes a service account's credential on. */
export const identityServiceAccountRevokeSubject = (githubId: number, login: string): string =>
  `${IDENTITY_ROOT}SERVICE_ACCOUNT.REVOKE.${githubId}.${login}`;

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

/** A service account's credential, handed down once: issued or rotated by an admin of its account. */
export interface ServiceAccountCredential {
  /** The account the service account belongs to. */
  account: string;
  /** The service account's name. */
  name: string;
  /** Its role: admin, writer or reader. */
  role: string;
  /** The public key the credential carries — what the account's registry records. */
  public_key: string;
  /** The credential file, in full; the service keeps no copy. */
  creds: string;
}

/** A service account's credential, revoked: it no longer connects. */
export interface ServiceAccountRevoked {
  account: string;
  name: string;
  /** The public key that was revoked. */
  public_key: string;
}

/**
 * A placement in the identity plane: where a login learns its accounts and
 * creates its first, and where an admin issues, rotates and revokes the
 * credentials of the service accounts in an account it administers.
 */
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

  /**
   * Issues a service account's credential in an account this identity
   * administers: a new service account with the role, or a registered
   * one that holds no credential yet. The credential comes back exactly
   * once. A service account that already holds one is refused
   * (`credential-exists`): rotate it instead.
   */
  issueServiceAccount(
    account: string,
    name: string,
    role: string,
    opts: CallOptions = {},
  ): Promise<ServiceAccountCredential> {
    return request<ServiceAccountCredential>(
      this.#nc,
      identityServiceAccountIssueSubject(this.githubId, this.login),
      { account, name, role },
      { timeout: 30_000, ...opts },
    );
  }

  /**
   * Rotates a service account's credential: the one it connects with now
   * stops working, and a new one for the same service account, with the
   * same role, comes back exactly once.
   */
  rotateServiceAccount(
    account: string,
    name: string,
    opts: CallOptions = {},
  ): Promise<ServiceAccountCredential> {
    return request<ServiceAccountCredential>(
      this.#nc,
      identityServiceAccountRotateSubject(this.githubId, this.login),
      { account, name },
      { timeout: 30_000, ...opts },
    );
  }

  /** Revokes a service account: its credential stops working first, then the service account is removed. */
  revokeServiceAccount(
    account: string,
    name: string,
    opts: CallOptions = {},
  ): Promise<ServiceAccountRevoked> {
    return request<ServiceAccountRevoked>(
      this.#nc,
      identityServiceAccountRevokeSubject(this.githubId, this.login),
      { account, name },
      { timeout: 30_000, ...opts },
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
