// The client: one account's sentences over one NATS connection — the SDK
// contract (design 12) as the Go client implements it, in TypeScript, in
// the words of decision 0044: a store, its types, their instances named by
// paths, children, snapshots. Control verbs are request/reply; queries are
// streamed replies; writes are guarded JetStream publishes; reads of data
// at rest are JetStream — ordered consumers, KV scans and watches — never
// a node verb. Every verb is spoken on the caller's own subject,
// CHRON.API.<principal>.<VERB…>, and no request names a principal; a write
// may go through the node instead of straight to the stream, which then
// vouches for the author (decision 0057). Paths are converted to their
// stored tails here, once, at the edge; subjects and keys never see a slash.
import {
  credsAuthenticator,
  headers as natsHeaders,
  nkeyAuthenticator,
  nuid,
  RequestError,
  tokenAuthenticator,
  wsconnect,
  type Authenticator,
  type MsgHdrs,
  type NatsConnection,
} from "@nats-io/nats-core";
import {
  DeliverPolicy,
  JetStreamApiCodes,
  JetStreamApiError,
  jetstream,
  jetstreamManager,
  type Consumer,
  type JetStreamClient,
  type JetStreamManager,
  type JsMsg,
} from "@nats-io/jetstream";
import { Kvm, type KV } from "@nats-io/kv";
import {
  InstanceExistsError,
  InstanceMovedError,
  NoResponderError,
  NoTypeError,
  NotFoundError,
  PreconditionFailedError,
  SchemaViolationError,
  StateOffSchemaError,
  StaleVersionError,
  UndeclaredChildError,
  UndefinedOperationError,
} from "./errors.js";
import { verifyAttestation } from "./contract/attest.js";
import {
  guardRetryLanded,
  judgePrecondition,
  judgeSnapshot,
  resolveInstance,
  type Resolution,
} from "./contract/fold.js";
import { mergePatch } from "./contract/merge.js";
import {
  applySubject,
  metaAttester,
  metaIndex,
  metaMember,
  metaStoreConfig,
  metaStoreType,
  opsFilter,
  opsSubject,
  pathTail,
  stateBucket,
  streamName,
  tailPath,
  TAIL_SEPARATOR,
  validateStoreName,
  validateTypeName,
} from "./contract/names.js";
import {
  HDR,
  opHeaders,
  parseAttestation,
  parseOpHeaders,
  rfc3339,
  ROLLUP_SUBJECT,
  SNAPSHOT,
  type IndexDeclaration,
  type Op,
  type OpDef,
  type StateValue,
  type StoreConfig,
  type TypeRecord,
} from "./contract/record.js";
import { compileSchema } from "./contract/schema.js";
import {
  GRAMMARS,
  SUBJECTS,
  type ApplyReply,
  type IndexDeclareReply,
  type IndexDeleteReply,
  type IndexQueryGraphNeighborsItem,
  type IndexQueryGraphNeighborsRequest,
  type IndexQueryGraphNeighborsTrailer,
  type IndexQueryGraphWalkItem,
  type IndexQueryGraphWalkRequest,
  type IndexQueryGraphWalkTrailer,
  type IndexQuerySearchItem,
  type IndexQuerySearchTrailer,
  type IndexQuerySemanticItem,
  type IndexQuerySemanticTrailer,
  type InstanceSnapshotReply,
  type ListIndexesItem,
  type ListInstancesItem,
  type ListMembersItem,
  type MemberAddReply,
  type MemberRevokeReply,
  type MemberSetRoleReply,
  type PingReply,
  type StoreCreateReply,
  type StoreDeleteReply,
  type TypeDefineReply,
  type TypeDeleteReply,
  type WatchDeclarationsItem,
} from "./generated/contract.js";
import { request, requestStream, serviceErrorOf, type CallOptions, type Streamed } from "./rpc.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** How long a bounded history waits for each op, and for the whole read. */
const REPLAY_WAIT_MS = 1_000;
const REPLAY_BUDGET_MS = 20_000;

/** How to reach chronicle and who to be. */
export interface ConnectOptions {
  /** The websocket URL(s): wss://connect.chronicle.impire.dev, or `chronicle up`'s ws:// one. */
  servers: string | string[];
  /** A credential file's contents; the principal is its user JWT's name. */
  creds?: Uint8Array | string;
  /** An nkey seed; needs `author`, since an nkey carries no name. */
  nkeySeed?: Uint8Array | string;
  /** A token for the server's auth_token. */
  token?: string | (() => string);
  /** The principal, when the credential carries no name (11-the-two-forms.md § bring your own NATS). */
  author?: string;
  /** The connection's name, as the server reports it. */
  name?: string;
  /** How long to wait for the connection, in milliseconds. */
  timeout?: number;
}

/** The principal a credential file's user JWT names. */
export function principalFromCreds(creds: Uint8Array | string): string {
  const text = typeof creds === "string" ? creds : decoder.decode(creds);
  const jwt = /-----BEGIN NATS USER JWT-----\s*([^\s-]+)\s*-+END NATS USER JWT-+/.exec(text)?.[1];
  const claims = jwt?.split(".")[1];
  if (claims === undefined) {
    throw new Error("creds: no user JWT");
  }
  const b64 = claims.replaceAll("-", "+").replaceAll("_", "/");
  const json = decoder.decode(Uint8Array.from(atob(b64), (c) => c.charCodeAt(0)));
  const name = (JSON.parse(json) as { name?: unknown }).name;
  if (typeof name !== "string" || name === "") {
    throw new Error("creds: the user JWT names no principal");
  }
  return name;
}

const bytes = (v: Uint8Array | string): Uint8Array => (typeof v === "string" ? encoder.encode(v) : v);

/** Opens a websocket connection with the authenticators the options name. */
export async function dial(opts: ConnectOptions): Promise<NatsConnection> {
  const authenticator: Authenticator[] = [];
  if (opts.creds !== undefined) {
    authenticator.push(credsAuthenticator(bytes(opts.creds)));
  }
  if (opts.nkeySeed !== undefined) {
    authenticator.push(nkeyAuthenticator(bytes(opts.nkeySeed)));
  }
  if (opts.token !== undefined) {
    authenticator.push(tokenAuthenticator(opts.token));
  }
  return wsconnect({
    servers: opts.servers,
    name: opts.name ?? "chronicle-js",
    timeout: opts.timeout ?? 5_000,
    ...(authenticator.length > 0 ? { authenticator } : {}),
  });
}

/** A landed op: its ID and the stream sequence it took. */
export interface Ack {
  opId: string;
  seq: number;
}

/** Options for a write. */
export interface ApplyOptions {
  /** The op IDs the writer had seen. */
  parents?: string[];
  /** The op ID; minted when absent, and kept across a retry. */
  opId?: string;
  /** The sequence of the instance's last operation; the write is refused if anything landed since. */
  expectedSeq?: number;
  /**
   * Ask for a verified author (decision 0057): the write goes through
   * chronicle, which vouches for who wrote it, instead of straight to the
   * stream. A store that requires verified authors is written this way
   * without asking.
   */
  verified?: boolean;
}

/** A type's definition (design 07 § types). */
export interface TypeDefinition {
  schema: unknown;
  /** The history policy: compactable (the default) or full. */
  history?: string;
  /** Child name → type: what may be nested under an instance of this type. */
  children?: Record<string, string>;
  operations?: Record<string, OpDef>;
}

/** Options for creating a store. */
export interface StoreOptions {
  description?: string;
  /** compactable (the default) or full. */
  history?: string;
  /** The store's byte budget; absent means the account's default. */
  maxBytes?: number;
  /** The store allows schedules and per-message TTLs (decision 0050); absent means not. */
  schedules?: boolean;
  /** The store requires verified authors (decision 0057): every write goes through chronicle, and an operation written any other way is kept but takes no effect. */
  verifiedAuthors?: boolean;
}

/** Where a tail starts, and how wide it reads. */
export interface TailOptions {
  /** Start after this sequence. */
  after?: number;
  /** Only what lands from now on. */
  live?: boolean;
  /** Include the operations of the instance's children — what lives under its path — in one order (decision 0049). */
  children?: boolean;
  /** Ends the tail. */
  signal?: AbortSignal;
}

/** Options for a live iterator. */
export interface LiveOptions {
  /** Ends the iterator. */
  signal?: AbortSignal;
}

/** A query's options. */
export interface QueryOptions {
  /** Cap the items; absent streams every match. */
  limit?: number;
  signal?: AbortSignal;
}

/** How a listing of instances is narrowed (decision 0045 § 2). */
export interface ListInstancesOptions {
  /** Only instances of this type: top-level ones, or — with `under` — the children declared as it. */
  type?: string;
  /** Only the direct children of this instance, by path. */
  under?: string;
  /** Only instances whose state fields equal these values (scalars, compared as text; a dotted field walks nested objects). */
  where?: Record<string, string | number | boolean | null>;
}

/** Options for adding a principal. */
export interface MemberOptions {
  /** The NATS user public key, where your NATS names users by key. */
  publicKey?: string;
  /** The person's GitHub user id, for signing in with GitHub. */
  githubId?: number;
  /** A person (member, the default) or a machine (service). */
  kind?: "member" | "service";
}

/** A graph query: neighbors or walk, without the field the SDK fills. */
export type GraphQuery<R> = Omit<R, "op">;

/** A declaration as a watch yields it. */
export type Declaration = WatchDeclarationsItem;

/** One account's sentences over one connection. */
export class Client {
  readonly #nc: NatsConnection;
  readonly #js: JetStreamClient;
  readonly #kvm: Kvm;
  readonly #author: string;
  readonly #buckets = new Map<string, Promise<KV>>();
  readonly #inFlight = new Map<string, Promise<unknown>>();
  /** Which stores require verified authors — set at creation, read once. */
  readonly #verifiedStores = new Map<string, Promise<boolean>>();
  /** The attester keys META lists, as they are met. */
  readonly #attesters = new Set<string>();
  #jsm: Promise<JetStreamManager> | undefined;

  private constructor(nc: NatsConnection, author: string) {
    this.#nc = nc;
    this.#author = author;
    this.#js = jetstream(nc);
    this.#kvm = new Kvm(this.#js);
  }

  /** Connects over a websocket. The principal is the credential's name, or `author`. */
  static async connect(opts: ConnectOptions): Promise<Client> {
    const author = opts.creds !== undefined ? principalFromCreds(opts.creds) : opts.author;
    if (author === undefined || author === "") {
      throw new Error("connect: the principal must be stated when the credential carries no name");
    }
    return new Client(await dial(opts), author);
  }

  /** Adopts a connection made elsewhere — a Node TCP connection, or a bridge placement. */
  static wrap(nc: NatsConnection, author: string): Client {
    return new Client(nc, author);
  }

  /** The principal every write names and every verb is spoken as. */
  get author(): string {
    return this.#author;
  }

  /** A verb's subject as this client speaks it: the caller's own (decision 0057). */
  #subject(name: keyof typeof SUBJECTS): string {
    return SUBJECTS[name].replace("<principal>", this.#author);
  }

  /** The connection underneath. */
  get connection(): NatsConnection {
    return this.#nc;
  }

  /** Closes the connection. */
  async close(): Promise<void> {
    await this.#nc.close();
  }

  // --- control verbs: request/reply -------------------------------------

  /** Asks the node who it is. */
  ping(opts?: CallOptions): Promise<PingReply> {
    return request(this.#nc, this.#subject("ping"), {}, opts);
  }

  /** Creates a store. */
  createStore(store: string, opts: StoreOptions = {}): Promise<StoreCreateReply> {
    return request(this.#nc, this.#subject("store.create"), {
      store,
      ...(opts.description ? { description: opts.description } : {}),
      ...(opts.history ? { history: opts.history } : {}),
      ...(opts.maxBytes ? { max_bytes: opts.maxBytes } : {}),
      ...(opts.schedules ? { schedules: true } : {}),
      ...(opts.verifiedAuthors ? { verified_authors: true } : {}),
    });
  }

  /** Defines, or redefines, a type in a store. */
  defineType(store: string, type: string, def: TypeDefinition): Promise<TypeDefineReply> {
    return request(this.#nc, this.#subject("type.define"), {
      store,
      type,
      schema: def.schema,
      ...(def.history ? { history: def.history } : {}),
      ...(def.children ? { children: def.children } : {}),
      ...(def.operations ? { operations: def.operations } : {}),
    });
  }

  /** Declares an index on a store. */
  declareIndex(store: string, index: string, kind: string, config?: unknown): Promise<IndexDeclareReply> {
    return request(this.#nc, this.#subject("index.declare"), {
      store,
      index,
      kind,
      ...(config === undefined || config === null ? {} : { config }),
    });
  }

  /** Deletes an index declaration. */
  deleteIndex(store: string, index: string): Promise<IndexDeleteReply> {
    return request(this.#nc, this.#subject("index.delete"), { store, index });
  }

  /**
   * Deletes a store (decision 0062): total and irreversible — its history,
   * state and indexes go, whatever the history policy — in a fixed order a
   * second call finishes from any step of. The name may be created again
   * once it is gone.
   */
  deleteStore(store: string): Promise<StoreDeleteReply> {
    return request(this.#nc, this.#subject("store.delete"), { store });
  }

  /**
   * Deletes a type (decision 0062): its instances keep their history and
   * are untyped — no state, absent from the listings — until the type is
   * defined again, which restores them.
   */
  deleteType(store: string, type: string): Promise<TypeDeleteReply> {
    return request(this.#nc, this.#subject("type.delete"), { store, type });
  }

  /**
   * Asks chronicle to take a snapshot of the instance: write its current
   * state as one entry and compact the history before it. Declining is an
   * answer (`taken: false`, with the reason), not an error.
   */
  snapshot(store: string, path: string): Promise<InstanceSnapshotReply> {
    pathTail(path);
    return request(this.#nc, this.#subject("instance.snapshot"), {
      store,
      instance: path,
    });
  }

  /** Adds a member — a person — or, with `kind: "service"`, a service account. */
  addMember(member: string, role: string, opts: MemberOptions = {}): Promise<MemberAddReply> {
    return request(this.#nc, this.#subject("member.add"), {
      member,
      role,
      ...(opts.kind ? { kind: opts.kind } : {}),
      ...(opts.publicKey ? { public_key: opts.publicKey } : {}),
      ...(opts.githubId ? { github_id: opts.githubId } : {}),
    });
  }

  /** Adds a service account: a machine with a credential. */
  addServiceAccount(
    name: string,
    role: string,
    opts: Omit<MemberOptions, "kind" | "githubId"> = {},
  ): Promise<MemberAddReply> {
    return this.addMember(name, role, { ...opts, kind: "service" });
  }

  /** Removes a member, or revokes a service account: the registry entry goes. The account's only admin cannot be removed. */
  revokeMember(member: string): Promise<MemberRevokeReply> {
    return request(this.#nc, this.#subject("member.revoke"), { member });
  }

  /**
   * Changes a member's or a service account's role in place (decision 0062):
   * effective from the principal's next request, the credential untouched.
   * The account's only admin cannot be demoted.
   */
  setRole(member: string, role: string): Promise<MemberSetRoleReply> {
    return request(this.#nc, this.#subject("member.set-role"), { member, role });
  }

  // --- queries: streamed replies -----------------------------------------

  /** Queries a search index; empty text matches everything. Hits name instances by path. */
  queryIndex(
    store: string,
    index: string,
    query: string,
    opts: QueryOptions = {},
  ): Streamed<IndexQuerySearchItem, IndexQuerySearchTrailer> {
    return requestStream(
      this.#nc,
      this.#querySubject(store, index),
      { query, ...(opts.limit ? { limit: opts.limit } : {}) },
      opts,
    );
  }

  /** An instance's edges in a graph index. */
  graphNeighbors(
    store: string,
    index: string,
    q: GraphQuery<IndexQueryGraphNeighborsRequest>,
    opts: { signal?: AbortSignal } = {},
  ): Streamed<IndexQueryGraphNeighborsItem, IndexQueryGraphNeighborsTrailer> {
    return requestStream(this.#nc, this.#querySubject(store, index), { ...q, op: "neighbors" }, opts);
  }

  /** The instances reachable from an instance in a graph index. */
  graphWalk(
    store: string,
    index: string,
    q: GraphQuery<IndexQueryGraphWalkRequest>,
    opts: { signal?: AbortSignal } = {},
  ): Streamed<IndexQueryGraphWalkItem, IndexQueryGraphWalkTrailer> {
    return requestStream(this.#nc, this.#querySubject(store, index), { ...q, op: "walk" }, opts);
  }

  /** Queries a semantic index by meaning. */
  querySemantic(
    store: string,
    index: string,
    text: string,
    opts: QueryOptions = {},
  ): Streamed<IndexQuerySemanticItem, IndexQuerySemanticTrailer> {
    return requestStream(
      this.#nc,
      this.#querySubject(store, index),
      { text, ...(opts.limit ? { limit: opts.limit } : {}) },
      opts,
    );
  }

  /** An index's query subject as this client speaks it. */
  #querySubject(store: string, index: string): string {
    return this.#subject("index.query.search").replace("<store>", store).replace("<index>", index);
  }

  // --- writes: guarded publishes ------------------------------------------

  /**
   * Creates an instance from a snapshot of its whole state, only if it does
   * not exist yet — the untyped form, and the application's own when it
   * folds state itself. A typed instance is created with `create`.
   */
  async createFromSnapshot(
    store: string,
    path: string,
    state: unknown = {},
    opts: ApplyOptions = {},
  ): Promise<Ack> {
    validateStoreName(store);
    const tail = pathTail(path);
    if (opts.expectedSeq !== undefined) {
      throw new Error("expectedSeq: a create expects no history by definition");
    }
    await this.#preflightSnapshot(store, tail, state);
    const payload = encoder.encode(JSON.stringify({ state, frontier: [] }));
    return this.#publish(
      store,
      tail,
      SNAPSHOT,
      payload,
      opts,
      { guard: 0 },
      (t) => new InstanceExistsError(t),
    );
  }

  /**
   * Creates an instance by applying one of its type's operations — `create`
   * by convention — only if it does not exist yet. The data is checked
   * against that operation's schema before anything is sent.
   */
  async create(
    store: string,
    path: string,
    op = "create",
    data: unknown = {},
    opts: ApplyOptions = {},
  ): Promise<Ack> {
    validateStoreName(store);
    const tail = pathTail(path);
    if (op === "") {
      throw new Error("operation: must not be empty");
    }
    if (op === SNAPSHOT) {
      throw new Error("create with a snapshot: createFromSnapshot is that form");
    }
    if (opts.expectedSeq !== undefined) {
      throw new Error("expectedSeq: a create expects no history by definition");
    }
    const bytesOf = toBytes(data);
    // A create applies one of the type's operations: a path that resolves
    // to no type has none to apply — a type never defined, or one deleted
    // (decision 0062). The untyped form is createFromSnapshot.
    const res = await this.#resolveTail(store, tail);
    if (res.kind === "untyped") {
      throw new NoTypeError(
        `${path} resolves to no type in store ${store}: define the type, or create the instance from a snapshot`,
      );
    }
    await this.#preflightApply(store, tail, op, bytesOf);
    return this.#publish(store, tail, op, bytesOf, opts, { guard: 0 }, (t) => new InstanceExistsError(t));
  }

  /**
   * Applies an operation to an instance; with expectedSeq, refused if
   * anything landed on the instance since that sequence.
   */
  async apply(store: string, path: string, op: string, data: unknown, opts: ApplyOptions = {}): Promise<Ack> {
    validateStoreName(store);
    const tail = pathTail(path);
    if (op === "") {
      throw new Error("operation: must not be empty");
    }
    const bytesOf = toBytes(data);
    await this.#preflightApply(store, tail, op, bytesOf);
    return this.#publish(
      store,
      tail,
      op,
      bytesOf,
      opts,
      opts.expectedSeq === undefined ? {} : { guard: opts.expectedSeq },
      (t) => new InstanceMovedError(t),
    );
  }

  /**
   * Saves a snapshot the application materialised itself: it replaces the
   * instance's history up to `upTo` (the sequence of the last operation the
   * state covers), guarded there.
   */
  async saveSnapshot(
    store: string,
    path: string,
    state: unknown,
    frontier: string[],
    upTo: number,
    opts: ApplyOptions = {},
  ): Promise<Ack> {
    validateStoreName(store);
    const tail = pathTail(path);
    if (upTo <= 0) {
      throw new Error(
        "upTo: the sequence of the last operation the state covers; a first write is createFromSnapshot",
      );
    }
    if (opts.expectedSeq !== undefined) {
      throw new Error("expectedSeq: a saved snapshot guards at upTo");
    }
    await this.#preflightSnapshot(store, tail, state);
    const payload = encoder.encode(JSON.stringify({ state, frontier }));
    return this.#publish(
      store,
      tail,
      SNAPSHOT,
      payload,
      opts,
      { guard: upTo, rollup: true },
      (t) => new StaleVersionError(t),
    );
  }

  /**
   * One write: the op's headers, the guard, one in flight per subject;
   * after a guard refusal, the subject's last op says whether this op
   * landed (dedup's echo) or the instance moved. A store that requires
   * verified authors, or a caller that asks for one, sends the same record
   * to the node instead (decision 0057).
   */
  async #publish(
    store: string,
    tail: string,
    opType: string,
    payload: Uint8Array,
    opts: ApplyOptions,
    guard: { guard?: number; rollup?: boolean },
    refused: (what: string) => Error,
  ): Promise<Ack> {
    const subject = opsSubject(store, tail);
    const opId = opts.opId ?? nuid.next();
    const h = natsHeaders();
    for (const [k, v] of opHeaders({
      id: opId,
      type: opType,
      author: this.#author,
      parents: opts.parents ?? [],
      ts: rfc3339(new Date()),
      version: "",
    })) {
      h.append(k, v);
    }
    if (guard.rollup) {
      h.set(HDR.rollup, ROLLUP_SUBJECT);
    }
    if (guard.guard !== undefined) {
      h.set(HDR.expectedLastSubjectSeq, String(guard.guard));
    }
    if (opts.verified || (await this.#requiresVerified(store))) {
      return this.#applyThroughNode(store, tail, opId, h, payload, refused);
    }
    return this.#serial(subject, async () => {
      try {
        const ack = await this.#js.publish(subject, payload, { headers: h });
        return { opId, seq: ack.seq };
      } catch (err) {
        if (guard.guard !== undefined && guardRefused(err)) {
          const last = await this.#lastOp(store, subject);
          if (guardRetryLanded(last?.id ?? "", opId)) {
            return { opId, seq: last?.seq ?? 0 };
          }
          throw refused(`${tailPath(tail)} in store ${store}`);
        }
        throw err;
      }
    });
  }

  /**
   * The verified write: the record, headers and payload as built, sent to
   * the node on this caller's own APPLY subject. The node binds the author
   * to this connection's name, signs the operation, publishes it with this
   * id and guard, and answers the sequence — or a catalogued refusal, read
   * here as the direct route's errors.
   */
  async #applyThroughNode(
    store: string,
    tail: string,
    opId: string,
    headers: MsgHdrs,
    payload: Uint8Array,
    refused: (what: string) => Error,
  ): Promise<Ack> {
    const subject = applySubject(this.#author, store, tail);
    let msg;
    try {
      msg = await this.#nc.request(subject, payload, { headers, timeout: 10_000 });
    } catch (err) {
      if (err instanceof RequestError && err.isNoResponders()) {
        throw new NoResponderError(subject);
      }
      throw err;
    }
    const error = serviceErrorOf(msg);
    if (error) {
      const what = `${tailPath(tail)} in store ${store}`;
      switch (error.code) {
        case "undefined-operation":
          throw new UndefinedOperationError(error.description);
        case "undeclared-child":
          throw new UndeclaredChildError(error.description);
        case "schema-violation":
          throw new SchemaViolationError(error.description);
        case "precondition-failed":
          throw new PreconditionFailedError(error.description);
        case "state-off-schema":
          throw new StateOffSchemaError(error.description);
        case "instance-exists":
          throw new InstanceExistsError(what);
        case "instance-moved":
          throw refused(what);
      }
      throw error;
    }
    const reply = JSON.parse(decoder.decode(msg.data)) as ApplyReply;
    return { opId, seq: reply.seq };
  }

  /** Whether the store requires verified authors — its declaration, set at creation, read once. */
  #requiresVerified(store: string): Promise<boolean> {
    let known = this.#verifiedStores.get(store);
    if (known === undefined) {
      known = this.getStore(store).then(
        (cfg) => cfg.verified_authors === true,
        // A store that does not exist requires nothing; the write then
        // fails where it would have anyway.
        (err: unknown) => {
          if (err instanceof NotFoundError) return false;
          throw err;
        },
      );
      known.catch(() => this.#verifiedStores.delete(store));
      this.#verifiedStores.set(store, known);
    }
    return known;
  }

  /**
   * Sets op.verified: the attestation present, its key one META lists —
   * read once per key — and the signature valid over the record.
   */
  async #verify(op: Op): Promise<Op> {
    op.verified = false;
    if (op.attester === "" || op.signature === "") {
      return op;
    }
    if (!this.#attesters.has(op.attester)) {
      const entry = await (await this.#bucket(GRAMMARS.metaBucket)).get(metaAttester(op.attester));
      if (!entry || entry.operation !== "PUT") {
        return op;
      }
      this.#attesters.add(op.attester);
    }
    op.verified = await verifyAttestation(op);
    return op;
  }

  /** Runs fn after the subject's previous publish settles: one in flight per subject. */
  async #serial<T>(subject: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.#inFlight.get(subject) ?? Promise.resolve();
    const next = prev.then(fn, fn);
    const settled = next.then(
      () => undefined,
      () => undefined,
    );
    this.#inFlight.set(subject, settled);
    try {
      return await next;
    } finally {
      if (this.#inFlight.get(subject) === settled) {
        this.#inFlight.delete(subject);
      }
    }
  }

  /** The subject's last op: its ID and sequence; undefined when there is none. */
  async #lastOp(store: string, subject: string): Promise<{ id: string; seq: number } | undefined> {
    this.#jsm ??= jetstreamManager(this.#nc);
    const jsm = await this.#jsm;
    const msg = await jsm.streams.getMessage(streamName(store), { last_by_subj: subject });
    if (msg === null) {
      return undefined;
    }
    return { id: msg.header.get(HDR.msgId), seq: msg.seq };
  }

  // --- preflight: the type's rules, before anything is sent -----------------

  /** Resolves an instance's path against the store's types. */
  async resolve(store: string, path: string): Promise<Resolution> {
    validateStoreName(store);
    return this.#resolveTail(store, pathTail(path));
  }

  async #resolveTail(store: string, tail: string): Promise<Resolution> {
    const meta = await this.#bucket(GRAMMARS.metaBucket);
    return resolveInstance(tail, async (name) => {
      const entry = await meta.get(metaStoreType(store, name));
      return entry && entry.operation === "PUT" ? entry.json<TypeRecord>() : undefined;
    });
  }

  async #preflightApply(store: string, tail: string, opType: string, payload: Uint8Array): Promise<void> {
    if (opType === SNAPSHOT) {
      const snap = parseJSON(payload);
      if (snap.ok && isRecord(snap.value)) {
        await this.#preflightSnapshot(store, tail, snap.value.state);
      }
      return;
    }
    const res = await this.#resolveTail(store, tail);
    if (res.kind === "undeclared") {
      throw new UndeclaredChildError(res.detail);
    }
    if (res.kind !== "typed") {
      return;
    }
    const def = res.record.operations?.[opType];
    if (def === undefined) {
      throw new UndefinedOperationError(
        `${res.typeName} defines no operation ${JSON.stringify(opType)}. It defines: ${Object.keys(
          res.record.operations ?? {},
        )
          .sort()
          .join(", ")}`,
      );
    }
    const parsed = parseJSON(payload);
    if (!parsed.ok) {
      throw new SchemaViolationError(`the data is not JSON: ${parsed.detail}`);
    }
    const failed = compileSchema(def.schema)(parsed.value);
    if (failed !== "") {
      throw new SchemaViolationError(`the data does not fit ${opType}'s schema: ${failed}`);
    }
    // The state's rules (decision 0059): where the operation declares a
    // precondition, or the type a schema a merge must keep, the current
    // state is read once and judged as chronicle will judge it.
    const effect = def.effect === undefined || def.effect === "" ? "none" : def.effect;
    const merges = effect === "merge" && res.record.schema !== undefined;
    if (def.precondition === undefined && !merges) {
      return;
    }
    const state = await this.#stateOfTail(store, tail);
    const unmet = judgePrecondition(def, state);
    if (unmet !== "") {
      throw new PreconditionFailedError(
        `${opType} on ${tailPath(tail)}: ${unmet.replace(/^the operation's precondition is not met: /, "")}`,
      );
    }
    if (merges) {
      const failedAfter = judgeSnapshot(res.record, mergePatch(state, parsed.value));
      if (failedAfter !== "") {
        throw new StateOffSchemaError(
          `${opType} on ${tailPath(tail)}: after the operation, ${failedAfter.replace(/^state fails the type's schema: /, `the state fails ${res.typeName}'s schema: `)}`,
        );
      }
    }
  }

  /** The instance's current derived state, or undefined when it has none yet. */
  async #stateOfTail(store: string, tail: string): Promise<unknown> {
    const entry = await (await this.#bucket(stateBucket(store))).get(tail);
    if (!entry || entry.operation !== "PUT") {
      return undefined;
    }
    return entry.json<StateValue>().state;
  }

  async #preflightSnapshot(store: string, tail: string, state: unknown): Promise<void> {
    const res = await this.#resolveTail(store, tail);
    if (res.kind === "undeclared") {
      throw new UndeclaredChildError(res.detail);
    }
    if (res.kind !== "typed" || res.record.schema === undefined) {
      return;
    }
    const failed = compileSchema(res.record.schema)(state);
    if (failed !== "") {
      throw new SchemaViolationError(`state fails ${res.typeName}'s schema: ${failed}`);
    }
  }

  // --- single values: replies at rest ---------------------------------------

  /** An instance's state and the sequence it stands at. */
  async state(store: string, path: string): Promise<StateValue> {
    validateStoreName(store);
    const tail = pathTail(path);
    const entry = await (await this.#bucket(stateBucket(store))).get(tail);
    if (!entry || entry.operation !== "PUT") {
      throw new NotFoundError(`no state for ${path} in store ${store}`);
    }
    return entry.json<StateValue>();
  }

  /** A store's settings. */
  async getStore(store: string): Promise<StoreConfig> {
    validateStoreName(store);
    const entry = await (await this.#bucket(GRAMMARS.metaBucket)).get(metaStoreConfig(store));
    if (!entry || entry.operation !== "PUT") {
      throw new NotFoundError(`store does not exist: ${store}`);
    }
    return entry.json<StoreConfig>();
  }

  /** A type's record. */
  async getType(store: string, type: string): Promise<TypeRecord> {
    validateStoreName(store);
    validateTypeName(type);
    const entry = await (await this.#bucket(GRAMMARS.metaBucket)).get(metaStoreType(store, type));
    if (!entry || entry.operation !== "PUT") {
      throw new NotFoundError(`type is not defined: ${type} in store ${store}`);
    }
    return entry.json<TypeRecord>();
  }

  /** An index's declaration. */
  async getIndexDeclaration(store: string, index: string): Promise<IndexDeclaration> {
    validateStoreName(store);
    const entry = await (await this.#bucket(GRAMMARS.metaBucket)).get(metaIndex(store, index));
    if (!entry || entry.operation !== "PUT") {
      throw new NotFoundError(`index is not declared: ${index} in store ${store}`);
    }
    return entry.json<IndexDeclaration>();
  }

  // --- collections at rest: KV scans ----------------------------------------

  /** The account's stores. */
  async *listStores(): AsyncGenerator<string> {
    for await (const key of this.#keys(GRAMMARS.metaBucket, "log.*.config")) {
      yield key.slice("log.".length, -".config".length);
    }
  }

  /** A store's types. */
  async *listTypes(store: string): AsyncGenerator<string> {
    validateStoreName(store);
    const prefix = metaStoreType(store, "");
    for await (const key of this.#keys(GRAMMARS.metaBucket, `${prefix}*`)) {
      yield key.slice(prefix.length);
    }
  }

  /** A store's indexes and their declarations. */
  async *listIndexes(store: string): AsyncGenerator<ListIndexesItem> {
    validateStoreName(store);
    const prefix = metaIndex(store, "");
    const meta = await this.#bucket(GRAMMARS.metaBucket);
    for await (const key of this.#keys(GRAMMARS.metaBucket, `${prefix}*`)) {
      const entry = await meta.get(key);
      if (!entry || entry.operation !== "PUT") {
        continue;
      }
      const decl = entry.json<IndexDeclaration>();
      yield {
        name: key.slice(prefix.length),
        kind: decl.kind,
        ...(decl.config ? { config: decl.config } : {}),
      };
    }
  }

  /** The account's members and service accounts; `kind` tells them apart. */
  async *listMembers(): AsyncGenerator<ListMembersItem> {
    const prefix = metaMember("");
    const meta = await this.#bucket(GRAMMARS.metaBucket);
    for await (const key of this.#keys(GRAMMARS.metaBucket, `${prefix}*`)) {
      const name = key.slice(prefix.length);
      const entry = await meta.get(key);
      if (name === "" || !entry || entry.operation !== "PUT") {
        continue;
      }
      const m = entry.json<Pick<ListMembersItem, "role" | "kind" | "public_key" | "github_id">>();
      yield {
        name,
        role: m.role,
        kind: m.kind ?? "member",
        ...(m.public_key ? { public_key: m.public_key } : {}),
        ...(m.github_id ? { github_id: m.github_id } : {}),
      };
    }
  }

  /**
   * A store's instances from its state: every one at every level, or
   * narrowed by type, to a parent's direct children, and by a field
   * filter — a bucket scan with the filter on this side (decision 0045).
   */
  async *listInstances(store: string, opts: ListInstancesOptions = {}): AsyncGenerator<ListInstancesItem> {
    validateStoreName(store);
    const underTail = opts.under === undefined ? undefined : pathTail(opts.under);
    const underDepth = underTail === undefined ? 0 : underTail.split(TAIL_SEPARATOR).length;
    // Under a parent, the parent's children map names each child's type —
    // what the listing shows — and, when a type is asked for, which child
    // names carry it.
    let childTypes: Record<string, string> = {};
    let childNames: Set<string> | undefined;
    if (underTail !== undefined) {
      const res = await this.#resolveTail(store, underTail);
      if (res.kind === "typed") {
        childTypes = res.record.children ?? {};
      }
      if (opts.type !== undefined) {
        childNames = new Set(
          Object.entries(childTypes)
            .filter(([, t]) => t === opts.type)
            .map(([name]) => name),
        );
      }
    }
    const kv = await this.#bucket(stateBucket(store));
    const where = Object.entries(opts.where ?? {});
    for await (const key of this.#keys(stateBucket(store), ">")) {
      if (key === GRAMMARS.stateFoldKey) {
        continue;
      }
      const toks = key.split(TAIL_SEPARATOR);
      if (underTail !== undefined) {
        if (!key.startsWith(`${underTail}${TAIL_SEPARATOR}`) || toks.length !== underDepth + 2) {
          continue;
        }
        if (childNames !== undefined && !childNames.has(toks[toks.length - 2] ?? "")) {
          continue;
        }
      } else if (opts.type !== undefined && (toks.length !== 2 || toks[0] !== opts.type)) {
        continue;
      }
      const entry = await kv.get(key);
      if (!entry || entry.operation !== "PUT") {
        continue;
      }
      const sv = entry.json<StateValue>();
      if (where.length > 0 && !matches(sv.state, where)) {
        continue;
      }
      const type =
        underTail !== undefined ? childTypeOf(childTypes, toks[toks.length - 2] ?? "") : (toks[0] ?? "");
      yield { path: tailPath(key), type, seq: sv.seq, state: sv.state };
    }
  }

  /** The keys at rest under a filter, then done. */
  async *#keys(bucket: string, filter: string): AsyncGenerator<string> {
    const kv = await this.#bucket(bucket);
    const keys = await kv.keys(filter);
    try {
      for await (const key of keys) {
        yield key;
      }
    } finally {
      keys.stop();
    }
  }

  async #bucket(name: string): Promise<KV> {
    let kv = this.#buckets.get(name);
    if (kv === undefined) {
      kv = this.#kvm.open(name);
      kv.catch(() => this.#buckets.delete(name));
      this.#buckets.set(name, kv);
    }
    return kv;
  }

  // --- history: ordered consumers --------------------------------------------

  /** An instance's history, from its first operation to the head observed at the start. */
  history(store: string, path: string): AsyncGenerator<Op> {
    validateStoreName(store);
    return this.#ops(store, [opsSubject(store, pathTail(path))], { bounded: true });
  }

  /**
   * An instance's history and its children's, in one order, to the head
   * observed at the start (decision 0049): a parent with what lives under it.
   */
  historyUnder(store: string, path: string): AsyncGenerator<Op> {
    validateStoreName(store);
    const subject = opsSubject(store, pathTail(path));
    return this.#ops(store, [subject, `${subject}.>`], { bounded: true });
  }

  /** An instance's operations after a sequence, to the head observed at the start. */
  foldTail(store: string, path: string, after: number): AsyncGenerator<Op> {
    validateStoreName(store);
    return this.#ops(store, [opsSubject(store, pathTail(path))], { bounded: true, after });
  }

  /**
   * The live tail of a store, or of one instance when path is not empty —
   * with its children when asked (decision 0049); never ends on its own.
   */
  tail(store: string, path = "", opts: TailOptions = {}): AsyncGenerator<Op> {
    validateStoreName(store);
    let subjects: string[];
    if (path === "") {
      subjects = [opsFilter(store)];
    } else {
      const subject = opsSubject(store, pathTail(path));
      subjects = opts.children ? [subject, `${subject}.>`] : [subject];
    }
    return this.#ops(store, subjects, { ...opts, bounded: false });
  }

  async *#ops(
    store: string,
    subjects: string[],
    o: { bounded: boolean; after?: number; live?: boolean; signal?: AbortSignal },
  ): AsyncGenerator<Op> {
    const consumer = await this.#js.consumers.get(streamName(store), {
      filter_subjects: subjects,
      ...(o.live
        ? { deliver_policy: DeliverPolicy.New }
        : o.after !== undefined && o.after > 0
          ? { deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: o.after + 1 }
          : {}),
    });
    try {
      for await (const op of o.bounded ? bounded(consumer) : unbounded(consumer, o.signal)) {
        yield await this.#verify(op);
      }
    } finally {
      await consumer.delete().catch(() => undefined);
    }
  }

  // --- the live surface: KV watches ----------------------------------------------

  /** An instance's state: the current value, then every change; never ends on its own. */
  async *watch(store: string, path: string, opts: LiveOptions = {}): AsyncGenerator<StateValue> {
    validateStoreName(store);
    const tail = pathTail(path);
    const kv = await this.#bucket(stateBucket(store));
    const w = await kv.watch({ key: tail, ignoreDeletes: true });
    const stop = () => {
      w.stop();
    };
    opts.signal?.addEventListener("abort", stop, { once: true });
    try {
      if (opts.signal?.aborted) {
        return;
      }
      for await (const entry of w) {
        yield entry.json<StateValue>();
      }
    } finally {
      opts.signal?.removeEventListener("abort", stop);
      w.stop();
    }
  }

  /** A store's type records and index declarations: every one at rest, then every change. */
  async *watchDeclarations(store: string, opts: LiveOptions = {}): AsyncGenerator<Declaration> {
    validateStoreName(store);
    const typePrefix = metaStoreType(store, "");
    const indexPrefix = metaIndex(store, "");
    const kv = await this.#bucket(GRAMMARS.metaBucket);
    const w = await kv.watch({ key: [`${typePrefix}*`, `${indexPrefix}*`] });
    const stop = () => {
      w.stop();
    };
    opts.signal?.addEventListener("abort", stop, { once: true });
    try {
      if (opts.signal?.aborted) {
        return;
      }
      for await (const entry of w) {
        const isType = entry.key.startsWith(typePrefix);
        const deleted = entry.operation !== "PUT";
        yield {
          kind: isType ? "type" : "index",
          name: entry.key.slice((isType ? typePrefix : indexPrefix).length),
          revision: entry.revision,
          ...(deleted ? { deleted: true } : { value: entry.json<unknown>() }),
        };
      }
    } finally {
      opts.signal?.removeEventListener("abort", stop);
      w.stop();
    }
  }
}

/** A payload as bytes: raw bytes pass as they are, anything else is JSON. */
function toBytes(payload: unknown): Uint8Array {
  return payload instanceof Uint8Array ? payload : encoder.encode(JSON.stringify(payload ?? {}));
}

const isRecord = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

function parseJSON(raw: Uint8Array): { ok: true; value: unknown } | { ok: false; detail: string } {
  try {
    return { ok: true, value: JSON.parse(decoder.decode(raw)) };
  } catch (err) {
    return { ok: false, detail: err instanceof Error ? err.message : String(err) };
  }
}

/**
 * A child's type in a listing under a parent: the type the parent's
 * children map declares for the child's name, else the name itself — the
 * grammar's own answer when the parent is untyped or the name is undeclared.
 */
function childTypeOf(childTypes: Record<string, string>, name: string): string {
  return childTypes[name] ?? name;
}

/** Every where clause holds on the state: the field exists and renders to the value's text. */
function matches(state: unknown, where: [string, string | number | boolean | null][]): boolean {
  if (!isRecord(state)) {
    return false;
  }
  for (const [field, value] of where) {
    let cur: unknown = state;
    for (const seg of field.split(".")) {
      if (!isRecord(cur) || !(seg in cur)) {
        return false;
      }
      cur = cur[seg];
    }
    if (isRecord(cur) || Array.isArray(cur)) {
      return false;
    }
    if (String(cur) !== String(value)) {
      return false;
    }
  }
  return true;
}

function guardRefused(err: unknown): boolean {
  return err instanceof JetStreamApiError && err.code === JetStreamApiCodes.StreamWrongLastSequence;
}

/** An op as JetStream delivered it; verified is the client's to set. */
function toOp(m: JsMsg): Op {
  const h: MsgHdrs | undefined = m.headers;
  const values = (name: string): string[] => h?.values(name) ?? [];
  return {
    ...parseOpHeaders(values),
    ...parseAttestation(values),
    subject: m.subject,
    seq: m.seq,
    payload: m.data,
    verified: false,
  };
}

/** Delivers the ops pending when it starts, then ends. */
async function* bounded(consumer: Consumer): AsyncGenerator<Op> {
  const pending = (await consumer.info()).num_pending;
  const deadline = Date.now() + REPLAY_BUDGET_MS;
  for (let delivered = 0; delivered < pending; delivered++) {
    let msg = await consumer.next({ expires: REPLAY_WAIT_MS });
    while (msg === null) {
      if (Date.now() > deadline) {
        throw new Error("history: timed out waiting for the next operation");
      }
      if ((await consumer.info()).num_pending === 0) {
        return;
      }
      msg = await consumer.next({ expires: REPLAY_WAIT_MS });
    }
    yield toOp(msg);
  }
}

/** Delivers every op as it lands, until the consumer stops or the signal aborts. */
async function* unbounded(consumer: Consumer, signal?: AbortSignal): AsyncGenerator<Op> {
  if (signal?.aborted) {
    return;
  }
  const messages = await consumer.consume();
  const stop = () => {
    void messages.close();
  };
  signal?.addEventListener("abort", stop, { once: true });
  try {
    for await (const m of messages) {
      yield toOp(m);
    }
  } finally {
    signal?.removeEventListener("abort", stop);
    await messages.close();
  }
}
