// The client: one account's sentences over one NATS connection — the SDK
// contract (design 12) as the Go client implements it, in TypeScript.
// Control verbs are request/reply; queries are streamed replies; appends
// are guarded JetStream publishes; reads of data at rest are JetStream —
// ordered consumers, KV scans and watches — never a node verb.
import {
  credsAuthenticator,
  headers as natsHeaders,
  nkeyAuthenticator,
  nuid,
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
  NotFoundError,
  SchemaViolationError,
  StaleVersionError,
  ThingExistsError,
  ThingMovedError,
  UndeclaredAspectError,
  UndefinedOperationError,
} from "./errors.js";
import { guardRetryLanded, resolveTail, type Resolution } from "./contract/fold.js";
import {
  metaIndex,
  metaLogType,
  metaMember,
  opsFilter,
  opsSubject,
  stateBucket,
  streamName,
  validateLogName,
  validateThing,
  validateTypeName,
} from "./contract/names.js";
import {
  HDR,
  opHeaders,
  parseOpHeaders,
  rfc3339,
  ROLLUP_SUBJECT,
  SNAPSHOT,
  type IndexDeclaration,
  type Op,
  type OpDef,
  type StateValue,
  type TypeRecord,
} from "./contract/record.js";
import { compileSchema } from "./contract/schema.js";
import {
  GRAMMARS,
  SUBJECTS,
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
  type ListIndexesItem,
  type ListMembersItem,
  type LogCreateReply,
  type MemberAddReply,
  type MemberRevokeReply,
  type PingReply,
  type ThingRollupReply,
  type TypeDefineReply,
  type WatchDeclarationsItem,
} from "./generated/contract.js";
import { request, requestStream, type CallOptions, type Streamed } from "./rpc.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** How long a bounded replay waits for each op, and for the whole replay. */
const REPLAY_WAIT_MS = 1_000;
const REPLAY_BUDGET_MS = 20_000;

/** How to reach chronicle and who to be. */
export interface ConnectOptions {
  /** The websocket URL(s): wss://connect.chronicle.impire.dev, or `chronicle up`'s ws:// one. */
  servers: string | string[];
  /** A .creds file's contents; the principal is its user JWT's name. */
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

/** The principal a .creds file's user JWT names. */
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
export interface AppendOptions {
  /** The op IDs the writer had seen. */
  parents?: string[];
  /** The op ID; minted when absent, and kept across a retry. */
  opId?: string;
  /** Guard the append: the thing's last op must be this sequence. */
  expectedSeq?: number;
}

/** A type's definition (design 07 § types). */
export interface TypeDefinition {
  schema: unknown;
  history?: string;
  aspects?: Record<string, string>;
  operations?: Record<string, OpDef>;
}

/** Options for creating a log. */
export interface LogOptions {
  description?: string;
  /** compactable (the default) or preserved. */
  history?: string;
  /** The log's byte budget; absent means the account's default. */
  maxBytes?: number;
}

/** Where a tail starts. */
export interface TailOptions {
  /** Start after this sequence. */
  after?: number;
  /** Only what lands from now on. */
  live?: boolean;
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

/** A graph query: neighbors or walk, without the fields the SDK fills. */
export type GraphQuery<R> = Omit<R, "principal" | "op">;

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
  #jsm: Promise<JetStreamManager> | undefined;

  private constructor(nc: NatsConnection, author: string) {
    this.#nc = nc;
    this.#author = author;
    this.#js = jetstream(nc);
    this.#kvm = new Kvm(this.#js);
  }

  /** Connects over a websocket. The principal is the creds' name, or `author`. */
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

  /** The principal every write and request names. */
  get author(): string {
    return this.#author;
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
    return request(this.#nc, SUBJECTS.ping, {}, opts);
  }

  /** Creates a log. */
  createLog(log: string, opts: LogOptions = {}): Promise<LogCreateReply> {
    return request(this.#nc, SUBJECTS["log.create"], {
      principal: this.#author,
      log,
      ...(opts.description ? { description: opts.description } : {}),
      ...(opts.history ? { history: opts.history } : {}),
      ...(opts.maxBytes ? { max_bytes: opts.maxBytes } : {}),
    });
  }

  /** Defines, or redefines, a type in a log. */
  defineType(log: string, type: string, def: TypeDefinition): Promise<TypeDefineReply> {
    return request(this.#nc, SUBJECTS["type.define"], {
      principal: this.#author,
      log,
      type,
      schema: def.schema,
      ...(def.history ? { history: def.history } : {}),
      ...(def.aspects ? { aspects: def.aspects } : {}),
      ...(def.operations ? { operations: def.operations } : {}),
    });
  }

  /** Declares an index on a log. */
  declareIndex(log: string, index: string, kind: string, config?: unknown): Promise<IndexDeclareReply> {
    return request(this.#nc, SUBJECTS["index.declare"], {
      principal: this.#author,
      log,
      index,
      kind,
      ...(config === undefined || config === null ? {} : { config }),
    });
  }

  /** Deletes an index declaration. */
  deleteIndex(log: string, index: string): Promise<IndexDeleteReply> {
    return request(this.#nc, SUBJECTS["index.delete"], { principal: this.#author, log, index });
  }

  /** Asks the node to roll a thing's history up into one snapshot; declining is an answer. */
  rollupThing(log: string, thing: string): Promise<ThingRollupReply> {
    return request(this.#nc, SUBJECTS["thing.rollup"], { principal: this.#author, log, thing });
  }

  /** Adds a member to the account. */
  addMember(
    member: string,
    role: string,
    opts: { publicKey?: string; githubId?: number } = {},
  ): Promise<MemberAddReply> {
    return request(this.#nc, SUBJECTS["member.add"], {
      principal: this.#author,
      member,
      role,
      ...(opts.publicKey ? { public_key: opts.publicKey } : {}),
      ...(opts.githubId ? { github_id: opts.githubId } : {}),
    });
  }

  /** Revokes a member. */
  revokeMember(member: string): Promise<MemberRevokeReply> {
    return request(this.#nc, SUBJECTS["member.revoke"], { principal: this.#author, member });
  }

  // --- queries: streamed replies -----------------------------------------

  /** Queries a search index; empty text matches everything. */
  queryIndex(
    log: string,
    index: string,
    query: string,
    opts: QueryOptions = {},
  ): Streamed<IndexQuerySearchItem, IndexQuerySearchTrailer> {
    return requestStream(
      this.#nc,
      querySubject(log, index),
      { principal: this.#author, query, ...(opts.limit ? { limit: opts.limit } : {}) },
      opts,
    );
  }

  /** A thing's edges in a graph index. */
  graphNeighbors(
    log: string,
    index: string,
    q: GraphQuery<IndexQueryGraphNeighborsRequest>,
    opts: { signal?: AbortSignal } = {},
  ): Streamed<IndexQueryGraphNeighborsItem, IndexQueryGraphNeighborsTrailer> {
    return requestStream(
      this.#nc,
      querySubject(log, index),
      { ...q, principal: this.#author, op: "neighbors" },
      opts,
    );
  }

  /** The things reachable from a thing in a graph index. */
  graphWalk(
    log: string,
    index: string,
    q: GraphQuery<IndexQueryGraphWalkRequest>,
    opts: { signal?: AbortSignal } = {},
  ): Streamed<IndexQueryGraphWalkItem, IndexQueryGraphWalkTrailer> {
    return requestStream(
      this.#nc,
      querySubject(log, index),
      { ...q, principal: this.#author, op: "walk" },
      opts,
    );
  }

  /** Queries a semantic index by meaning. */
  querySemantic(
    log: string,
    index: string,
    text: string,
    opts: QueryOptions = {},
  ): Streamed<IndexQuerySemanticItem, IndexQuerySemanticTrailer> {
    return requestStream(
      this.#nc,
      querySubject(log, index),
      { principal: this.#author, text, ...(opts.limit ? { limit: opts.limit } : {}) },
      opts,
    );
  }

  // --- writes: guarded publishes ------------------------------------------

  /** Births a thing by snapshot: its whole state, guarded at 0. */
  async createThing(log: string, thing: string, state: unknown = {}, opts: AppendOptions = {}): Promise<Ack> {
    validateLogName(log);
    validateThing(thing);
    if (opts.expectedSeq !== undefined) {
      throw new Error("expectedSeq: a birth guards at 0 by definition");
    }
    await this.#preflightSnapshot(log, thing, state);
    const payload = encoder.encode(JSON.stringify({ state, frontier: [] }));
    return this.#publish(log, thing, SNAPSHOT, payload, opts, { guard: 0 }, (t) => new ThingExistsError(t));
  }

  /** Births a thing through one of its type's operations, guarded at 0. */
  async createWith(
    log: string,
    thing: string,
    opType: string,
    payload: unknown = {},
    opts: AppendOptions = {},
  ): Promise<Ack> {
    validateLogName(log);
    validateThing(thing);
    if (opType === "") {
      throw new Error("op type: must not be empty");
    }
    if (opType === SNAPSHOT) {
      throw new Error("create with snapshot: createThing is the snapshot birth");
    }
    if (opts.expectedSeq !== undefined) {
      throw new Error("expectedSeq: a birth guards at 0 by definition");
    }
    const data = toBytes(payload);
    await this.#preflightAppend(log, thing, opType, data);
    return this.#publish(log, thing, opType, data, opts, { guard: 0 }, (t) => new ThingExistsError(t));
  }

  /** Appends an operation; with expectedSeq, guarded on the thing's last sequence. */
  async append(
    log: string,
    thing: string,
    opType: string,
    payload: unknown,
    opts: AppendOptions = {},
  ): Promise<Ack> {
    validateLogName(log);
    validateThing(thing);
    if (opType === "") {
      throw new Error("op type: must not be empty");
    }
    const data = toBytes(payload);
    await this.#preflightAppend(log, thing, opType, data);
    return this.#publish(
      log,
      thing,
      opType,
      data,
      opts,
      opts.expectedSeq === undefined ? {} : { guard: opts.expectedSeq },
      (t) => new ThingMovedError(t),
    );
  }

  /** Saves a version: a snapshot replacing the thing's history up to upTo, guarded there. */
  async saveVersion(
    log: string,
    thing: string,
    state: unknown,
    frontier: string[],
    upTo: number,
    opts: AppendOptions = {},
  ): Promise<Ack> {
    validateLogName(log);
    validateThing(thing);
    if (upTo <= 0) {
      throw new Error("upTo: the seq of the last op the state covers; birth is createThing");
    }
    if (opts.expectedSeq !== undefined) {
      throw new Error("expectedSeq: a save guards at upTo");
    }
    await this.#preflightSnapshot(log, thing, state);
    const payload = encoder.encode(JSON.stringify({ state, frontier }));
    return this.#publish(
      log,
      thing,
      SNAPSHOT,
      payload,
      opts,
      { guard: upTo, rollup: true },
      (t) => new StaleVersionError(t),
    );
  }

  /**
   * One publish: the op's headers, the guard, one in flight per subject;
   * after a guard refusal, the subject's last op says whether this op
   * landed (dedup's echo) or the thing moved.
   */
  async #publish(
    log: string,
    thing: string,
    opType: string,
    payload: Uint8Array,
    opts: AppendOptions,
    guard: { guard?: number; rollup?: boolean },
    refused: (what: string) => Error,
  ): Promise<Ack> {
    const subject = opsSubject(log, thing);
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
    return this.#serial(subject, async () => {
      try {
        const ack = await this.#js.publish(subject, payload, { headers: h });
        return { opId, seq: ack.seq };
      } catch (err) {
        if (guard.guard !== undefined && guardRefused(err)) {
          const last = await this.#lastOp(log, subject);
          if (guardRetryLanded(last?.id ?? "", opId)) {
            return { opId, seq: last?.seq ?? 0 };
          }
          throw refused(`${thing} in ${log}`);
        }
        throw err;
      }
    });
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
  async #lastOp(log: string, subject: string): Promise<{ id: string; seq: number } | undefined> {
    this.#jsm ??= jetstreamManager(this.#nc);
    const jsm = await this.#jsm;
    const msg = await jsm.streams.getMessage(streamName(log), { last_by_subj: subject });
    if (msg === null) {
      return undefined;
    }
    return { id: msg.header.get(HDR.msgId), seq: msg.seq };
  }

  // --- preflight: the type's rules, before anything is sent -----------------

  /** Resolves a thing's tail against the log's types. */
  async resolve(log: string, thing: string): Promise<Resolution> {
    validateLogName(log);
    validateThing(thing);
    const meta = await this.#bucket(GRAMMARS.metaBucket);
    return resolveTail(thing, async (name) => {
      const entry = await meta.get(metaLogType(log, name));
      return entry && entry.operation === "PUT" ? entry.json<TypeRecord>() : undefined;
    });
  }

  async #preflightAppend(log: string, thing: string, opType: string, payload: Uint8Array): Promise<void> {
    if (opType === SNAPSHOT) {
      const snap = parseJSON(payload);
      if (snap.ok && isRecord(snap.value)) {
        await this.#preflightSnapshot(log, thing, snap.value.state);
      }
      return;
    }
    const res = await this.resolve(log, thing);
    if (res.kind === "undeclared") {
      throw new UndeclaredAspectError(res.detail);
    }
    if (res.kind !== "typed") {
      return;
    }
    const def = res.record.operations?.[opType];
    if (def === undefined) {
      throw new UndefinedOperationError(
        `type ${JSON.stringify(res.typeName)} defines no operation ${JSON.stringify(opType)}`,
      );
    }
    const parsed = parseJSON(payload);
    if (!parsed.ok) {
      throw new SchemaViolationError(`payload is not JSON: ${parsed.detail}`);
    }
    const failed = compileSchema(def.schema)(parsed.value);
    if (failed !== "") {
      throw new SchemaViolationError(`${log} ${opType}: ${failed}`);
    }
  }

  async #preflightSnapshot(log: string, thing: string, state: unknown): Promise<void> {
    const res = await this.resolve(log, thing);
    if (res.kind === "undeclared") {
      throw new UndeclaredAspectError(res.detail);
    }
    if (res.kind !== "typed" || res.record.schema === undefined) {
      return;
    }
    const failed = compileSchema(res.record.schema)(state);
    if (failed !== "") {
      throw new SchemaViolationError(`${log} ${res.typeName}: state fails the thing schema: ${failed}`);
    }
  }

  // --- single values: replies at rest ---------------------------------------

  /** A thing's folded state and the sequence it covers. */
  async state(log: string, thing: string): Promise<StateValue> {
    validateLogName(log);
    validateThing(thing);
    const entry = await (await this.#bucket(stateBucket(log))).get(thing);
    if (!entry || entry.operation !== "PUT") {
      throw new NotFoundError(`no state for thing: ${thing} in ${log}`);
    }
    return entry.json<StateValue>();
  }

  /** A type's record. */
  async getType(log: string, type: string): Promise<TypeRecord> {
    validateLogName(log);
    validateTypeName(type);
    const entry = await (await this.#bucket(GRAMMARS.metaBucket)).get(metaLogType(log, type));
    if (!entry || entry.operation !== "PUT") {
      throw new NotFoundError(`type is not defined: ${type} in ${log}`);
    }
    return entry.json<TypeRecord>();
  }

  /** An index's declaration. */
  async getIndexDeclaration(log: string, index: string): Promise<IndexDeclaration> {
    validateLogName(log);
    const entry = await (await this.#bucket(GRAMMARS.metaBucket)).get(metaIndex(log, index));
    if (!entry || entry.operation !== "PUT") {
      throw new NotFoundError(`index is not declared: ${index} in ${log}`);
    }
    return entry.json<IndexDeclaration>();
  }

  // --- collections at rest: KV scans ----------------------------------------

  /** The account's logs. */
  async *listLogs(): AsyncGenerator<string> {
    for await (const key of this.#keys(GRAMMARS.metaBucket, "log.*.config")) {
      yield key.slice("log.".length, -".config".length);
    }
  }

  /** A log's types. */
  async *listTypes(log: string): AsyncGenerator<string> {
    validateLogName(log);
    const prefix = metaLogType(log, "");
    for await (const key of this.#keys(GRAMMARS.metaBucket, `${prefix}*`)) {
      yield key.slice(prefix.length);
    }
  }

  /** A log's indexes and their declarations. */
  async *listIndexes(log: string): AsyncGenerator<ListIndexesItem> {
    validateLogName(log);
    const prefix = metaIndex(log, "");
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

  /** The account's members. */
  async *listMembers(): AsyncGenerator<ListMembersItem> {
    const prefix = metaMember("");
    const meta = await this.#bucket(GRAMMARS.metaBucket);
    for await (const key of this.#keys(GRAMMARS.metaBucket, `${prefix}*`)) {
      const name = key.slice(prefix.length);
      const entry = await meta.get(key);
      if (name === "" || !entry || entry.operation !== "PUT") {
        continue;
      }
      const m = entry.json<Pick<ListMembersItem, "role" | "public_key" | "github_id">>();
      yield {
        name,
        role: m.role,
        ...(m.public_key ? { public_key: m.public_key } : {}),
        ...(m.github_id ? { github_id: m.github_id } : {}),
      };
    }
  }

  /** A log's things with state, narrowed to a prefix and its descendants. */
  async *listThings(log: string, prefix = ""): AsyncGenerator<string> {
    validateLogName(log);
    for await (const key of this.#keys(stateBucket(log), ">")) {
      if (key === GRAMMARS.stateFoldKey) {
        continue;
      }
      if (prefix !== "" && key !== prefix && !key.startsWith(`${prefix}.`)) {
        continue;
      }
      yield key;
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

  /** A thing's history, from its first op to the head observed at the start. */
  replay(log: string, thing: string): AsyncGenerator<Op> {
    validateLogName(log);
    validateThing(thing);
    return this.#ops(log, opsSubject(log, thing), { bounded: true });
  }

  /** A thing's ops after a sequence, to the head observed at the start. */
  foldTail(log: string, thing: string, after: number): AsyncGenerator<Op> {
    validateLogName(log);
    validateThing(thing);
    return this.#ops(log, opsSubject(log, thing), { bounded: true, after });
  }

  /** The live tail of a log, or of one thing when thing is not empty; never ends on its own. */
  tail(log: string, thing = "", opts: TailOptions = {}): AsyncGenerator<Op> {
    validateLogName(log);
    if (thing !== "") {
      validateThing(thing);
    }
    return this.#ops(log, thing === "" ? opsFilter(log) : opsSubject(log, thing), {
      ...opts,
      bounded: false,
    });
  }

  async *#ops(
    log: string,
    subject: string,
    o: { bounded: boolean; after?: number; live?: boolean; signal?: AbortSignal },
  ): AsyncGenerator<Op> {
    const consumer = await this.#js.consumers.get(streamName(log), {
      filter_subjects: [subject],
      ...(o.live
        ? { deliver_policy: DeliverPolicy.New }
        : o.after !== undefined && o.after > 0
          ? { deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: o.after + 1 }
          : {}),
    });
    try {
      if (o.bounded) {
        yield* bounded(consumer);
      } else {
        yield* unbounded(consumer, o.signal);
      }
    } finally {
      await consumer.delete().catch(() => undefined);
    }
  }

  // --- the live surface: KV watches ----------------------------------------------

  /** A thing's state: the current value, then every change; never ends on its own. */
  async *watch(log: string, thing: string, opts: LiveOptions = {}): AsyncGenerator<StateValue> {
    validateLogName(log);
    validateThing(thing);
    const kv = await this.#bucket(stateBucket(log));
    const w = await kv.watch({ key: thing, ignoreDeletes: true });
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

  /** A log's type records and index declarations: every one at rest, then every change. */
  async *watchDeclarations(log: string, opts: LiveOptions = {}): AsyncGenerator<Declaration> {
    validateLogName(log);
    const typePrefix = metaLogType(log, "");
    const indexPrefix = metaIndex(log, "");
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

function querySubject(log: string, index: string): string {
  return SUBJECTS["index.query.search"].replace("<log>", log).replace("<index>", index);
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

function guardRefused(err: unknown): boolean {
  return err instanceof JetStreamApiError && err.code === JetStreamApiCodes.StreamWrongLastSequence;
}

/** An op as JetStream delivered it. */
function toOp(m: JsMsg): Op {
  const h: MsgHdrs | undefined = m.headers;
  return {
    ...parseOpHeaders((name) => h?.values(name) ?? []),
    subject: m.subject,
    seq: m.seq,
    payload: m.data,
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
        throw new Error("replay next: timed out");
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
