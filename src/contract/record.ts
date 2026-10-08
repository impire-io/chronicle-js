// The operation record on the wire (design 02 § the operation record): an
// op is its headers plus a data-only payload, and the records chronicle
// keeps at rest.
import { HEADERS } from "../generated/contract.js";

/** Header names of the operation record. */
export const HDR = {
  msgId: "Nats-Msg-Id",
  type: "Op-Type",
  author: "Op-Author",
  parents: "Op-Parents",
  ts: "Op-Ts",
  version: "Op-Version",
  expectedLastSubjectSeq: "Nats-Expected-Last-Subject-Sequence",
  rollup: "Nats-Rollup",
  chunk: "Chron-Chunk",
  end: "Chron-End",
  serviceErrorCode: "Nats-Service-Error-Code",
  serviceError: "Nats-Service-Error",
} as const;

/** The envelope version an op carries when none is set. */
export const ENVELOPE_VERSION = HEADERS.envelopeVersion;
/** The only Nats-Rollup value chronicle ever sends. */
export const ROLLUP_SUBJECT = HEADERS.rollupSubject;
/** The op type of a snapshot. */
export const SNAPSHOT = "snapshot";

/** One operation as it lives in a log. */
export interface Op {
  /** Nats-Msg-Id: the op ID and the dedup key. */
  id: string;
  /** Op-Type: the operation's kind in the log's vocabulary. */
  type: string;
  /** Op-Author: the principal that wrote it. */
  author: string;
  /** Op-Parents: the op IDs the writer had seen. */
  parents: string[];
  /** Op-Ts: the author-claimed clock, RFC 3339; informational only. Empty when unset. */
  ts: string;
  /** Op-Version: the envelope version. */
  version: string;
  /** The exact subject the op lives on; empty before it is written. */
  subject: string;
  /** The stream sequence — the only order; 0 before it is written. */
  seq: number;
  /** The payload, data only. */
  payload: Uint8Array;
}

/** The fields an op's headers carry. */
export type OpHeaderFields = Pick<Op, "id" | "type" | "author" | "parents" | "ts" | "version">;

/**
 * The headers an op is published with, in order, one entry per value:
 * Op-Parents repeats once per parent; an empty ts sets no Op-Ts; an empty
 * version is the envelope version.
 */
export function opHeaders(op: OpHeaderFields): [string, string][] {
  const out: [string, string][] = [
    [HDR.msgId, op.id],
    [HDR.type, op.type],
    [HDR.author, op.author],
  ];
  for (const p of op.parents) {
    out.push([HDR.parents, p]);
  }
  if (op.ts !== "") {
    out.push([HDR.ts, op.ts]);
  }
  out.push([HDR.version, op.version === "" ? ENVELOPE_VERSION : op.version]);
  return out;
}

/** Reads an op's header fields back; values(name) returns every value of a header. */
export function parseOpHeaders(values: (name: string) => string[]): OpHeaderFields {
  const first = (name: string): string => values(name)[0] ?? "";
  return {
    id: first(HDR.msgId),
    type: first(HDR.type),
    author: first(HDR.author),
    parents: values(HDR.parents),
    ts: first(HDR.ts),
    version: first(HDR.version),
  };
}

/**
 * Formats a clock as RFC 3339 in UTC with the fraction's trailing zeros
 * dropped — what Go's time.RFC3339Nano writes, so ops read the same
 * whichever SDK wrote them.
 */
export function rfc3339(date: Date): string {
  const iso = date.toISOString();
  const [whole, frac = ""] = iso.slice(0, -1).split(".");
  const trimmed = frac.replace(/0+$/, "");
  return `${whole ?? ""}${trimmed === "" ? "" : `.${trimmed}`}Z`;
}

/** An instance's state as the state bucket holds it. */
export interface StateValue {
  /** The last op the state covers. */
  seq: number;
  /** The folded state. */
  state: unknown;
}

/** A snapshot op's payload. */
export interface Snapshot {
  state: unknown;
  frontier: string[];
}

/** An operation a type defines. */
export interface OpDef {
  /** The JSON Schema the payload must pass. */
  schema: unknown;
  /** What applying it does to the state: merge (updates it) or none (recorded in history only). Always stated; absent reads as none. */
  effect?: string;
}

/** A type record, as META holds it under log.<store>.type.<type>. */
export interface TypeRecord {
  revision: number;
  /** The instance's JSON Schema; absent means no constraint. */
  schema?: unknown;
  /** The history policy: compactable (the default) or full. */
  history?: string;
  /** Child name → the type of the instances nested under that name. */
  children?: Record<string, string>;
  operations?: Record<string, OpDef>;
}

/** An index declaration, as META holds it under index.<store>.<index>. */
export interface IndexDeclaration {
  kind: string;
  config?: unknown;
}

/** A store's settings, as META holds them under log.<store>.config. */
export interface StoreConfig {
  status: string;
  description?: string;
  /** The store's byte budget; absent means the account's default. */
  max_bytes?: number;
  /** The history policy: compactable (the default) or full. */
  history?: string;
}
