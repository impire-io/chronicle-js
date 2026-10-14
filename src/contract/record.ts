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
  /** The node's public key, on an operation it attested (decision 0057). */
  attester: "Op-Attester",
  /** The node's signature over the record's signed form, base64. */
  signature: "Op-Signature",
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
  /** Op-Attester: the public key of the node that attested the author; empty on a direct write (decision 0057). */
  attester: string;
  /** Op-Signature: the node's signature over the signed form, base64; empty on a direct write. */
  signature: string;
  /** The exact subject the op lives on; empty before it is written. */
  subject: string;
  /** The stream sequence — the only order; 0 before it is written. */
  seq: number;
  /** The payload, data only. */
  payload: Uint8Array;
  /**
   * The reader's finding, never a header: the signature verifies against
   * an attester key the store's META lists. False for every operation
   * written directly.
   */
  verified: boolean;
}

/** The fields an op's record headers carry. */
export type OpHeaderFields = Pick<Op, "id" | "type" | "author" | "parents" | "ts" | "version">;

/** Reads the attestation headers back; both empty on a direct write. */
export function parseAttestation(values: (name: string) => string[]): Pick<Op, "attester" | "signature"> {
  return { attester: values(HDR.attester)[0] ?? "", signature: values(HDR.signature)[0] ?? "" };
}

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
  /** The store allows schedules and per-message TTLs (decision 0050). */
  schedules?: boolean;
  /** The store requires verified authors (decision 0057): writes go through chronicle, and an operation written any other way is kept but takes no effect. */
  verified_authors?: boolean;
}
