// Instance resolution (decisions 0021 § 4, 0022 § 2) and the fold step
// (design 03 § the state buckets) — the rules a state reader and a
// preflight share, ported from the Go contract's resolve.go and judge.go,
// in the words of decision 0044.
import type { FoldDecision } from "../generated/contract.js";
import { mergePatch } from "./merge.js";
import { SNAPSHOT, type TypeRecord } from "./record.js";
import { compileSchema } from "./schema.js";

/** How an instance's stored tail resolves against the store's types. */
export type Resolution =
  | { kind: "typed"; typeName: string; record: TypeRecord }
  | { kind: "untyped"; detail: string }
  | { kind: "undeclared"; detail: string };

/** Looks a type up by name; undefined when the store defines none. */
export type TypeLookup = (name: string) => TypeRecord | undefined | Promise<TypeRecord | undefined>;

/**
 * Resolves an instance's stored tail: alternating name.id pairs, the first
 * against the store's types, each further pair through the current type's
 * children map.
 */
export async function resolveInstance(tail: string, lookup: TypeLookup): Promise<Resolution> {
  const toks = tail.split(".");
  if (toks.length % 2 !== 0) {
    return { kind: "untyped", detail: "the path does not read as type/id pairs" };
  }
  let name = toks[0] ?? "";
  let record = await lookup(name);
  if (record === undefined) {
    return { kind: "untyped", detail: `no type ${JSON.stringify(name)} is defined` };
  }
  for (let i = 2; i < toks.length; i += 2) {
    const child = toks[i] ?? "";
    const childType = record.children?.[child];
    if (childType === undefined) {
      return {
        kind: "undeclared",
        detail: `type ${JSON.stringify(name)} declares no child ${JSON.stringify(child)}`,
      };
    }
    record = await lookup(childType);
    if (record === undefined) {
      return {
        kind: "undeclared",
        detail: `child type ${JSON.stringify(childType)} (${JSON.stringify(child)} of ${JSON.stringify(name)}) is not defined`,
      };
    }
    name = childType;
  }
  return { kind: "typed", typeName: name, record };
}

/** Parses JSON, or returns undefined with the reason. */
function parseJSON(raw: Uint8Array | string): { ok: true; value: unknown } | { ok: false; detail: string } {
  try {
    const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
    return { ok: true, value: JSON.parse(text) };
  } catch (err) {
    return { ok: false, detail: err instanceof Error ? err.message : String(err) };
  }
}

/** Judges a snapshot's state against the type's schema: "" when it passes or there is none. */
export function judgeSnapshot(record: TypeRecord | undefined, state: unknown): string {
  if (record?.schema === undefined) {
    return "";
  }
  let validate;
  try {
    validate = compileSchema(record.schema);
  } catch (err) {
    return `the type's schema does not compile: ${err instanceof Error ? err.message : String(err)}`;
  }
  if (state === undefined) {
    return "state is not JSON: absent";
  }
  const detail = validate(state);
  return detail === "" ? "" : `state fails the type's schema: ${detail}`;
}

/** What one op's record says about it: the decision and why. */
export interface Judgement {
  decision: FoldDecision;
  detail: string;
}

/** Judges an op under its type record: whether it is defined, valid and which effect it has. */
export function judgeRecord(record: TypeRecord, op: { type: string; payload: Uint8Array | string }): Judgement {
  const def = record.operations?.[op.type];
  if (def === undefined) {
    return { decision: "unknown-type", detail: `the type defines no operation ${op.type}` };
  }
  let validate;
  try {
    validate = compileSchema(def.schema);
  } catch (err) {
    return {
      decision: "bad-type-record",
      detail: `compile operation schema: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
  const parsed = parseJSON(op.payload);
  if (!parsed.ok) {
    return { decision: "invalid", detail: `payload is not JSON: ${parsed.detail}` };
  }
  const failed = validate(parsed.value);
  if (failed !== "") {
    return { decision: "invalid", detail: `payload fails its schema: ${failed}` };
  }
  const effect = def.effect === undefined || def.effect === "" ? "none" : def.effect;
  switch (effect) {
    case "none":
      return { decision: "none", detail: "" };
    case "merge":
      return { decision: "merge", detail: "" };
    default:
      return { decision: "unknown-effect", detail: `effect ${JSON.stringify(effect)} is outside this build's vocabulary` };
  }
}

/** One fold step's outcome: the decision, the state after, and whether it moved. */
export interface FoldOutcome extends Judgement {
  /** The state after the step; undefined when the instance has none. */
  state: unknown;
  moved: boolean;
}

/** Parses a snapshot payload; undefined when it is not one. */
function parseSnapshot(payload: Uint8Array | string): { state: unknown; frontier: string[] } | string {
  const parsed = parseJSON(payload);
  if (!parsed.ok) {
    return `snapshot payload: ${parsed.detail}`;
  }
  const v = parsed.value;
  if (typeof v !== "object" || v === null || Array.isArray(v)) {
    return "snapshot payload: not an object";
  }
  const obj = v as Record<string, unknown>;
  const frontier = obj.frontier ?? [];
  if (!Array.isArray(frontier) || !frontier.every((f) => typeof f === "string")) {
    return "snapshot payload: frontier is not a list of op IDs";
  }
  return { state: obj.state, frontier };
}

/**
 * Applies one op to an instance's state under its resolution: an
 * undeclared child is marked and moves nothing; a snapshot resets state to
 * its payload's, judged against the type's schema when typed; a declared
 * merge applies RFC 7386 onto current state, empty included; none, an
 * unknown type, an unknown effect, a bad record or an invalid payload moves
 * nothing; an untyped subject moves only by snapshot.
 */
export function foldStep(
  res: Resolution,
  state: unknown,
  op: { type: string; payload: Uint8Array | string },
): FoldOutcome {
  if (res.kind === "undeclared") {
    return { decision: "undeclared", detail: res.detail, state, moved: false };
  }
  if (op.type === SNAPSHOT) {
    const snap = parseSnapshot(op.payload);
    if (typeof snap === "string") {
      return { decision: "malformed-snapshot", detail: snap, state, moved: false };
    }
    if (res.kind === "typed") {
      const detail = judgeSnapshot(res.record, snap.state);
      if (detail !== "") {
        return { decision: "malformed-snapshot", detail, state, moved: false };
      }
    }
    return { decision: "reset", detail: "", state: snap.state, moved: true };
  }
  if (res.kind !== "typed") {
    return { decision: "unknown-type", detail: `instance is untyped: ${res.detail}`, state, moved: false };
  }
  const judged = judgeRecord(res.record, op);
  if (judged.decision !== "merge") {
    return { ...judged, state, moved: false };
  }
  const parsed = parseJSON(op.payload);
  if (!parsed.ok) {
    return { decision: "invalid", detail: `merge failed: ${parsed.detail}`, state, moved: false };
  }
  return { decision: "merge", detail: "", state: mergePatch(state, parsed.value), moved: true };
}

/**
 * The retry judgement after an expected-sequence refusal: the guard fires
 * before dedup, so a retried guarded write is refused even when its first
 * attempt landed. Equal op IDs mean it landed; anything else means the
 * instance moved.
 */
export function guardRetryLanded(lastOpID: string, retriedOpID: string): boolean {
  return lastOpID !== "" && lastOpID === retriedOpID;
}
