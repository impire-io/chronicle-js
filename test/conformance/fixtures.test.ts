// The golden fixtures (conformance/fixtures): JSON in, JSON out, for every
// pure rule the SDK re-implements, asserted against the SDK's own
// functions. The table in the suite's README names each file's rule.
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  foldStep,
  guardRetryLanded,
  isValidName,
  mergePatch,
  opHeaders,
  parseOpHeaders,
  resolveInstance,
  type NameKind,
  type OpHeaderFields,
  type TypeRecord,
} from "../../src/index.js";
import * as names from "../../src/contract/names.js";
import { CONTRACT_VERSION } from "../../src/index.js";
import { releaseDir, suiteFile } from "./suite.js";

describe("the contract artifact", () => {
  it("is the version this SDK declares, byte for byte the release's", () => {
    const vendored = readFileSync("contract/sdk-contract.json", "utf8");
    const released = readFileSync(`${releaseDir}/contract/sdk-contract.json`, "utf8");
    expect(vendored).toBe(released);
    const pkg = JSON.parse(readFileSync("package.json", "utf8")) as { chronicle: { contract: string } };
    expect(CONTRACT_VERSION).toBe(pkg.chronicle.contract);
  });
});

describe("names.json", () => {
  const { cases } = suiteFile<{ cases: { kind: NameKind; input: string; valid: boolean }[] }>(
    "fixtures",
    "names.json",
  );
  it.each(cases)("$kind $input → $valid", ({ kind, input, valid }) => {
    expect(isValidName(kind, input)).toBe(valid);
  });
});

describe("derivations.json", () => {
  interface Case {
    store: string;
    tail: string;
    path: string;
    upper: string;
    stream: string;
    stateBucket: string;
    storeSubjects: string;
    opsFilter: string;
    opsSubject: string;
    opsPrefix: string;
    instanceFromSubject: string;
    metaStoreConfig: string;
    metaType: { type: string; key: string };
    metaIndex: { index: string; key: string };
  }
  const fx = suiteFile<{
    cases: Case[];
    identity: Record<"principal" | "member", { id: string; key: string }> & {
      invite: { digest: string; key: string };
    };
  }>("fixtures", "derivations.json");
  it.each(fx.cases)("$store / $path", (c) => {
    expect(names.pathTail(c.path)).toBe(c.tail);
    expect(names.tailPath(c.tail)).toBe(c.path);
    expect(names.upperStore(c.store)).toBe(c.upper);
    expect(names.streamName(c.store)).toBe(c.stream);
    expect(names.stateBucket(c.store)).toBe(c.stateBucket);
    expect(names.storeSubjects(c.store)).toBe(c.storeSubjects);
    expect(names.opsFilter(c.store)).toBe(c.opsFilter);
    expect(names.opsSubject(c.store, c.tail)).toBe(c.opsSubject);
    expect(names.opsPrefix(c.store)).toBe(c.opsPrefix);
    expect(names.instanceFromSubject(c.store, c.opsSubject)).toBe(c.instanceFromSubject);
    expect(names.metaStoreConfig(c.store)).toBe(c.metaStoreConfig);
    expect(names.metaStoreType(c.store, c.metaType.type)).toBe(c.metaType.key);
    expect(names.metaIndex(c.store, c.metaIndex.index)).toBe(c.metaIndex.key);
  });
  it("identity keys", () => {
    expect(names.metaPrincipal(fx.identity.principal.id)).toBe(fx.identity.principal.key);
    expect(names.metaMember(fx.identity.member.id)).toBe(fx.identity.member.key);
    expect(names.metaInvite(fx.identity.invite.digest)).toBe(fx.identity.invite.key);
  });
});

describe("headers.json", () => {
  const { cases } = suiteFile<{
    cases: { name: string; op: OpHeaderFields; headers: Record<string, string[]> }[];
  }>("fixtures", "headers.json");
  it.each(cases)("$name", ({ op, headers }) => {
    const got: Record<string, string[]> = {};
    for (const [k, v] of opHeaders(op)) {
      (got[k] ??= []).push(v);
    }
    expect(got).toEqual(headers);
    const back = parseOpHeaders((name) => headers[name] ?? []);
    expect(back).toEqual({ ...op, version: op.version === "" ? "1" : op.version });
  });
});

describe("merge.json", () => {
  const { cases } = suiteFile<{ cases: { target: unknown; patch: unknown; result: unknown }[] }>(
    "fixtures",
    "merge.json",
  );
  it.each(cases.map((c, i) => ({ ...c, i })))("case $i", ({ target, patch, result }) => {
    expect(mergePatch(target, patch)).toEqual(result);
  });
});

type Types = Record<string, TypeRecord>;
const lookupIn = (types: Types) => (name: string) => types[name];

describe("resolve.json", () => {
  const fx = suiteFile<{ types: Types; cases: { tail: string; kind: string; type?: string }[] }>(
    "fixtures",
    "resolve.json",
  );
  it.each(fx.cases)("$tail → $kind", async ({ tail, kind, type }) => {
    const res = await resolveInstance(tail, lookupIn(fx.types));
    expect(res.kind).toBe(kind);
    if (res.kind === "typed") {
      expect(res.typeName).toBe(type);
    }
  });
});

describe("fold.json", () => {
  interface Case {
    name: string;
    tail: string;
    ops: { type: string; payload?: unknown; raw?: string }[];
    decisions: string[];
    state: unknown;
  }
  const fx = suiteFile<{ types: Types; cases: Case[] }>("fixtures", "fold.json");
  it.each(fx.cases)("$name", async ({ tail, ops, decisions, state }) => {
    const res = await resolveInstance(tail, lookupIn(fx.types));
    let current: unknown = undefined;
    const got: string[] = [];
    for (const op of ops) {
      const payload = op.raw ?? JSON.stringify(op.payload);
      const out = foldStep(res, current, { type: op.type, payload });
      got.push(out.decision);
      current = out.state;
    }
    expect(got).toEqual(decisions);
    expect(current ?? null).toEqual(state);
  });
});

describe("guard-retry.json", () => {
  const { cases } = suiteFile<{
    cases: { name: string; lastOpID: string; retriedOpID: string; landed: boolean }[];
  }>("fixtures", "guard-retry.json");
  it.each(cases)("$name", ({ lastOpID, retriedOpID, landed }) => {
    expect(guardRetryLanded(lastOpID, retriedOpID)).toBe(landed);
  });
});
