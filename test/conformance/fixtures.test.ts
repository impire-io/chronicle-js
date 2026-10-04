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
  resolveTail,
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
    log: string;
    thing: string;
    upper: string;
    stream: string;
    stateBucket: string;
    logSubjects: string;
    opsFilter: string;
    opsSubject: string;
    opsPrefix: string;
    thingFromSubject: string;
    metaLogConfig: string;
    metaType: { type: string; key: string };
    metaIndex: { index: string; key: string };
  }
  const fx = suiteFile<{
    cases: Case[];
    identity: Record<"principal" | "member", { id: string; key: string }> & {
      invite: { digest: string; key: string };
    };
  }>("fixtures", "derivations.json");
  it.each(fx.cases)("$log / $thing", (c) => {
    expect(names.upperLog(c.log)).toBe(c.upper);
    expect(names.streamName(c.log)).toBe(c.stream);
    expect(names.stateBucket(c.log)).toBe(c.stateBucket);
    expect(names.logSubjects(c.log)).toBe(c.logSubjects);
    expect(names.opsFilter(c.log)).toBe(c.opsFilter);
    expect(names.opsSubject(c.log, c.thing)).toBe(c.opsSubject);
    expect(names.opsPrefix(c.log)).toBe(c.opsPrefix);
    expect(names.thingFromSubject(c.log, c.opsSubject)).toBe(c.thingFromSubject);
    expect(names.metaLogConfig(c.log)).toBe(c.metaLogConfig);
    expect(names.metaLogType(c.log, c.metaType.type)).toBe(c.metaType.key);
    expect(names.metaIndex(c.log, c.metaIndex.index)).toBe(c.metaIndex.key);
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
  const fx = suiteFile<{ types: Types; cases: { thing: string; kind: string; type?: string }[] }>(
    "fixtures",
    "resolve.json",
  );
  it.each(fx.cases)("$thing → $kind", async ({ thing, kind, type }) => {
    const res = await resolveTail(thing, lookupIn(fx.types));
    expect(res.kind).toBe(kind);
    if (res.kind === "typed") {
      expect(res.typeName).toBe(type);
    }
  });
});

describe("fold.json", () => {
  interface Case {
    name: string;
    thing: string;
    ops: { type: string; payload?: unknown; raw?: string }[];
    decisions: string[];
    state: unknown;
  }
  const fx = suiteFile<{ types: Types; cases: Case[] }>("fixtures", "fold.json");
  it.each(fx.cases)("$name", async ({ thing, ops, decisions, state }) => {
    const res = await resolveTail(thing, lookupIn(fx.types));
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
