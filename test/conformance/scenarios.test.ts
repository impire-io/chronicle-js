// The live scenarios (conformance/scenarios): sentences against `chronicle
// up` in the open form, judged as the Go runner judges them — derived
// observations polled until they hold, subscribe steps opening the
// iterator, performing the write in `then`, and expecting the item.
import { readdirSync } from "node:fs";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { isDeepStrictEqual } from "node:util";
import { afterAll, beforeAll, describe, it } from "vitest";
import {
  InstanceExistsError,
  InstanceMovedError,
  NoResponderError,
  ServiceError,
  UndefinedOperationError,
  type Client,
  type TypeDefinition,
} from "../../src/index.js";
import { releaseDir, suiteFile } from "./suite.js";
import { up, type Up } from "./up.js";

const SETTLE_MS = 15_000;

interface Expect {
  state?: Record<string, unknown>;
  types?: string[];
  items?: string[];
  instances?: string[];
  total?: number;
  count?: number;
  error?: string;
  taken?: boolean;
  type?: string;
  first?: unknown;
  contains?: Record<string, unknown>;
  arrives?: string;
}

interface Step {
  do: string;
  store?: string;
  instance?: string;
  type?: string;
  index?: string;
  kind?: string;
  op?: string;
  text?: string;
  in?: string;
  where?: Record<string, string>;
  def?: TypeDefinition;
  config?: unknown;
  payload?: unknown;
  expectSeq?: number;
  limit?: number;
  live?: boolean;
  after?: number;
  then?: Step;
  expect?: Expect;
}

const str = (v: string | undefined): string => v ?? "";

/** An error's message, whatever was thrown. */
const message = (e: unknown): string => (e instanceof Error ? e.message : JSON.stringify(e));

/** Every key the expectation names equals the actual's. */
function subset(actual: unknown, expected: unknown): string {
  if (typeof expected !== "object" || expected === null || Array.isArray(expected)) {
    return isDeepStrictEqual(actual, expected)
      ? ""
      : `${JSON.stringify(actual)} ≠ ${JSON.stringify(expected)}`;
  }
  if (typeof actual !== "object" || actual === null) {
    return `${JSON.stringify(actual)} is not an object`;
  }
  for (const [k, v] of Object.entries(expected)) {
    if (!isDeepStrictEqual((actual as Record<string, unknown>)[k], v)) {
      return `${k}: ${JSON.stringify((actual as Record<string, unknown>)[k])}, want ${JSON.stringify(v)}`;
    }
  }
  return "";
}

const sameSet = (a: string[], b: string[]): boolean => isDeepStrictEqual([...a].sort(), [...b].sort());

/** Whether err is the error the scenario names (the Go runner's matchesError). */
function matchesError(err: unknown, want: string): boolean {
  switch (want) {
    case "instance-exists":
      return err instanceof InstanceExistsError;
    case "instance-moved":
      return err instanceof InstanceMovedError;
    case "undefined-operation":
      return err instanceof UndefinedOperationError;
    case "no-responder":
      return err instanceof NoResponderError;
    case "preflight":
      return (
        !(err instanceof ServiceError) &&
        !(err instanceof InstanceExistsError) &&
        !(err instanceof InstanceMovedError)
      );
  }
  return err instanceof ServiceError && err.code === want;
}

async function collect(items: AsyncIterable<string>): Promise<string[]> {
  const out: string[] = [];
  for await (const item of items) {
    out.push(item);
  }
  return out;
}

class Runner {
  constructor(private readonly c: Client) {}

  async step(label: string, s: Step): Promise<void> {
    const want = s.expect?.error ?? "";
    let err: unknown;
    try {
      await this.perform(label, s);
    } catch (e) {
      err = e;
    }
    if (want === "") {
      if (err !== undefined) {
        throw new Error(`${label}: ${message(err)}`, { cause: err });
      }
      return;
    }
    if (err === undefined) {
      throw new Error(`${label}: expected error ${want}, got none`);
    }
    if (!matchesError(err, want)) {
      throw new Error(`${label}: expected error ${want}, got ${message(err)}`, { cause: err });
    }
  }

  /**
   * Polls a derived observation until it holds, or the settle time passes;
   * an expected refusal is observed once, not waited for, and its own
   * error surfaces.
   */
  async until(label: string, s: Step, check: () => Promise<string>): Promise<void> {
    if (s.expect?.error) {
      const why = await check();
      if (why !== "") throw new Error(`${label}: ${why}`);
      return;
    }
    const deadline = Date.now() + SETTLE_MS;
    for (;;) {
      let why: string;
      try {
        why = await check();
      } catch (err) {
        if (Date.now() > deadline) throw new Error(`${label}: ${message(err)}`, { cause: err });
        why = message(err);
      }
      if (why === "") return;
      if (Date.now() > deadline) throw new Error(`${label}: ${why}`);
      await sleep(100);
    }
  }

  async perform(label: string, s: Step): Promise<void> {
    const c = this.c;
    const store = str(s.store);
    const instance = str(s.instance);
    const e = s.expect ?? {};
    switch (s.do) {
      case "store.create":
        await c.createStore(store);
        return;
      case "type.define":
        await c.defineType(store, str(s.type), s.def ?? { schema: {} });
        return;
      case "index.declare":
        await c.declareIndex(store, str(s.index), str(s.kind), s.config);
        return;
      case "index.delete":
        await c.deleteIndex(store, str(s.index));
        return;
      case "instance.create.snapshot":
        await c.createFromSnapshot(store, instance, s.payload);
        return;
      case "instance.create":
        await c.create(store, instance, s.op ?? "create", s.payload);
        return;
      case "apply":
        await c.apply(
          store,
          instance,
          str(s.op),
          s.payload,
          s.expectSeq === undefined ? {} : { expectedSeq: s.expectSeq },
        );
        return;
      case "apply.guarded": {
        const sv = await c.state(store, instance);
        await c.apply(store, instance, str(s.op), s.payload, { expectedSeq: sv.seq });
        return;
      }
      case "snapshot": {
        const r = await c.snapshot(store, instance);
        if (e.taken !== undefined && r.taken !== e.taken) {
          throw new Error(`taken=${String(r.taken)} (${str(r.reason)}), want ${String(e.taken)}`);
        }
        return;
      }
      case "get":
        await this.until(label, s, async () => subset((await c.state(store, instance)).state, e.state));
        return;
      case "history":
        await this.until(label, s, async () => {
          const types: string[] = [];
          for await (const op of c.history(store, instance)) {
            types.push(op.type);
          }
          return isDeepStrictEqual(types, e.types) ? "" : `history types ${types.join(",")}`;
        });
        return;
      case "query":
        await this.until(label, s, async () => {
          const st = c.queryIndex(store, str(s.index), str(s.text), s.limit ? { limit: s.limit } : {});
          const instances: string[] = [];
          for await (const hit of st) {
            instances.push(hit.instance);
          }
          const tr = st.trailer;
          if (tr === undefined) {
            return "no trailer";
          }
          if (e.instances && !sameSet(instances, e.instances)) {
            return `hits ${instances.join(",")}`;
          }
          if (e.total !== undefined && tr.total !== e.total) {
            return `total ${tr.total}`;
          }
          if (e.count !== undefined && st.count !== e.count) {
            return `count ${st.count}`;
          }
          return "";
        });
        return;
      case "list.stores":
      case "list.types":
      case "list.indexes":
      case "list.members":
      case "list.instances":
        await this.until(label, s, async () => {
          let items: string[] = [];
          if (s.do === "list.stores") items = await collect(c.listStores());
          if (s.do === "list.types") items = await collect(c.listTypes(store));
          if (s.do === "list.instances") {
            for await (const i of c.listInstances(store, {
              ...(s.type !== undefined ? { type: s.type } : {}),
              ...(s.in !== undefined ? { under: s.in } : {}),
              ...(s.where !== undefined ? { where: s.where } : {}),
            })) {
              items.push(i.path);
            }
          }
          if (s.do === "list.indexes") {
            // State is read as each instance's state, not listed as an index.
            for await (const i of c.listIndexes(store)) if (i.kind !== "state") items.push(i.name);
          }
          if (s.do === "list.members") for await (const m of c.listMembers()) items.push(m.name);
          return sameSet(items, e.items ?? []) ? "" : `items ${items.join(",")}`;
        });
        return;
      case "watch":
        await this.live(label, s, (signal) => map(c.watch(store, instance, { signal }), (sv) => sv.state));
        return;
      case "tail":
        await this.live(label, s, (signal) =>
          map(
            c.tail(store, instance, {
              signal,
              ...(s.live ? { live: true } : {}),
              ...(s.after !== undefined ? { after: s.after } : {}),
            }),
            (op) => op.type,
          ),
        );
        return;
      case "watch.declarations":
        await this.live(label, s, (signal) =>
          map(c.watchDeclarations(store, { signal }), (d) => `${d.kind}:${d.name}`),
        );
        return;
    }
    throw new Error(`unknown sentence ${s.do}`);
  }

  /** Opens a live iterator, takes what is at rest, performs `then`, and expects what arrives. */
  async live(label: string, s: Step, open: (signal: AbortSignal) => AsyncIterable<unknown>): Promise<void> {
    const ctl = new AbortController();
    const timer = setTimeout(() => {
      ctl.abort(new Error(`nothing arrived within ${SETTLE_MS}ms`));
    }, SETTLE_MS);
    const queue: unknown[] = [];
    let waiting: (() => void) | undefined;
    let ended: unknown;
    let done = false;
    const pump = (async () => {
      try {
        for await (const v of open(ctl.signal)) {
          queue.push(v);
          waiting?.();
        }
      } catch (err) {
        ended = err;
      } finally {
        done = true;
        waiting?.();
      }
    })();
    const next = async (): Promise<unknown> => {
      while (queue.length === 0) {
        if (done || ctl.signal.aborted) {
          throw new Error(
            `${label}: ${message(ended ?? ctl.signal.reason ?? new Error("the iterator ended"))}`,
          );
        }
        await new Promise<void>((resolve) => {
          waiting = resolve;
          ctl.signal.addEventListener(
            "abort",
            () => {
              resolve();
            },
            { once: true },
          );
        });
        waiting = undefined;
      }
      return queue.shift();
    };
    try {
      const e = s.expect ?? {};
      if (e.first !== undefined) {
        if (Array.isArray(e.first) && e.first.length > 0) {
          const seen: string[] = [];
          while (seen.length < e.first.length) seen.push(String(await next()));
          if (!sameSet(seen, e.first as string[])) {
            throw new Error(`${label}: first ${seen.join(",")}, want ${(e.first as string[]).join(",")}`);
          }
        } else {
          const why = subset(await next(), e.first);
          if (why !== "") throw new Error(`${label}: first: ${why}`);
        }
      }
      if (s.then) {
        await sleep(500);
        await this.perform(`${label} then`, s.then);
      }
      if (e.contains) {
        while (subset(await next(), e.contains) !== "");
      } else if (e.arrives !== undefined || e.type !== undefined) {
        const want = e.arrives ?? e.type;
        while ((await next()) !== want);
      } else if (e.types) {
        const types: string[] = [];
        while (types.length < e.types.length) types.push(String(await next()));
        if (!isDeepStrictEqual(types, e.types)) throw new Error(`${label}: types ${types.join(",")}`);
      }
    } finally {
      clearTimeout(timer);
      ctl.abort(new Error("done"));
      await pump;
    }
  }
}

async function* map<T, U>(items: AsyncIterable<T>, f: (t: T) => U): AsyncGenerator<U> {
  for await (const item of items) yield f(item);
}

const files = readdirSync(join(releaseDir, "conformance", "scenarios"))
  .filter((f) => f.endsWith(".json"))
  .sort();

for (const file of files) {
  const sc = suiteFile<{ name: string; steps: Step[] }>("scenarios", file);
  describe(`scenario: ${sc.name}`, () => {
    let u: Up;
    beforeAll(async () => {
      u = await up();
    });
    afterAll(async () => {
      await u.stop();
    });
    it("every step", async () => {
      const r = new Runner(u.client);
      for (const [i, s] of sc.steps.entries()) {
        await r.step(`step ${i + 1} ${s.do}`, s);
      }
    });
  });
}
