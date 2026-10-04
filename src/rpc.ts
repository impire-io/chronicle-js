// The two request shapes chronicle's wire uses (design 12 § the shapes):
// request/reply, and the streamed reply — chunks of items numbered by
// Chron-Chunk, ending on a trailer marked Chron-End.
import { createInbox, RequestError, type Msg, type NatsConnection } from "@nats-io/nats-core";
import { NoResponderError, ServiceError, StreamGapError, StreamStalledError } from "./errors.js";
import { HDR } from "./contract/record.js";
import { CLIENT_DEFAULTS } from "./generated/contract.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** The catalogued error a service answered with, if any. */
function serviceError(msg: Msg): ServiceError | undefined {
  const code = msg.headers?.get(HDR.serviceErrorCode) ?? "";
  return code === "" ? undefined : new ServiceError(code, msg.headers?.get(HDR.serviceError) ?? "");
}

/** Options for one request. */
export interface CallOptions {
  /** How long to wait for the reply, in milliseconds. */
  timeout?: number;
}

/**
 * Request/reply: one JSON request, one JSON reply. No responder is a
 * NoResponderError; a catalogued refusal is a ServiceError.
 */
export async function request<Reply>(
  nc: NatsConnection,
  subject: string,
  req: unknown,
  opts: CallOptions = {},
): Promise<Reply> {
  let msg: Msg;
  try {
    msg = await nc.request(subject, encoder.encode(JSON.stringify(req)), { timeout: opts.timeout ?? 10_000 });
  } catch (err) {
    if (err instanceof RequestError && err.isNoResponders()) {
      throw new NoResponderError(subject);
    }
    throw err;
  }
  const refused = serviceError(msg);
  if (refused) {
    throw refused;
  }
  return JSON.parse(decoder.decode(msg.data)) as Reply;
}

/** Options for a streamed reply. */
export interface StreamOptions {
  /** Ends the stream early; the iterator throws the signal's reason. */
  signal?: AbortSignal;
  /** How long a stream may go quiet before it is stalled, in milliseconds. */
  stall?: number;
}

/**
 * A streamed reply: iterate it for the items, then read the trailer.
 * A collection is never an array (design 12 § the surface rule).
 */
export class Streamed<Item, Trailer> implements AsyncIterable<Item> {
  #trailer: Trailer | undefined;
  #done = false;
  #count = 0;
  readonly #open: (self: Streamed<Item, Trailer>) => AsyncGenerator<Item>;

  constructor(open: (self: Streamed<Item, Trailer>) => AsyncGenerator<Item>) {
    this.#open = open;
  }

  [Symbol.asyncIterator](): AsyncIterator<Item> {
    return this.#open(this);
  }

  /** The trailer, once the stream ended on it; undefined before or after a failure. */
  get trailer(): Trailer | undefined {
    return this.#done ? this.#trailer : undefined;
  }

  /** How many items arrived. */
  get count(): number {
    return this.#count;
  }

  /** @internal */
  _item(): void {
    this.#count++;
  }

  /** @internal */
  _end(trailer: Trailer | undefined): void {
    this.#trailer = trailer;
    this.#done = true;
  }

  /** Collects every item; for small results and tests — a collection is meant to be iterated. */
  async toArray(): Promise<Item[]> {
    const out: Item[] = [];
    for await (const item of this) {
      out.push(item);
    }
    return out;
  }
}

/** Waits for the next message, or fails when the stream stalls or the signal aborts. */
async function nextWithin(
  it: AsyncIterator<Msg>,
  stall: number,
  subject: string,
  signal?: AbortSignal,
): Promise<Msg | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  try {
    const stalled = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        reject(new StreamStalledError(`${subject}: streamed reply stalled after ${stall}ms`));
      }, stall);
    });
    const aborted = new Promise<never>((_, reject) => {
      if (signal) {
        onAbort = () => {
          const reason: unknown = signal.reason;
          reject(reason instanceof Error ? reason : new Error(String(reason)));
        };
        if (signal.aborted) {
          onAbort();
        }
        signal.addEventListener("abort", onAbort, { once: true });
      }
    });
    const next = await Promise.race([it.next(), stalled, aborted]);
    return next.done ? undefined : next.value;
  } finally {
    clearTimeout(timer);
    if (signal && onAbort) {
      signal.removeEventListener("abort", onAbort);
    }
  }
}

/**
 * The streamed reply (design 12 § the streamed reply): one request on a
 * fresh inbox; chunks are JSON arrays numbered from 1 by Chron-Chunk; the
 * trailer continues the numbering, carries the item count in Chron-End
 * and the kind's totals in its body. A gap, a disagreeing count or a
 * stall fails the stream after the items already yielded.
 */
export function requestStream<Item, Trailer>(
  nc: NatsConnection,
  subject: string,
  req: unknown,
  opts: StreamOptions = {},
): Streamed<Item, Trailer> {
  const stall = opts.stall ?? CLIENT_DEFAULTS.streamStallSeconds * 1000;
  return new Streamed<Item, Trailer>(async function* (self) {
    const inbox = createInbox();
    const sub = nc.subscribe(inbox);
    const it = sub[Symbol.asyncIterator]();
    try {
      nc.publish(subject, encoder.encode(JSON.stringify(req)), { reply: inbox });
      let expect = 1;
      for (;;) {
        const msg = await nextWithin(it, stall, subject, opts.signal);
        if (msg === undefined) {
          throw new StreamGapError(`${subject}: the reply ended without a trailer`);
        }
        if (msg.headers?.code === 503) {
          throw new NoResponderError(subject);
        }
        const refused = serviceError(msg);
        if (refused) {
          throw refused;
        }
        const chunk = Number(msg.headers?.get(HDR.chunk) ?? "");
        if (chunk !== expect) {
          throw new StreamGapError(
            `${subject}: got chunk ${JSON.stringify(msg.headers?.get(HDR.chunk) ?? "")}, want ${expect}`,
          );
        }
        expect++;
        const end = msg.headers?.get(HDR.end) ?? "";
        if (end !== "") {
          if (Number(end) !== self.count) {
            throw new StreamGapError(`${subject}: the trailer counts ${end} items, ${self.count} arrived`);
          }
          self._end(msg.data.length > 0 ? (JSON.parse(decoder.decode(msg.data)) as Trailer) : undefined);
          return;
        }
        const page = JSON.parse(decoder.decode(msg.data)) as Item[];
        for (const item of page) {
          self._item();
          yield item;
        }
      }
    } finally {
      sub.unsubscribe();
    }
  });
}
