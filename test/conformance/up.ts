// Boots the open quick start of the declared chronicle release with its
// websocket listener (`chronicle up --websocket-port -1`, chronicle spec
// 025) and connects the SDK to it the way a browser would: over the
// websocket, as the quick start's one user. No mocked NATS.
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { Client } from "../../src/index.js";
import { chronicleBinary } from "./suite.js";

/** A running quick start and a client connected to it over the websocket. */
export interface Up {
  dir: string;
  websocketURL: string;
  client: Client;
  stop(): Promise<void>;
}

/** Boots `chronicle up` in a fresh data dir and connects as its user. */
export async function up(): Promise<Up> {
  const dir = mkdtempSync(join(tmpdir(), "chronicle-js-"));
  const child: ChildProcess = spawn(
    chronicleBinary,
    ["up", "--dir", dir, "--port", "-1", "--websocket-port", "-1"],
    {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, CHRONICLE_CONFIG_HOME: join(dir, "config") },
    },
  );
  let output = "";
  child.stdout?.on("data", (d: Buffer) => (output += d.toString()));
  child.stderr?.on("data", (d: Buffer) => (output += d.toString()));
  const urlFile = join(dir, "websocket.url");
  const deadline = Date.now() + 30_000;
  while (!existsSync(urlFile) || !output.includes("ws:")) {
    if (child.exitCode !== null || Date.now() > deadline) {
      child.kill("SIGKILL");
      throw new Error(`chronicle up did not come up:\n${output}`);
    }
    await sleep(100);
  }
  const websocketURL = readFileSync(urlFile, "utf8").trim();
  const client = await Client.connect({
    servers: websocketURL,
    nkeySeed: readFileSync(join(dir, "user.nk"), "utf8").trim(),
    author: "admin",
  });
  return {
    dir,
    websocketURL,
    client,
    async stop() {
      await client.close().catch(() => undefined);
      const exited = new Promise((resolve) => child.once("exit", resolve));
      child.kill("SIGTERM");
      await Promise.race([exited, sleep(10_000)]);
      if (child.exitCode === null) {
        child.kill("SIGKILL");
      }
      rmSync(dir, { recursive: true, force: true });
    },
  };
}
