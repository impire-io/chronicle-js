// Fetches the chronicle release this SDK declares conformance against
// (package.json "chronicle.conformance"): the archive for this platform,
// unpacked into .chronicle/<version>/ — the `chronicle` binary the live
// scenarios boot, the contract artifact, and the conformance suite.
// Design 12 § the conformance suite: an SDK's CI fetches the archive and
// runs both halves against it.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { arch, platform } from "node:os";
import { join } from "node:path";

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const version = pkg.chronicle.conformance;
const dir = join(".chronicle", version);
if (existsSync(join(dir, "chronicle"))) {
  process.exit(0);
}

const os = { darwin: "darwin", linux: "linux" }[platform()];
const cpu = { x64: "amd64", arm64: "arm64" }[arch()];
if (!os || !cpu) {
  throw new Error(`no chronicle release archive for ${platform()}/${arch()}`);
}
const name = `chronicle_${version}_${os}_${cpu}.tar.gz`;
const url = `https://github.com/impire-io/chronicle/releases/download/v${version}/${name}`;
const res = await fetch(url);
if (!res.ok) {
  throw new Error(`fetch ${url}: ${res.status} ${res.statusText}`);
}
mkdirSync(dir, { recursive: true });
const archive = join(dir, name);
writeFileSync(archive, Buffer.from(await res.arrayBuffer()));
execFileSync("tar", ["-xzf", name], { cwd: dir, stdio: "inherit" });
console.log(`chronicle ${version} unpacked into ${dir}`);
