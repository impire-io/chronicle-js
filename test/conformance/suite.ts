// Where the conformance suite of the declared version lives: the release
// archive scripts/fetch-release.mjs unpacks (design 12 § the conformance
// suite — an SDK's CI fetches the archive and runs both halves).
import { readFileSync } from "node:fs";
import { join } from "node:path";

const pkg = JSON.parse(readFileSync("package.json", "utf8")) as { chronicle: { conformance: string } };

/** The chronicle release the suite comes from. */
export const releaseVersion = pkg.chronicle.conformance;
/** The unpacked release. */
export const releaseDir = join(".chronicle", releaseVersion);
/** The `chronicle` binary the live scenarios boot. */
export const chronicleBinary = join(releaseDir, "chronicle");

/** Reads one JSON file of the suite, typed by the caller. */
// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- a JSON loader is typed at the call
export function suiteFile<T>(...path: string[]): T {
  return JSON.parse(readFileSync(join(releaseDir, "conformance", ...path), "utf8")) as T;
}
