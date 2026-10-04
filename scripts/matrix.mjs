// Renders the support matrix (design 12 § the support matrix) into the
// README from the two facts it is made of: the contract version this SDK
// declares (package.json) and the conformance suite's result (vitest's JSON
// report of test/conformance). Never hand-edited: CI renders it and fails
// when the README differs.
import { readFileSync, writeFileSync } from "node:fs";
import prettier from "prettier";

const [, , reportPath = ".chronicle/conformance.json"] = process.argv;
const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const report = JSON.parse(readFileSync(reportPath, "utf8"));

const halves = { fixtures: { passed: 0, failed: 0 }, scenarios: { passed: 0, failed: 0 } };
for (const file of report.testResults) {
  const half = file.name.endsWith("fixtures.test.ts")
    ? "fixtures"
    : file.name.endsWith("scenarios.test.ts")
      ? "scenarios"
      : null;
  if (!half) continue;
  for (const t of file.assertionResults) {
    halves[half][t.status === "passed" ? "passed" : "failed"]++;
  }
}
const cell = ({ passed, failed }) =>
  failed === 0 && passed > 0 ? `✅ ${passed} passed` : `❌ ${failed} failed of ${passed + failed}`;
const supported =
  halves.fixtures.failed === 0 &&
  halves.scenarios.failed === 0 &&
  halves.fixtures.passed > 0 &&
  halves.scenarios.passed > 0;

const table = [
  "| SDK | Contract | Suite (chronicle release) | Fixtures | Scenarios | Supports |",
  "|---|---|---|---|---|---|",
  `| \`${pkg.name}\` ${pkg.version} | ${pkg.chronicle.contract} | v${pkg.chronicle.conformance} | ${cell(halves.fixtures)} | ${cell(halves.scenarios)} | ${supported ? "yes" : "no"} |`,
].join("\n");

const begin = "<!-- support-matrix:begin (rendered by scripts/matrix.mjs — do not edit) -->";
const end = "<!-- support-matrix:end -->";
const readme = readFileSync("README.md", "utf8");
const a = readme.indexOf(begin);
const b = readme.indexOf(end);
if (a < 0 || b < a) {
  throw new Error("README.md has no support-matrix markers");
}
const rendered = `${readme.slice(0, a + begin.length)}\n\n${table}\n\n${readme.slice(b)}`;
const config = await prettier.resolveConfig("README.md");
writeFileSync("README.md", await prettier.format(rendered, { ...config, parser: "markdown" }));
console.log(table);
if (!supported) {
  process.exitCode = 1;
}
