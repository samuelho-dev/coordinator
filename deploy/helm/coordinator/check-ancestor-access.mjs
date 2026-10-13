// Run inside the disposable installed runner, from /app, under its actual
// UID1000/six-capability setpriv command. No model turn or enrollment is issued.
import assert from "node:assert/strict";
import { access, readFile, readdir } from "node:fs/promises";
import { constants } from "node:fs";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { resolve, join } from "node:path";
import { Schema } from "effect";
const { RunnerConfig } = await import(pathToFileURL(resolve("src/contracts.ts")).href);
assert.equal(process.getuid(), 1000, "Regression must exercise the real supervisor UID");
assert.match(await readFile("/proc/self/status", "utf8"), /^CapEff:\s+00000000000001e3$/m, "Regression requires exactly the six supervisor capabilities");
const config = Schema.decodeUnknownSync(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(await readFile(process.env.COORDINATOR_CONFIG, "utf8"));
for (const path of [config.stateDirectory, config.modelDirectory, config.childDirectory, ...Object.values(config.repositories)]) {
  await access(path, constants.R_OK | constants.W_OK | constants.X_OK);
  await readdir(path);
}
await access(config.tokenFile, constants.R_OK);
const seedPaths = [];
for (const agent of [".omp/agent", ".pi/agent"]) {
  const directory = join(config.modelDirectory, agent);
  let entries;
  try { entries = await readdir(directory); }
  catch (error) { if (error.code === "ENOENT") continue; throw error; }
  for (const entry of entries) {
    const path = join(directory, entry);
    await access(path, constants.R_OK);
    seedPaths.push(path);
  }
}
assert.ok(seedPaths.length > 0, "A real configured seed is required");
const childProbe = `const fs=require('node:fs');for(const p of process.argv.slice(1)){try{fs.accessSync(p,fs.constants.R_OK);process.exit(1)}catch(e){if(e.code!=='EACCES')process.exit(2)}}`;
await promisify(execFile)("/usr/bin/setpriv", [
  `--reuid=${config.childUidBase}`, `--regid=${config.childUidBase}`, "--clear-groups",
  "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--no-new-privs",
  process.execPath, "-e", childProbe, config.tokenFile, config.stateDirectory, config.modelDirectory, ...Object.values(config.repositories),
]);
const traverseProbe = "require('node:fs').accessSync(process.argv[1],require('node:fs').constants.X_OK)";
await promisify(execFile)("/usr/bin/setpriv", [
  `--reuid=${config.childUidBase}`, `--regid=${config.childUidBase}`, "--clear-groups",
  "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--no-new-privs",
  process.execPath, "-e", traverseProbe, config.childDirectory,
]);
console.log(JSON.stringify({ scenario: "deploy-init-ancestor-access", supervisorUid: 1000, supervisorTraversal: true, modelSeedReadable: true, childTraversal: true, childPrivatePathsDenied: true }));
