#!/usr/bin/env node
const { spawn } = require("node:child_process");
const { join } = require("node:path");

const binary = join(__dirname, "bin", `${process.platform}-${process.arch}`, "coordinator");
const child = spawn(binary, process.argv.slice(2), { stdio: "inherit" });
const signals = ["SIGINT", "SIGTERM", "SIGHUP"];
for (const signal of signals) process.on(signal, () => child.kill(signal));
child.on("error", (error) => {
  console.error(`Unable to start Coordinator binary ${binary}: ${error.message}`);
  process.exitCode = 1;
});
child.on("exit", (code, signal) => {
  if (signal) {
    process.removeAllListeners(signal);
    process.kill(process.pid, signal);
  } else {
    process.exitCode = code ?? 1;
  }
});
