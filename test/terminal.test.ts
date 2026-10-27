import { assert, it } from "@effect/vitest";
import { ConfigProvider, Effect, Schema } from "effect";
import { readTerminalContext } from "../src/terminal.ts";
import { SessionTerminal } from "../src/inspection.ts";

it("accepts arbitrary terminal names and preserves missing versions through the JSON wire codec", async () => {
  for (const environment of [
    { TERM_PROGRAM: "tern", TERM_PROGRAM_VERSION: "0.4.3" },
    { TERM_PROGRAM: "future-terminal" },
  ]) {
    const observed = await Effect.runPromise(readTerminalContext.pipe(Effect.provideService(
      ConfigProvider.ConfigProvider,
      ConfigProvider.fromEnvRecord({ ...environment, PRIVATE_TOKEN: "private-credential", ORCA_TERMINAL_HANDLE: "private-pane", REX_SESSION: "private-session" }),
    )));
    assert.ok(observed);
    assert.deepStrictEqual(observed, { name: environment.TERM_PROGRAM, ...(environment.TERM_PROGRAM_VERSION ? { version: environment.TERM_PROGRAM_VERSION } : {}) });
    assert.deepStrictEqual(Schema.decodeUnknownSync(SessionTerminal)(Schema.encodeSync(Schema.toCodecJson(SessionTerminal))(observed)), observed);
    assert.strictEqual(JSON.stringify(observed).includes("private-"), false);
  }
});

it("leaves terminal identity unknown without valid standard markers, regardless of vendor or container hints", async () => {
  for (const environment of [
    {},
    { TERM: "xterm-256color" },
    { TERM_PROGRAM_VERSION: "1.0" },
    { ORCA_TERMINAL_HANDLE: "term_native", REX_SESSION: "native-session", KUBERNETES_SERVICE_HOST: "10.0.0.1" },
    { TERM_PROGRAM: "   " },
    { TERM_PROGRAM: "name\nbad" },
    { TERM_PROGRAM: "x".repeat(4097) },
    { TERM_PROGRAM: "valid-name", TERM_PROGRAM_VERSION: "version\u0085bad" },
  ]) {
    assert.strictEqual(await Effect.runPromise(readTerminalContext.pipe(
      Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(environment)),
    )), undefined);
  }
});
