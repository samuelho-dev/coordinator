import { timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import { arch, cpus, homedir, hostname, platform, release } from "node:os";
import { NodeHttpServer, NodeStdio, NodeStream } from "@effect/platform-node";
import { Context, Effect, FileSystem, Layer, PlatformError, Stdio } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

export class Host extends Context.Service<Host, {
  readonly homeDirectory: string;
  readonly hostname: string;
  readonly release: string;
  readonly platform: string;
  readonly architecture: string;
  readonly pid: number;
  readonly ppid: number;
  readonly uid: number | undefined;
  readonly executable: string;
  readonly cpuTimes?: Effect.Effect<ReadonlyArray<{ readonly user: number; readonly nice: number; readonly sys: number; readonly idle: number; readonly irq: number }>, PlatformError.PlatformError>;
}>()("@samuelho-dev/coordinator/Host") {
  static readonly layer = Layer.sync(this, () => ({
    homeDirectory: homedir(), hostname: hostname(), release: release(), platform: platform(),
    architecture: arch(), pid: process.pid, ppid: process.ppid, uid: process.getuid?.(), executable: process.execPath,
    cpuTimes: Effect.try({ try: () => cpus().map((cpu) => cpu.times), catch: (cause) => PlatformError.systemError({ _tag: "Unknown", module: "Host", method: "cpuTimes", cause }) }),
  }));
}

export const serverLayer = (host: string, port: number) => NodeHttpServer.layerServer(
  () => createServer({ requestTimeout: 15000 }), { host, port, gracefulShutdownTimeout: "1 second" },
);

// Only the CLI owns stdin; native plugins must retain NodeStdio's non-closing stream.
export const cliStdioLayer = Layer.effect(Stdio.Stdio, Effect.map(Stdio.Stdio, (stdio) => Stdio.make({
  ...stdio,
  stdin: NodeStream.fromReadable({
    evaluate: () => process.stdin,
    onError: (cause) => PlatformError.systemError({ module: "Stdio", method: "stdin", _tag: "Unknown", cause }),
    closeOnDone: true,
  }),
}))).pipe(Layer.provide(NodeStdio.layer));

export const equalDigests = (left: Uint8Array, right: Uint8Array): Effect.Effect<boolean> =>
  Effect.sync(() => timingSafeEqual(left, right));

export const checkExecutable = (file: string): Effect.Effect<void, PlatformError.PlatformError, FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.access(file);
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const code = yield* spawner.exitCode(ChildProcess.make("/bin/test", ["-x", file], {
      stdin: "ignore", stdout: "ignore", stderr: "ignore",
    }));
    if (code !== 0) return yield* PlatformError.systemError({
      _tag: "PermissionDenied", module: "FileSystem", method: "access", pathOrDescriptor: file,
      description: "Execute permission required",
    });
  });
