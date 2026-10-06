import { randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { isAbsolute } from "node:path";
import { Address, Work, decode, inspectedRepo, privateFile, rpc, fingerprint, type PluginConfig, type Task } from "./protocol.js";
import { Journal } from "./journal.js";

// Structural subset shared by omp 18.6.1 and Pi 1.0.4 (Pi 0.84.2 source-compatible).
// Address the native session, not an unavailable sub-worker API; no runtime host import.
interface Context {
  cwd: string; hasUI: boolean; isIdle(): boolean; hasPendingMessages(): boolean;
  sessionManager: { getSessionId(): string; getSessionFile(): string | undefined };
  ui: { notify(text: string, level?: "info" | "warning" | "error"): void; confirm(title: string, text: string): Promise<boolean> };
}
interface Host {
  on(event: "session_start" | "session_shutdown" | "session_switch", handler: (event: unknown, ctx: Context) => Promise<void>): unknown;
  registerCommand(name: string, options: { description: string; handler: (args: string, ctx: Context) => Promise<void> }): void;
  sendUserMessage(text: string, options: { deliverAs: "followUp" }): void;
}
interface Poll { messages: { messageId: string; incarnation: string; payload: unknown; expiresAt: number }[]; offers: Task[]; releases: Task[]; archiveEnabled?: boolean }

export default function coordinator(host: Host): void {
  const path = process.env.COORDINATOR_CONFIG;
  if (!path) return;
  if (!isAbsolute(path)) throw new Error("COORDINATOR_CONFIG must be absolute");
  const config = JSON.parse(privateFile(path)) as PluginConfig;
  if (!isAbsolute(config.stateDirectory) || !isAbsolute(config.tokenFile) || !["omp", "pi"].includes(config.host) || !/^[a-zA-Z0-9_.:-]{1,160}$/.test(config.machineId)) throw new Error("Invalid coordinator configuration");
  const token = privateFile(config.tokenFile);
  let active: { ctx: Context; sessionId: string; incarnation: string; journal: Journal; repo: string; heartbeat: number } | undefined;
  let timer: NodeJS.Timeout | undefined;
  let chain = Promise.resolve();
  const serial = (operation: () => Promise<void>): Promise<void> => {
    const result = chain.then(operation);
    chain = result.catch(() => {});
    return result;
  };
  const call = <T = unknown>(method: string, params: unknown): Promise<T> => rpc<T>(config.brokerUrl, token, method, params);
  const notify = (ctx: Context, text: string, level: "info" | "warning" | "error" = "info") => ctx.ui.notify(text, level);
  const local = (ctx: Context) => ({ repo: inspectedRepo(ctx.cwd, config.selectedRemote),
    ref: execFileSync("git", ["rev-parse", "--symbolic-full-name", "HEAD"], { cwd: ctx.cwd, encoding: "utf8" }).trim(),
    sha: execFileSync("git", ["rev-parse", "HEAD"], { cwd: ctx.cwd, encoding: "utf8" }).trim() });
  const tick = async (): Promise<void> => {
    const current = active;
    if (!current) return;
    const { ctx, sessionId, incarnation, journal } = current;
    if (ctx.sessionManager.getSessionId() !== sessionId) return;
    current.repo = inspectedRepo(ctx.cwd, config.selectedRemote);
    if (Date.now() - current.heartbeat >= 10000) {
      await call("session.heartbeat", { sessionId, incarnation, machineId: config.machineId, host: config.host, repo: current.repo, workerId: sessionId });
      current.heartbeat = Date.now();
    }
    const poll = await call<Poll>("session.poll", { sessionId, incarnation });
    if (poll.archiveEnabled === false && journal.state.consent) journal.erase();
    for (const message of poll.messages) {
      if (message.incarnation !== incarnation || message.expiresAt <= Date.now()) continue;
      const newlyHandled = journal.receive(message.messageId, message.payload);
      if (newlyHandled) notify(ctx, `Coordinator message ${message.messageId}; /coordinator inbox shows the durably received content.`);
      await call("peer.ack", { sessionId, incarnation, messageId: message.messageId });
    }
    for (const offer of poll.offers) {
      if (journal.handled(`offer:${offer.taskId}`)) continue;
      notify(ctx, `Coordinator offer ${offer.taskId}: ${offer.request.repo} ${offer.request.ref} ${offer.request.sha}. /coordinator status {"taskId":"${offer.taskId}"}; then accept or decline explicitly.`);
      journal.handle(`offer:${offer.taskId}`);
    }
    for (const task of poll.releases) {
      const address = task.address;
      if (!address || task.confirmedRevision !== address.revision || task.disposition !== "ready" || !task.readback || fingerprint(task.readback) !== fingerprint(address) || address.sessionId !== sessionId || address.incarnation !== incarnation) continue;
      const approvedAddress = task.instruction?.address;
      if (address.deadline <= Date.now() || address.repo !== task.request.repo || (approvedAddress ? fingerprint(address) !== fingerprint(approvedAddress) : address.ref !== task.request.ref || address.sha !== task.request.sha) || address.workerId !== sessionId) continue;
      if (!ctx.isIdle() || ctx.hasPendingMessages()) continue;
      const actual = local(ctx);
      if (actual.repo !== address.repo || actual.ref !== address.ref || actual.sha !== address.sha) {
        await call("task.readback", { taskId: task.taskId, address, disposition: "hold", reason: "Local repository/ref/SHA changed", nextCheck: Date.now() + 60000 });
        continue;
      }
      const key = `${task.taskId}:${address.revision}`;
      if (journal.state.releases[key]) {
        if (journal.state.releases[key] === "intent") {
          await call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "HELD", result: { reason: "Release delivery uncertain; explicit new addressed revision required" } });
        }
        continue;
      }
      if (!journal.releaseIntent(key)) continue;
      // Durable intent precedes the host call. A crash/throw is held, never retried.
      host.sendUserMessage(task.instruction?.prompt ?? task.request.prompt, { deliverAs: "followUp" });
      journal.releaseSent(key);
      await call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "RUNNING", result: { revision: address.revision, promptAdmitted: true, evidenceVerified: false } });
    }
    journal.scan(ctx.sessionManager.getSessionFile());
    await journal.upload((entry) => call("archive.append", { sessionId, ...entry }), 1);
  };
  const stop = async (): Promise<void> => {
    clearInterval(timer);
    timer = undefined;
    const previous = active; active = undefined;
    if (previous) {
      try { await call("session.stop", { sessionId: previous.sessionId, incarnation: previous.incarnation }); }
      catch { notify(previous.ctx, "Coordinator stop unavailable; prior enrollment expires by heartbeat lease.", "warning"); }
    }
  };
  const start = async (_: unknown, ctx: Context): Promise<void> => serial(async () => {
    await stop();
    const sessionId = ctx.sessionManager.getSessionId();
    active = { ctx, sessionId, incarnation: randomUUID(), repo: inspectedRepo(ctx.cwd, config.selectedRemote), journal: new Journal(config.stateDirectory, sessionId), heartbeat: 0 };
    let scheduled = false;
    timer = setInterval(() => {
      if (scheduled) return;
      scheduled = true;
      void serial(tick).catch(() => notify(ctx, "Coordinator connection or journal unavailable; dispatch held.", "warning")).finally(() => { scheduled = false; });
    }, 1000);
    timer.unref();
    await tick();
  });
  host.on("session_start", start);
  // omp emits session_switch; Pi 0.84.2 emits shutdown/start for replacements.
  if (config.host === "omp") host.on("session_switch", start);
  host.on("session_shutdown", async () => serial(stop));
  host.registerCommand("coordinator", { description: "Coordinator: explicit addressed dispatch, status, consent and runner-owned control (JSON arguments)", handler: async (args, ctx) => serial(async () => {
    try {
      const split = args.trim().indexOf(" ");
      const action = split < 0 ? args.trim() : args.trim().slice(0, split);
      const data: Record<string, unknown> = split < 0 ? {} : JSON.parse(args.trim().slice(split + 1));
      const current = active;
      if (!current || current.sessionId !== ctx.sessionManager.getSessionId()) throw new Error("Session not enrolled");
      const { sessionId, incarnation, journal } = current;
      let result: unknown;
      if (action === "participant-consent") {
        if (typeof data.owner !== "string" || typeof data.sessionId !== "string" || typeof data.enabled !== "boolean") throw new Error("Explicit owner, target sessionId and enabled required");
        if (!ctx.hasUI || !await ctx.ui.confirm("Your participant archive consent", `${data.enabled ? "Grant" : "Revoke"} YOUR consent for owner ${data.owner}, session ${data.sessionId}? Content omitted; graph metadata only.`)) return;
        result = await call("archive.participantConsent", { owner: data.owner, sessionId: data.sessionId, enabled: data.enabled });
      } else if (action === "consent") {
        const participants = data.participants;
        if (!Array.isArray(participants) || !participants.length || !participants.every((id) => typeof id === "string" && /^[a-zA-Z0-9_.:-]{1,160}$/.test(id))) throw new Error("Explicit participant IDs required");
        if (!ctx.hasUI || !await ctx.ui.confirm("Your session archive consent", `Grant YOUR consent to archive this session with participants ${participants.join(", ")}? Each other participant must separately enroll consent with their own credential; broker checks it. Only graph metadata is archived across branches; content is omitted.`)) return;
        await call("archive.consent", { sessionId, participants, enabled: true });
        journal.consent(participants);
        result = { enabled: true };
      } else if (action === "optout") {
        // Revoke server enrollment first; failure leaves local spool intact for retry.
        await call("archive.consent", { sessionId, participants: journal.state.participants, enabled: false });
        journal.erase();
        await call("archive.delete", { sessionId });
        result = { enabled: false };
      } else if (action === "accept" || action === "decline") {
        result = await call("peer.reply", { taskId: data.taskId, sessionId, incarnation, decision: action === "accept" ? "accepted" : "declined" });
      } else if (action === "submit") {
        const work = decode(Work, data);
        if (!ctx.hasUI || !await ctx.ui.confirm("Approve coordinator task", `${work.taskId}: ${work.repo} ${work.ref} ${work.sha}; deadline ${work.deadline}; runner ${work.launcher}. Submit this exact approved work?`)) return;
        result = await call("task.submit", work);
      } else if (action === "readback" || ["hold", "clarify", "unable"].includes(action)) {
        const address = decode(Address, data.address);
        if (address.sessionId !== sessionId || address.incarnation !== incarnation || address.workerId !== sessionId) throw new Error("Readback must address this native session incarnation and worker");
        if (action === "readback") {
          const actual = local(ctx);
          if (address.deadline <= Date.now() || address.repo !== actual.repo || address.sha !== actual.sha || address.ref !== actual.ref) throw new Error("Readback does not match current local repository/ref/SHA/worker/deadline");
        }
        result = await call("task.readback", { taskId: data.taskId, address, disposition: action === "readback" ? "ready" : action, ...(data.reason === undefined ? {} : { reason: data.reason }), ...(data.nextCheck === undefined ? {} : { nextCheck: data.nextCheck }) });
      } else if (action === "address") result = await call("task.address", { taskId: data.taskId, address: decode(Address, data.address) });
      else if (action === "confirm") result = await call("task.confirm", data);
      else if (action === "handoff") result = await call("task.handoff", data);
      else if (action === "instruction") {
        if (!ctx.hasUI || !await ctx.ui.confirm("Approve later addressed instruction", `Approve exact message ${String(data.taskId)} and its new revision, ref, scope and restrictions? Existing native work is not retroactively gated.`)) return;
        result = await call("task.instruction", data);
      }
      else if (action === "report") result = await call("task.report", { ...data, sessionId, incarnation });
      else if (action.startsWith("evidence-")) result = await call(`evidence.${action.slice(9)}`, data);
      else if (action === "status") result = await call("task.get", data);
      else if (action === "attach") result = await call("task.attach", data);
      else if (action === "control") result = await call("task.control", data);
      else if (action === "archive") result = await call("archive.list", { ...data, sessionId });
      else if (action === "inbox") {
        if (data.clear === true) { journal.state.inbox = {}; journal.save(); result = { cleared: true }; }
        else result = journal.state.inbox;
      }
      else if (action === "identity") result = { sessionId, incarnation, machineId: config.machineId, ...local(ctx), workerId: sessionId, workerIdentitySource: "native-session" };
      else throw new Error("Use identity/inbox, submit/status, accept/decline, address/readback/confirm/handoff, hold/clarify/unable, attach/control, participant-consent/consent/optout/archive; JSON arguments required");
      // Status/readback is user-visible, not model context. Never echo credentials or arbitrary server errors.
      notify(ctx, JSON.stringify(result));
    } catch { notify(ctx, "Coordinator command failed; no automatic retry. Check exact JSON fields, authorization and connection.", "error"); }
  }) });
}
