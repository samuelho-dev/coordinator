import { Clock, Context, Crypto, Deferred, Effect, Layer, type Scope } from "effect";
import { Hex } from "effect/encoding";
import { equalDigests } from "./platform.ts";
import {
  INSPECTION_LIMIT, HttpError, sessionRefKey, unavailableMetric,
  type ConnectorAccepted, type ConnectorAnnounce, type ConnectorDisconnect, type ConnectorPoll,
  type ConnectorPollResult, type ConnectorResult, type InspectorConfig, type InspectorPrincipal,
  type ProcessIdentity, type ProcessObservation, type SessionInspection, type SessionInspectInput, type SessionListInput,
  type SessionListOutput, type SessionPresence, type SessionRef,
} from "./inspection.ts";

const LIVE_MS = 30_000;
const EXPIRE_MS = 120_000;
const REQUEST_MS = 10_000;
const MAX_CONNECTORS = 256;
const MAX_SESSIONS = 4096;
const MAX_PENDING = 1024;
const MAX_PER_CONNECTOR = 32;
const MAX_CURSORS = 1024;
const accepted: ConnectorAccepted = { accepted: true };
const error = (status: number, message: string) => new HttpError({ status, message });
const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;
const key = (owner: string, id: string) => JSON.stringify([owner, id]);
const processKey = (process: ProcessIdentity) => JSON.stringify([process.pid, process.startIdentity]);
const machineProcessKey = (machineId: string, process: ProcessIdentity) => JSON.stringify([machineId, process.pid, process.startIdentity]);

export interface InspectorService {
  readonly authorize: (token: string) => Effect.Effect<InspectorPrincipal, HttpError>;
  readonly list: (principal: InspectorPrincipal, input: SessionListInput) => Effect.Effect<SessionListOutput, HttpError>;
  readonly inspect: (principal: InspectorPrincipal, input: SessionInspectInput) => Effect.Effect<SessionInspection, HttpError>;
  readonly announce: (principal: InspectorPrincipal, input: ConnectorAnnounce) => Effect.Effect<ConnectorAccepted, HttpError>;
  readonly poll: (principal: InspectorPrincipal, input: ConnectorPoll) => Effect.Effect<ConnectorPollResult, HttpError>;
  readonly complete: (principal: InspectorPrincipal, input: ConnectorResult) => Effect.Effect<ConnectorAccepted, HttpError>;
  readonly disconnect: (principal: InspectorPrincipal, input: ConnectorDisconnect) => Effect.Effect<ConnectorAccepted, HttpError>;
}

type Connector = {
  principalId: string; owner: string; machineId: string; connectorId: string; incarnation: string;
  receivedAt: number; seen: number; disconnected: boolean; retired: Set<string>;
};
type Session = { presence: SessionPresence; connector: Connector; claims: ReadonlyArray<ProcessIdentity> };
type Pending = {
  requestId: string; sessionKey: string; ref: SessionRef; connector: Connector; incarnation: string;
  deadline: number; delivered: boolean; reply: Deferred.Deferred<SessionInspection, HttpError>;
};
type Cursor = { owner: string; machineId: string | undefined; harness: string | undefined; after: string; expires: number };

const make = (config: InspectorConfig): Effect.Effect<InspectorService, HttpError, Scope.Scope | Crypto.Crypto> => Effect.gen(function*() {
  const crypto = yield* Crypto.Crypto;
  const uuid = crypto.randomUUIDv4().pipe(Effect.mapError(() => error(500, "Inspector randomness unavailable")));
  const clock = yield* Clock.Clock;
  const principals = new Map<string, InspectorPrincipal>();
  const credentials = new Map<string, Uint8Array>();
  const hashes = new Set<string>();
  for (const principal of config.principals) {
    if (principals.has(principal.id) || hashes.has(principal.tokenHash) || !/^[a-f0-9]{64}$/.test(principal.tokenHash)) {
      return yield* Effect.fail(error(500, "Ambiguous inspector credentials"));
    }
    principals.set(principal.id, { ...principal });
    credentials.set(principal.id, yield* Effect.fromResult(Hex.decode(principal.tokenHash)).pipe(Effect.mapError(() => error(500, "Invalid inspector credentials"))));
    hashes.add(principal.tokenHash);
  }
  const connectors = new Map<string, Connector>();
  const sessions = new Map<string, Session>();
  const pending = new Map<string, Pending>();
  const cursors = new Map<string, Cursor>();
  let closed = false;
  const now = () => Number(clock.monotonicTimeNanosUnsafe() / 1_000_000n);
  const wall = () => clock.currentTimeMillisUnsafe();
  const failPending = (request: Pending, failure: HttpError) => {
    pending.delete(request.requestId);
    Deferred.doneUnsafe(request.reply, Effect.fail(failure));
  };
  const removeSession = (id: string) => {
    sessions.delete(id);
    for (const request of pending.values()) if (request.sessionKey === id) failPending(request, error(409, "Native session disconnected"));
  };
  const prune = () => {
    const time = now();
    for (const request of pending.values()) if (time >= request.deadline) failPending(request, error(504, "Native session observation timed out"));
    for (const [id, connector] of connectors) {
      if (time - connector.seen < EXPIRE_MS) continue;
      for (const [sessionId, session] of sessions) if (session.connector === connector) removeSession(sessionId);
      connectors.delete(id);
    }
    for (const [id, cursor] of cursors) if (time >= cursor.expires) cursors.delete(id);
  };
  const authenticated = (principal: InspectorPrincipal) => {
    const configured = principals.get(principal.id);
    return !closed && configured !== undefined && configured.owner === principal.owner && configured.tokenHash === principal.tokenHash;
  };
  const live = (connector: Connector) => !connector.disconnected && now() - connector.seen < LIVE_MS;
  const connection = (connector: Connector): NonNullable<SessionPresence["connection"]> => ({
    state: live(connector) ? "live" : "stale", receivedAt: connector.receivedAt,
    liveUntil: connector.disconnected ? connector.receivedAt : connector.receivedAt + LIVE_MS,
  });
  const sharedProcesses = (owner: string) => {
    const seen = new Set<string>();
    const shared = new Set<string>();
    for (const session of sessions.values()) {
      if (session.connector.owner !== owner || !live(session.connector)) continue;
      const claims = new Set(session.claims.map((process) => machineProcessKey(session.presence.ref.machineId, process)));
      for (const claim of claims) {
        if (seen.has(claim)) shared.add(claim);
        else seen.add(claim);
      }
    }
    return shared;
  };
  const sharedProcess = (session: Session, process: ProcessIdentity, shared: Set<string>) =>
    shared.has(machineProcessKey(session.presence.ref.machineId, process));
  const normalizedPresence = (session: Session, shared: Set<string>, presence = session.presence): SessionPresence => ({
    ...presence, connection: connection(session.connector),
    ...(presence.processes === undefined ? {} : {
      processes: presence.processes.map((process): ProcessIdentity => sharedProcess(session, process, shared) ? { ...process, ownership: "shared" } : process),
    }),
  });
  const normalizedInspection = (session: Session, observation: SessionInspection): SessionInspection => {
    const shared = sharedProcesses(session.connector.owner);
    const seen = new Set<string>();
    let ambiguous = session.claims.some((process) => process.ownership !== "owned" || sharedProcess(session, process, shared));
    const processes = observation.resources.processes.flatMap((entry): ReadonlyArray<ProcessObservation> => {
      const id = processKey(entry.process);
      if (seen.has(id)) { ambiguous = true; return []; }
      seen.add(id);
      const isShared = sharedProcess(session, entry.process, shared) || entry.process.ownership === "shared";
      if (entry.process.ownership !== "owned" || isShared) ambiguous = true;
      if (!isShared) return [entry];
      return [{
        ...entry, process: { ...entry.process, ownership: "shared" },
        cpuPercent: { ...entry.cpuPercent, provenance: { ...entry.cpuPercent.provenance, shared: true } },
        cpuTimeMs: { ...entry.cpuTimeMs, provenance: { ...entry.cpuTimeMs.provenance, shared: true } },
        rssBytes: { ...entry.rssBytes, provenance: { ...entry.rssBytes.provenance, shared: true } },
      }];
    });
    const resources = observation.resources;
    return {
      ...observation, presence: normalizedPresence(session, shared, observation.presence), queriedAt: wall(),
      resources: {
        ...resources, processes,
        ...(ambiguous ? {
          ownedProcessCount: unavailableMetric("count", "Process ownership is shared or unattributable", { ...resources.ownedProcessCount.provenance, shared: true, partial: true }),
          ownedCpuPercent: unavailableMetric("percent-one-core", "Process ownership is shared or unattributable", { ...resources.ownedCpuPercent.provenance, shared: true, partial: true }),
          ownedRssBytes: unavailableMetric("bytes", "Process ownership is shared or unattributable", { ...resources.ownedRssBytes.provenance, shared: true, partial: true }),
        } : {}),
      },
    };
  };
  const findConnector = (principal: InspectorPrincipal, input: ConnectorPoll) => {
    const connector = connectors.get(key(principal.owner, input.connectorId));
    return connector && connector.principalId === principal.id && connector.incarnation === input.incarnation && !connector.disconnected ? connector : undefined;
  };
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    closed = true;
    for (const request of pending.values()) failPending(request, error(503, "Inspector disconnected"));
    connectors.clear(); sessions.clear(); cursors.clear();
  }));

  return {
    authorize: (token: string): Effect.Effect<InspectorPrincipal, HttpError> => Effect.gen(function*() {
      const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(token)).pipe(Effect.mapError(() => error(500, "Inspector digest unavailable")));
      let match: InspectorPrincipal | undefined;
      // Compare every fixed-size digest, including after a match.
      for (const [id, hash] of credentials) if (yield* equalDigests(digest, hash)) match = principals.get(id);
      return match && !closed ? { ...match } : yield* error(401, "Unauthorized");
    }),
    list: (principal: InspectorPrincipal, input: SessionListInput): Effect.Effect<SessionListOutput, HttpError> => uuid.pipe(Effect.flatMap((next) => Effect.suspend<SessionListOutput, HttpError, never>(() => {
      if (!authenticated(principal)) return Effect.fail(error(401, "Unauthorized"));
      prune();
      const cursor = input.cursor === undefined ? undefined : cursors.get(input.cursor);
      if (input.cursor !== undefined && (!cursor || cursor.owner !== principal.owner || cursor.machineId !== input.machineId || cursor.harness !== input.harness)) return Effect.fail(error(400, "Invalid session cursor"));
      const entries = Array.from(sessions.entries()).filter(([id, session]) =>
        session.connector.owner === principal.owner && (cursor === undefined || id > cursor.after) &&
        (input.machineId === undefined || session.presence.ref.machineId === input.machineId) &&
        (input.harness === undefined || session.presence.ref.harness === input.harness),
      ).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
      const page: Array<SessionPresence> = [];
      const shared = sharedProcesses(principal.owner);
      let after: string | undefined;
      for (const [id, session] of entries) {
        if (page.length >= input.limit) break;
        const presence = normalizedPresence(session, shared);
        page.push(presence);
        if (bytes({ sessions: page, nextCursor: next }) > INSPECTION_LIMIT) {
          page.pop();
          if (page.length === 0) return Effect.fail(error(413, "Native session presence exceeds response limit"));
          break;
        }
        after = id;
      }
      if (page.length < entries.length && after !== undefined) {
        if (cursors.size >= MAX_CURSORS) {
          const oldest = cursors.keys().next().value;
          if (oldest !== undefined) cursors.delete(oldest);
        }
        cursors.set(next, { owner: principal.owner, machineId: input.machineId, harness: input.harness, after, expires: now() + EXPIRE_MS });
        return Effect.succeed({ sessions: page, nextCursor: next });
      }
      return Effect.succeed({ sessions: page, nextCursor: null });
    }))),
    inspect: (principal: InspectorPrincipal, input: SessionInspectInput): Effect.Effect<SessionInspection, HttpError> => uuid.pipe(Effect.flatMap((requestId) => Effect.uninterruptibleMask((restore) => Effect.suspend(() => {
      if (!authenticated(principal)) return Effect.fail(error(401, "Unauthorized"));
      prune();
      const id = key(principal.owner, sessionRefKey(input.ref));
      const session = sessions.get(id);
      if (!session) return Effect.fail(error(404, "Native session not found"));
      if (!live(session.connector)) return Effect.fail(error(409, "Native session is stale"));
      let count = 0;
      for (const request of pending.values()) if (request.connector === session.connector) count++;
      if (pending.size >= MAX_PENDING || count >= MAX_PER_CONNECTOR) return Effect.fail(error(429, "Inspection capacity reached"));
      const request: Pending = {
        requestId, sessionKey: id, ref: session.presence.ref, connector: session.connector,
        incarnation: session.connector.incarnation, deadline: now() + REQUEST_MS, delivered: false,
        reply: Deferred.makeUnsafe<SessionInspection, HttpError>(),
      };
      pending.set(request.requestId, request);
      return restore(Deferred.await(request.reply).pipe(
        Effect.timeoutOrElse({ duration: REQUEST_MS, orElse: () => Effect.fail(error(504, "Native session observation timed out")) }),
      )).pipe(
        Effect.ensuring(Effect.sync(() => { pending.delete(request.requestId); })),
      );
    })))),
    announce: (principal: InspectorPrincipal, input: ConnectorAnnounce): Effect.Effect<ConnectorAccepted, HttpError> => Effect.suspend(() => {
      if (!authenticated(principal)) return Effect.fail(error(401, "Unauthorized"));
      if (input.sessions.some((session) => session.ref.machineId !== input.machineId || (session.parent !== undefined && session.parent.machineId !== input.machineId))) return Effect.fail(error(403, "Machine binding mismatch"));
      prune();
      if (input.sessions.length > 256) return Effect.fail(error(413, "Native session capacity reached"));
      const ids = new Set(input.sessions.map((session) => key(principal.owner, sessionRefKey(session.ref))));
      if (ids.size !== input.sessions.length) return Effect.fail(error(409, "Duplicate native session"));
      const connectorKey = key(principal.owner, input.connectorId);
      const previous = connectors.get(connectorKey);
      if (previous && (previous.principalId !== principal.id || previous.machineId !== input.machineId)) return Effect.fail(error(403, "Connector binding mismatch"));
      if (previous && previous.incarnation !== input.incarnation && (live(previous) || previous.retired.has(input.incarnation) || previous.retired.size >= 16)) return Effect.fail(error(409, "Connector incarnation conflict"));
      if (previous?.disconnected && previous.incarnation === input.incarnation) return Effect.fail(error(409, "Connector incarnation retired"));
      if (!previous && connectors.size >= MAX_CONNECTORS) return Effect.fail(error(429, "Connector capacity reached"));
      let newCount = 0;
      let removedCount = 0;
      for (const [id, session] of sessions) if (session.connector === previous && !ids.has(id)) removedCount++;
      for (const id of ids) {
        const current = sessions.get(id);
        if (!current) newCount++;
        else if (current.connector !== previous && live(current.connector)) return Effect.fail(error(409, "Native session already has an active publisher"));
      }
      if (sessions.size + newCount - removedCount > MAX_SESSIONS) return Effect.fail(error(429, "Native session capacity reached"));
      // UUID cursors always occupy 36 ASCII bytes; validate presence before admitting it.
      for (const presence of input.sessions) if (bytes({ sessions: [{ ...presence, connection: { state: "live", receivedAt: wall(), liveUntil: wall() + LIVE_MS } }], nextCursor: "0".repeat(36) }) > INSPECTION_LIMIT) return Effect.fail(error(413, "Native session presence exceeds response limit"));
      let connector = previous;
      if (!connector || connector.incarnation !== input.incarnation) {
        const retired = previous?.retired ?? new Set<string>();
        if (previous) retired.add(previous.incarnation);
        connector = { principalId: principal.id, owner: principal.owner, machineId: input.machineId, connectorId: input.connectorId, incarnation: input.incarnation, receivedAt: wall(), seen: now(), disconnected: false, retired };
        if (previous) for (const [id, session] of sessions) if (session.connector === previous) removeSession(id);
        connectors.set(connectorKey, connector);
      }
      connector.receivedAt = wall(); connector.seen = now();
      for (const [id, session] of sessions) if (session.connector === connector && !ids.has(id)) removeSession(id);
      for (const presence of input.sessions) {
        const id = key(principal.owner, sessionRefKey(presence.ref));
        const existing = sessions.get(id);
        if (existing && existing.connector !== connector) removeSession(id);
        sessions.set(id, { presence: { ...presence, connection: connection(connector) }, connector, claims: presence.processes ?? [] });
      }
      return Effect.succeed(accepted);
    }),
    poll: (principal: InspectorPrincipal, input: ConnectorPoll): Effect.Effect<ConnectorPollResult, HttpError> => Effect.suspend(() => {
      if (!authenticated(principal)) return Effect.fail(error(401, "Unauthorized"));
      prune();
      const connector = findConnector(principal, input);
      if (!connector || !live(connector)) return Effect.fail(error(409, "Connector must announce presence"));
      const requests: Array<{ requestId: string; ref: SessionRef }> = [];
      for (const request of pending.values()) {
        if (request.connector !== connector || request.delivered || request.incarnation !== connector.incarnation) continue;
        const entry = { requestId: request.requestId, ref: request.ref };
        requests.push(entry);
        if (bytes({ requests }) > INSPECTION_LIMIT) { requests.pop(); break; }
        request.delivered = true;
        if (requests.length >= MAX_PER_CONNECTOR) break;
      }
      return Effect.succeed({ requests });
    }),
    complete: (principal: InspectorPrincipal, input: ConnectorResult): Effect.Effect<ConnectorAccepted, HttpError> => Effect.suspend(() => {
      if (!authenticated(principal)) return Effect.fail(error(401, "Unauthorized"));
      prune();
      const connector = findConnector(principal, input);
      const request = pending.get(input.requestId);
      if (!connector || !request || request.connector !== connector || request.incarnation !== input.incarnation || !request.delivered || !live(connector)) return Effect.fail(error(409, "Inspection request is no longer active"));
      if ("code" in input.observation) {
        failPending(request, error(input.observation.code === "not-found" ? 404 : 503, "Native session observation unavailable"));
        return Effect.succeed(accepted);
      }
      if (sessionRefKey(input.observation.presence.ref) !== sessionRefKey(request.ref)) return Effect.fail(error(409, "Inspection session mismatch"));
      const session = sessions.get(request.sessionKey);
      if (!session || session.connector !== connector) return Effect.fail(error(409, "Native session publisher changed"));
      const claims = new Map<string, ProcessIdentity>();
      for (const process of [...(session.presence.processes ?? []), ...input.observation.resources.processes.map((entry) => entry.process)]) claims.set(processKey(process), process);
      if (claims.size > 256) return Effect.fail(error(413, "Native process identity capacity reached"));
      session.claims = Array.from(claims.values());
      const observation = normalizedInspection(session, input.observation);
      if (bytes(observation) > INSPECTION_LIMIT) {
        failPending(request, error(413, "Native observation exceeds response limit"));
        return Effect.fail(error(413, "Native observation exceeds response limit"));
      }
      pending.delete(request.requestId);
      Deferred.doneUnsafe(request.reply, Effect.succeed(observation));
      return Effect.succeed(accepted);
    }),
    disconnect: (principal: InspectorPrincipal, input: ConnectorDisconnect): Effect.Effect<ConnectorAccepted, HttpError> => Effect.suspend(() => {
      if (!authenticated(principal)) return Effect.fail(error(401, "Unauthorized"));
      prune();
      const connector = findConnector(principal, input);
      if (!connector) return Effect.fail(error(409, "Connector incarnation mismatch"));
      connector.disconnected = true;
      connector.receivedAt = wall();
      for (const request of pending.values()) if (request.connector === connector) failPending(request, error(409, "Native session disconnected"));
      return Effect.succeed(accepted);
    }),
  };
});

export class Inspector extends Context.Service<Inspector, InspectorService>()("coordinator/Inspector") {
  static readonly make = make;
  static readonly layer = (config: InspectorConfig) => Layer.effect(Inspector, make(config));
}
