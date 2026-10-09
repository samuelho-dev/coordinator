import { Schema } from "effect";
import { NativeBinding, NativeControlError, NativeEventPage, NativeEventSequence, NativeLifecycleEmission, NativeLifecycleEvent, NativeMcpResultBytes, nativeEventsUri } from "./contracts.js";

const decodeEmission = Schema.decodeUnknownSync(NativeLifecycleEmission, { onExcessProperty: "error" });
const decodeEvent = Schema.decodeUnknownSync(NativeLifecycleEvent, { onExcessProperty: "error" });
const decodeBinding = Schema.decodeUnknownSync(NativeBinding, { onExcessProperty: "error" });
const sequence = Schema.decodeUnknownSync(NativeEventSequence);
const utf8 = new TextEncoder();
const eventText = Schema.encodeUnknownSync(Schema.fromJsonString(NativeLifecycleEvent), { onExcessProperty: "error" });
const escapedText = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.String));

export class NativeLifecycleJournal {
  readonly #retention: number;
  readonly #maxPageSize: number;
  readonly #entries: NativeLifecycleEvent[] = [];
  readonly #listeners = new Set<(event: NativeLifecycleEvent) => void>();
  #highWater = sequence(0);
  #evictedThrough = sequence(0);
  #fanoutThrough = sequence(0);
  #fanoutScheduled = false;
  readonly #flushFanout = (): void => {
    const through = this.#highWater;
    // Snapshot retained rows only. Reentrant appends cannot reorder this batch or grow a pending queue.
    const retained = this.#entries.filter((event) => event.seq > this.#fanoutThrough && event.seq <= through);
    const listeners = Array.from(this.#listeners);
    for (const event of retained) {
      for (const listener of listeners) {
        if (!this.#listeners.has(listener)) continue;
        try { listener(event); } catch { /* Subscriber failures cannot roll back committed metadata. */ }
      }
    }
    this.#fanoutThrough = through;
    this.#fanoutScheduled = false;
    if (this.#highWater > through) {
      this.#fanoutScheduled = true;
      queueMicrotask(this.#flushFanout);
    }
  };

  constructor(options: { readonly retention?: number; readonly maxPageSize?: number } = {}) {
    this.#retention = options.retention ?? 1024;
    this.#maxPageSize = options.maxPageSize ?? 256;
    if (!Number.isSafeInteger(this.#retention) || this.#retention < 1 || this.#retention > 65536 ||
        !Number.isSafeInteger(this.#maxPageSize) || this.#maxPageSize < 1 || this.#maxPageSize > 256) {
      throw new NativeControlError({ code: "invalid_input", message: "Invalid lifecycle journal bounds" });
    }
  }

  get highWater(): NativeEventSequence { return this.#highWater; }

  append(emission: NativeLifecycleEmission): NativeLifecycleEvent {
    if (this.#highWater === Number.MAX_SAFE_INTEGER) {
      throw new NativeControlError({ code: "sequence_exhausted", message: "Native lifecycle sequence exhausted" });
    }
    const metadata = decodeEmission(emission);
    const event = decodeEvent({ ...metadata, seq: sequence(this.#highWater + 1) });
    Object.freeze(event.binding);
    if (event.kind === "session_retired" && event.replacement) Object.freeze(event.replacement);
    Object.freeze(event);
    this.#entries.push(event);
    this.#highWater = event.seq;
    if (this.#entries.length > this.#retention) {
      const evicted = this.#entries.shift();
      if (evicted) this.#evictedThrough = evicted.seq;
    }
    // Native physical fences unwind before any subscriber runs. Fanout is a retained-event hint, not delivery.
    if (!this.#fanoutScheduled) {
      this.#fanoutScheduled = true;
      queueMicrotask(this.#flushFanout);
    }
    return event;
  }

  read(binding: NativeBinding, after: NativeEventSequence, limit = this.#maxPageSize): NativeEventPage {
    const identity = decodeBinding(binding);
    const cursor = sequence(after);
    if (cursor > this.#highWater || !Number.isSafeInteger(limit) || limit < 1 || limit > this.#maxPageSize) {
      throw new NativeControlError({ code: "invalid_input", message: "Invalid lifecycle resource cursor or page limit" });
    }
    const gap = cursor < this.#evictedThrough ? Object.freeze({ from: sequence(cursor + 1), through: this.#evictedThrough }) : null;
    const entries: NativeLifecycleEvent[] = [];
    const pageBase = { binding: identity, after: cursor, oldestAvailable: this.#entries[0]?.seq ?? this.#highWater, highWater: this.#highWater, gap };
    const baseBytes = utf8.encode(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))({
      result: { contents: [{ uri: nativeEventsUri(identity), mimeType: "application/json",
        text: Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))({ ...pageBase, entries: [], nextAfter: this.#highWater, more: false }),
      }] },
    })).byteLength;
    let encodedEntriesBytes = 0;
    let more = false;
    const available = this.#entries.filter((event) => event.seq > cursor && event.binding.host === identity.host &&
      event.binding.sessionId === identity.sessionId && event.binding.incarnation === identity.incarnation && event.binding.generation === identity.generation);
    for (const event of available) {
      if (entries.length === limit) { more = true; break; }
      const entryBytes = utf8.encode(escapedText(eventText(event))).byteLength - 2 + (entries.length === 0 ? 0 : 1);
      const candidateMore = entries.length + 1 < available.length;
      // JSON's true is one byte shorter; the cursor contains only unescaped decimal digits.
      const metadataBytes = candidateMore ? -1 + String(event.seq).length - String(this.#highWater).length : 0;
      if (baseBytes + encodedEntriesBytes + entryBytes + metadataBytes > NativeMcpResultBytes) { more = true; break; }
      encodedEntriesBytes += entryBytes;
      entries.push(event);
    }
    const last = entries.at(-1);
    const nextAfter = more && last ? last.seq : this.#highWater;
    return Object.freeze({ ...pageBase, binding: Object.freeze(identity),
      entries: Object.freeze(entries), nextAfter, more });
  }

  subscribe(listener: (event: NativeLifecycleEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }
}
