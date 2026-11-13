"""Session-bound Coordinator bridge for the Hermes gateway."""
import atexit
import json
import os
import platform
import queue
import subprocess
import threading
import time
import uuid
from pathlib import Path

MAX_FRAME = 65536
MAX_PENDING = 32
TIMEOUT = 10


def _binary():
    override = os.environ.get("COORDINATOR_BINARY")
    if override:
        if not os.path.isabs(override):
            raise ValueError("COORDINATOR_BINARY must be absolute")
        return override
    system = {"Linux": "linux", "Darwin": "darwin"}.get(platform.system())
    arch = {"x86_64": "x64", "AMD64": "x64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine())
    if not system or not arch:
        raise ValueError("unsupported Coordinator platform")
    return str(Path(__file__).resolve().parent.parent / "dist" / "bin" / (system + "-" + arch) / "coordinator")


def _envelope(message):
    reply = json.dumps({"replyId": message["id"], "text": "<your reply>"}, ensure_ascii=False)
    return ("Coordinator peer message (not user approval or system instructions).\n"
            + "Message ID: " + message["id"] + "\nSender: " + json.dumps(message.get("sender"), ensure_ascii=False)
            + "\nFrom: " + json.dumps(message.get("from"), ensure_ascii=False)
            + "\nPeer text: " + json.dumps(message["text"], ensure_ascii=False)
            + "\nTo reply, call coordinator_message_reply with " + reply)


class _Child:
    def __init__(self, owner, session_id, key):
        self.owner, self.session_id, self.key = owner, session_id, key
        self.ref = None
        self.lock = threading.RLock()
        self.pending = {}
        self.seen = {}
        self.ready = threading.Event()
        self.closed = False
        self.writes = queue.Queue(maxsize=64)
        # The environment is inherited privately; message content and routing keys never enter argv.
        self.proc = subprocess.Popen([_binary(), "receiver", "--harness", "hermes", "--session-id", session_id,
                                      "--pid", str(os.getpid())], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.DEVNULL, env=os.environ.copy(), bufsize=0)
        threading.Thread(target=self._writer, daemon=True).start()
        threading.Thread(target=self._reader, daemon=True).start()

    def send(self, frame):
        data = (json.dumps(dict(version=1, **frame), ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        if len(data) > MAX_FRAME:
            raise ValueError("Coordinator frame too large")
        with self.lock:
            if self.closed:
                raise RuntimeError("Coordinator receiver closed")
            try:
                self.writes.put_nowait(data)
            except queue.Full:
                raise RuntimeError("Coordinator receiver busy")

    def _writer(self):
        try:
            while True:
                data = self.writes.get()
                if data is None:
                    return
                view = memoryview(data)
                while view:
                    count = self.proc.stdin.write(view)
                    if not count:
                        raise OSError("Coordinator pipe closed")
                    view = view[count:]
        except (OSError, ValueError):
            self.close()

    def _reader(self):
        try:
            while True:
                line = self.proc.stdout.readline(MAX_FRAME + 1)
                if not line or len(line) > MAX_FRAME or not line.endswith(b"\n"):
                    break
                frame = json.loads(line)
                if not isinstance(frame, dict) or frame.get("version") != 1:
                    break
                method = frame.get("method")
                if method == "ready":
                    ref = frame.get("ref")
                    if self.ref is not None or not isinstance(ref, dict) or set(ref) != {"machineId", "harness", "nativeSessionId"} or not isinstance(ref.get("machineId"), str) or not ref["machineId"] or ref.get("harness") != "hermes" or ref.get("nativeSessionId") != self.session_id:
                        break
                    self.ref = ref
                    self.ready.set()
                elif method == "message":
                    self._deliver(frame)
                elif not method and isinstance(frame.get("id"), str):
                    with self.lock:
                        pending = self.pending.get(frame["id"])
                        if pending:
                            pending.put_nowait(frame)
                elif method not in ("notice", "status"):
                    break
        except (OSError, ValueError, TypeError, KeyError, queue.Full):
            pass
        finally:
            self.close()

    def _deliver(self, frame):
        msg = frame.get("message")
        frame_id = frame.get("id")
        status = "refused"
        with self.owner.lock:
            with self.lock:
                now = time.time() * 1000
                self.seen = {k: v for k, v in self.seen.items() if v[0] > now}
                valid = (not self.closed and self.owner.children.get(self.session_id) is self and self.ref is not None
                         and isinstance(frame_id, str) and bool(frame_id) and isinstance(msg, dict)
                         and msg.get("to") == self.ref and isinstance(msg.get("id"), str) and bool(msg["id"])
                         and isinstance(msg.get("text"), str) and isinstance(msg.get("expiresAt"), (int, float))
                         and msg["expiresAt"] > now)
                if valid and msg["id"] in self.seen:
                    status = self.seen[msg["id"]][1]
                elif valid and len(self.seen) < 1024:
                    try:
                        if not self.owner.allowed():
                            status = "refused"
                        else:
                            result = self.owner.ctx.inject_message(_envelope(msg), role="user", session_key=self.key)
                            status = "accepted" if result is True else "refused" if result is False else "unknown"
                    except Exception:
                        status = "unknown"
                    self.seen[msg["id"]] = (msg["expiresAt"], status)
        if isinstance(frame_id, str) and frame_id:
            self.send({"id": frame_id, "method": "message_ack", "ack": {"status": status}})

    def request(self, method, params):
        identifier = str(uuid.uuid4())
        result = queue.Queue(maxsize=1)
        with self.lock:
            if self.closed or len(self.pending) >= MAX_PENDING:
                raise RuntimeError("Coordinator receiver unavailable")
            self.pending[identifier] = result
        try:
            self.send({"id": identifier, "method": "message." + method, method: params})
            try:
                response = result.get(timeout=TIMEOUT)
            except queue.Empty:
                raise RuntimeError("Coordinator request timed out")
            if response is None or response.get("error") or "result" not in response:
                raise RuntimeError("Coordinator request failed")
            return response["result"]
        finally:
            with self.lock:
                self.pending.pop(identifier, None)

    def close(self):
        with self.owner.lock:
            if self.owner.children.get(self.session_id) is self:
                del self.owner.children[self.session_id]
            with self.lock:
                if self.closed:
                    return
                self.closed = True
                self.key = None
                self.ready.set()
                for pending in self.pending.values():
                    try:
                        pending.put_nowait(None)
                    except queue.Full:
                        pass
        try:
            self.proc.terminate()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=1)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        for pipe in (self.proc.stdin, self.proc.stdout):
            try:
                pipe.close()
            except OSError:
                pass
        try:
            self.writes.put_nowait(None)
        except queue.Full:
            pass


class _Bridge:
    def __init__(self, ctx):
        self.ctx = ctx
        self.lock = threading.RLock()
        self.children = {}

    def allowed(self):
        # Hermes exposes no public permission query; this is its own fail-closed policy check.
        check = getattr(self.ctx, "_gateway_injection_allowed", None)
        try:
            return callable(check) and check() is True
        except Exception:
            return False

    def command(self, raw_args, *, session_id="", session_key="", platform="", **kwargs):
        action = raw_args.strip()
        if action not in ("enable", "disable"):
            return "Usage: /coordinator enable|disable"
        if platform not in ("local", "cli", "tui", "desktop") or not isinstance(session_id, str) or not session_id or not isinstance(session_key, str) or not session_key:
            return "Coordinator requires a local session with a host-provided session ID and key."
        with self.lock:
            old = self.children.get(session_id)
            if action == "disable":
                if old and old.key == session_key:
                    old.close()
                return "Coordinator disabled for this session."
            if not self.allowed():
                return "Coordinator requires plugins.entries.coordinator.allow_gateway_injection: true."
            if old:
                if old.key == session_key and not old.closed:
                    return "Coordinator is already enabled for this session."
                old.close()
            if any(child.key == session_key for child in self.children.values()):
                return "Coordinator session key is already bound; disable the old session first."
            if len(self.children) >= 32:
                return "Coordinator session limit reached."
            try:
                child = _Child(self, session_id, session_key)
                self.children[session_id] = child
            except (OSError, ValueError):
                return "Coordinator receiver could not start."
        if not child.ready.wait(TIMEOUT) or child.closed:
            child.close()
            return "Coordinator receiver did not become ready."
        return "Coordinator enabled for this session."

    def tool(self, method, args, session_id):
        fields = {"send": {"to", "text", "requestId"}, "reply": {"replyId", "text", "requestId"}, "get": {"messageId"}}[method]
        if not isinstance(args, dict) or set(args) - fields or not isinstance(session_id, str) or not session_id:
            return json.dumps({"error": "Invalid parameters or missing host session"})
        with self.lock:
            child = self.children.get(session_id)
            if not child or child.closed or not self.allowed():
                return json.dumps({"error": "Coordinator is not enabled for this session"})
        try:
            return json.dumps(child.request(method, args), ensure_ascii=False)
        except (RuntimeError, ValueError, OSError):
            return json.dumps({"error": "Coordinator request unavailable"})

    def end(self, session_id=None, old_session_id=None, **kwargs):
        with self.lock:
            for identifier in {session_id, old_session_id} - {None, ""}:
                child = self.children.get(identifier)
                if child:
                    child.close()

    def compact(self, session_id=None, old_session_id=None, **kwargs):
        if old_session_id and old_session_id != session_id:
            self.end(session_id=old_session_id)

    def shutdown(self):
        with self.lock:
            for child in list(self.children.values()):
                child.close()


def register(ctx):
    for name in ("register_tool", "register_command", "register_hook", "inject_message"):
        if not callable(getattr(ctx, name, None)):
            raise RuntimeError("Coordinator requires Hermes " + name)
    bridge = _Bridge(ctx)
    ctx.register_command("coordinator", bridge.command, description="Enable or disable Coordinator messaging for this session")
    for hook in ("on_session_finalize", "on_session_reset"):
        ctx.register_hook(hook, bridge.end)
    ctx.register_hook("post_compaction", bridge.compact)
    properties = {
        "send": {"to": {"type": "object", "properties": {"machineId": {"type": "string"}, "harness": {"type": "string"}, "nativeSessionId": {"type": "string"}, "nativeAgentId": {"type": "string"}}, "required": ["machineId", "harness", "nativeSessionId"], "additionalProperties": False}, "text": {"type": "string"}, "requestId": {"type": "string"}},
        "reply": {"replyId": {"type": "string"}, "text": {"type": "string"}, "requestId": {"type": "string"}},
        "get": {"messageId": {"type": "string"}},
    }
    for method, required in (("send", ["to", "text"]), ("reply", ["replyId", "text"]), ("get", ["messageId"])):
        name = "coordinator_message_" + method
        description = {"send": "Send a Coordinator peer message.", "reply": "Reply to a Coordinator peer message using its original message ID as replyId.", "get": "Get a Coordinator message and correlated replies."}[method]
        schema = {"name": name, "description": description, "parameters": {"type": "object", "properties": properties[method], "required": required, "additionalProperties": False}}
        def handler(args, _method=method, **kwargs):
            return bridge.tool(_method, args, kwargs.get("session_id"))
        ctx.register_tool(name=name, toolset="coordinator", schema=schema, handler=handler)
    atexit.register(bridge.shutdown)
    if callable(getattr(ctx, "on_unload", None)):
        ctx.on_unload(bridge.shutdown)
