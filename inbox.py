"""intercom inbox — per-session UDS server + peer registry.

Each gateway publishes its interactive local sessions in the peer registry.
Cross-process traffic uses an owner-only Unix socket; same-process delivery is direct.

Envelope protocol (length-prefixed AES-GCM-sealed JSON over the socket):
  -> {"type": "send", "from_name": ..., "from_cwd": ..., "message": ...}
  <- {"ok": true} | {"ok": false, "error": ...}

Delivery on the receiving side:
  gateway -> authenticated local-session injection
"""

from __future__ import annotations

import math
import stat
import json
import logging
import os
import socket
import struct
import re
from collections import OrderedDict

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import threading
import time
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)

_DIR_NAME = "intercom"
_REGISTRY = "sessions"
_MAX_MSG_CHARS = 8000
_RATE_WINDOW_S = 60
_RATE_MAX_PER_PEER = 6
_DEDUPE_WINDOW_S = 120

_lock = threading.RLock()
_sending = set()
_MAX_WIRE_BYTES = _MAX_MSG_CHARS * 16 + 8192
_NONCE_CAP = 16384
_nonces = OrderedDict()
_ACCEPTED = frozenset(("submitted_as_turn", "reply_resolved"))
_state: dict = {}          # session-scoped singleton state
_rate: dict[tuple[str, str], list[float]] = {}
_dedupe: dict[tuple[str, str], list[tuple[int, float]]] = {}


# ---------------------------------------------------------------------------
# paths / registry helpers
# ---------------------------------------------------------------------------

def _base_dir() -> Path:
    from hermes_constants import get_hermes_home
    return Path(get_hermes_home()) / _DIR_NAME


def _registry_dir() -> Path:
    return _base_dir() / _REGISTRY


def _sanitize_boundary(text: str) -> str:
    """Neutralise boundary-marker injection in untrusted message parts.

    A malicious peer could put a forged ``📡 hermes@... says:`` header line in
    its body to pose as another sender. Insert a zero-width space after the
    emoji so only the real first line parses as the header.
    """
    # Header forgery: a body line mimicking our new header format gets broken
    # by a zero-width space right after the emoji, so only the true first
    # line (added by _wrap) parses as the header.
    text = re.sub(r"📡(?=\s*hermes@[^\n]*says:)", "📡\u200b", text)
    return text


def _sanitize_name(name: str) -> str:
    """Strip newlines/brackets from sender names before framing them."""
    cleaned = "".join(ch for ch in str(name) if ch not in "\r\n[]")
    return cleaned.strip() or "unknown"


def _frame_parts(from_name: str, message: str) -> tuple[str, str]:
    """Return (header_line, sanitized_body)."""
    body = _sanitize_boundary(str(message))
    header = f"📡 hermes@{from_name} says:"
    return header, body


def list_peers() -> tuple[list[dict], dict]:
    """Live peers with busy state + self meta. For the tool's action=list."""
    from . import desktop

    desktop.refresh()
    me = self_meta()
    peers = []
    for identifier, metadata in _load_peers().items():
        if identifier == me.get("session_id"):
            continue
        peers.append({"name": metadata["name"],
                      "session_id": identifier, "cwd": metadata.get("cwd"),
                      "busy": bool(metadata.get("turn_active"))})
    return peers, me


def _load_peers() -> dict:
    """Read live peer metadata from the registry, keyed by endpoint id."""
    peers = {}
    for path in sorted(_registry_dir().glob("*.json")):
        try:
            metadata = json.loads(path.read_text())
            if not isinstance(metadata["name"], str) or not metadata["name"]:
                raise ValueError("invalid peer name")
            os.kill(int(metadata["pid"]), 0)
            if metadata["session_id"] != path.stem:
                raise ValueError("registry endpoint mismatch")
            peers[path.stem] = metadata
        except (OSError, ValueError, KeyError, TypeError):
            path.unlink(missing_ok=True)
    return peers


def ask_to(target: str, message: str, from_name: str = "", from_cwd: str = "",
           timeout: int = 600) -> dict:
    """Send `message` to peer and block until their reply or timeout.

    The reply arrives on our own inbox as type='reply' addressed to
    request_id; the inbox thread hands it to the waiter event.
    """
    resolved = resolve_target(target)
    if not resolved.get("ok"):
        return {**resolved, "action": "ask"}
    request_id = uuid.uuid4().hex
    event = threading.Event()
    waiter = {"evt": event, "reply": None,
              "expected_peer": resolved["meta"]["session_id"]}
    with _lock:
        _state.setdefault("waiters", {})[request_id] = waiter
    try:
        result = send_to(resolved["name"], message, from_name, from_cwd,
                         payload_extra={"type": "ask", "request_id": request_id,
                                        "_resolved_target": resolved})
        if not result.get("ok"):
            return {**result, "action": "ask"}
        if not event.wait(timeout=max(0, min(timeout, 3600))):
            return {"ok": False, "error": "timeout waiting for reply",
                    "request_id": request_id, "action": "ask"}
        reply = waiter["reply"]
        return {"ok": reply is not None, "reply": reply, "from": target, "action": "ask"}
    finally:
        with _lock:
            _state.get("waiters", {}).pop(request_id, None)


def resolve_ask(request_id: str, sender: str, reply_text: str) -> bool:
    """Deliver a reply for a pending ask. True if a local waiter took it."""
    with _lock:
        waiter = _state.get("waiters", {}).get(request_id)
        if waiter is None or waiter.get("expected_peer") != sender or waiter["evt"].is_set():
            return False
        waiter["reply"] = reply_text[:_MAX_MSG_CHARS]
        waiter["evt"].set()
    return True


def reply_to(
    target: str,
    request_id: str,
    message: str,
    from_name: str = "",
    from_cwd: str = "",
) -> dict:
    """Reply to a peer's pending ask using its original request id."""
    return send_to(
        target,
        message,
        from_name,
        from_cwd,
        payload_extra={"type": "reply", "request_id": request_id},
        dedupe=False,
    )


def resolve_target(target: str) -> dict:
    """Resolve a unique live peer name from an exact name or prefix."""
    peers = _load_peers()
    exact = [(identifier, metadata) for identifier, metadata in peers.items()
             if metadata["name"] == target or identifier == target]
    matches = exact or [(identifier, metadata) for identifier, metadata in peers.items()
                        if metadata["name"].startswith(target)]
    if not matches:
        return {"ok": False, "error": f"no live peer named '{target}'",
                "peers": sorted(metadata["name"] for metadata in peers.values())}
    if len(matches) > 1:
        return {"ok": False, "error": "ambiguous name", "candidates": [
            {"name": metadata["name"], "session_id": identifier}
            for identifier, metadata in matches]}
    _, metadata = matches[0]
    return {"ok": True, "name": metadata["name"], "meta": metadata}


def _exchange(meta: dict, payload: dict) -> dict:
    """Exchange one authenticated-local envelope with a registered peer."""
    from . import desktop

    sender = self_meta().get("session_id", "")
    local = desktop._metadata.get(meta.get("session_key"))
    if local is not None and local["session_id"] == meta["session_id"]:
        return _receive({**payload, "session_key": meta["session_key"]}, sender)
    destination = meta["session_id"]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(5)
            try:
                connection.connect(meta["socket"])
            except OSError:
                _drop_peer(meta)
                raise
            if _peer_uid(connection) != os.getuid():
                raise PermissionError("server uid mismatch")
            _send_frame(connection, _seal({**payload, "session_key": meta.get("session_key", "")}, sender, destination))
            envelope = _read_frame(connection)
            if envelope["sender"] != destination:
                raise ValueError("receipt sender mismatch")
            reply = _open(envelope, destination=sender)
            if envelope["type"] != "receipt" or envelope["request_id"] != payload.get("request_id", ""):
                raise ValueError("receipt correlation mismatch")
            return reply
    except (OSError, ValueError, KeyError, TypeError, EOFError, InvalidTag) as exception:
        return {"ok": False, "error": f"delivery failed: {exception}"}


def send_to(target: str, message: str, from_name: str = "", from_cwd: str = "",
            payload_extra: dict | None = None, *, dedupe: bool = True) -> dict:
    """Send `message` to peer `target`. Returns status dict."""
    extra = dict(payload_extra or {})
    resolved = extra.pop("_resolved_target", None) or resolve_target(target)
    if not resolved.get("ok"):
        return resolved
    name, metadata = resolved["name"], resolved["meta"]
    me = self_meta()
    if (metadata.get("session_key") and metadata["session_key"] == me.get("session_key")) or (
        not metadata.get("session_key") and int(metadata.get("pid", -1)) == os.getpid()
    ):
        return {"ok": False, "error": "target is the current session"}
    payload = {"type": "send", "from_name": from_name, "from_cwd": from_cwd,
               "message": message[:_MAX_MSG_CHARS], **extra}
    if payload["type"] in ("ask", "reply") and not re.fullmatch(r"[0-9a-f]{32}", str(payload.get("request_id", ""))):
        return {"ok": False, "error": "invalid request id"}
    peer = (me.get("session_id", ""), metadata.get("session_id", name))
    digest = payload.get("request_id") if payload["type"] == "ask" else hash(payload["message"])
    with _lock:
        _prune_windows(time.time())
        if peer in _sending:
            return {"ok": False, "error": "delivery already in progress for this peer"}
        if len(_rate.get(peer, [])) >= _RATE_MAX_PER_PEER:
            return {"ok": False, "error": "rate limit reached for this peer"}
        if dedupe and any(value == digest for value, timestamp in _dedupe.get(peer, [])):
            return {"ok": False, "error": "identical message already sent recently"}
        _sending.add(peer)
    try:
        reply = _exchange(metadata, payload)
        status = reply.get("status")
        accepted = reply.get("ok") is True and status in _ACCEPTED
        if not accepted:
            return {**reply, "ok": False, "receipt": "refused", "to": name}
        with _lock:
            _rate.setdefault(peer, []).append(time.time())
            if dedupe:
                _dedupe.setdefault(peer, []).append((digest, time.time()))
    finally:
        with _lock:
            _sending.discard(peer)
    return {"ok": True, "receipt": "delivered", "delivered_as": status, "to": name}


# ---------------------------------------------------------------------------
# inbox server thread
# ---------------------------------------------------------------------------

def _peer_uid(conn: socket.socket) -> int:
    """Return the effective UID authenticated by the local socket transport."""
    getpeereid = getattr(conn, "getpeereid", None)
    if getpeereid is not None:
        uid, _gid = getpeereid()
        return int(uid)
    local_peercred = getattr(socket, "LOCAL_PEERCRED", None)
    if local_peercred is not None:
        raw = conn.getsockopt(0, local_peercred, 12)
        _version, uid, _groups = struct.unpack("@iIh", raw[:10])
        return int(uid)
    so_peercred = getattr(socket, "SO_PEERCRED", None)
    if so_peercred is not None:
        raw = conn.getsockopt(socket.SOL_SOCKET, so_peercred, 12)
        _pid, uid, _gid = struct.unpack("3i", raw)
        return int(uid)
    raise PermissionError("peer uid authentication is unavailable on this platform")


def _receive(request: dict, sender: str) -> dict:
    request_type = request["type"]
    if request_type not in ("send", "ask", "reply"):
        raise ValueError("unsupported envelope type")
    message = str(request.get("message", ""))[:_MAX_MSG_CHARS]
    if request_type == "reply":
        accepted = resolve_ask(request["request_id"], sender, message)
        response = {"ok": accepted, "status": "reply_resolved" if accepted else "no_pending_ask"}
    else:
        name = _sanitize_name(request.get("from_name") or "unknown")
        header, body = _frame_parts(name, message)
        text = f"{header}\n{body}"
        if request_type == "ask":
            text += (f'\n[The sender expects a reply. Use intercom(action="reply", '
                     f'to={json.dumps(name, ensure_ascii=False)}, request_id="{request["request_id"]}", message="...").]')
        from . import desktop

        status = desktop.deliver(request.get("session_key"), text)
        response = {"ok": status in _ACCEPTED, "status": status}
    return response


def _serve(conn: socket.socket) -> None:
    try:
        conn.settimeout(10)
        if _peer_uid(conn) != os.getuid():
            raise PermissionError("peer uid does not match server uid")
        envelope = _read_frame(conn)
        request = _open(envelope)
        response = _receive(request, envelope["sender"])
        response.update(type="receipt", request_id=request.get("request_id", ""))
        _send_frame(conn, _seal(response, envelope["destination"], envelope["sender"]))
    except Exception as exception:
        logger.warning("intercom inbox error: %s", exception)
    finally:
        conn.close()


def stop() -> None:
    with _lock:
        for waiter in _state.pop("waiters", {}).values():
            waiter["evt"].set()
        _sending.clear()
        _rate.clear()
        _dedupe.clear()
        _nonces.clear()


def self_meta() -> dict:
    from . import desktop

    if desktop._context is not None:
        return desktop.self_meta()
    return {}


def _prepare_directories() -> None:
    for directory in (_base_dir(), _registry_dir()):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if directory.is_symlink() or directory.stat().st_uid != os.getuid():
            raise PermissionError("unsafe intercom directory")
        os.chmod(directory, 0o700)
    _initialize_key()


def _initialize_key() -> None:
    path = _base_dir() / "key"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        descriptor = None
    if descriptor is not None:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(os.urandom(32))
            stream.flush()
            os.fsync(stream.fileno())
    for _ in range(20):
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
                raise PermissionError("intercom key must be owner-only and owned by current uid")
            key = stream.read(33)
        if len(key) == 32:
            with _lock:
                _state["key"] = key
            return
        time.sleep(0.01)
    raise ValueError("intercom key must contain exactly 32 bytes")


def _write_registry(path: Path, metadata: dict) -> None:
    if path.parent.resolve() != _registry_dir().resolve():
        raise ValueError("registry path escapes registry")
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(metadata, stream, ensure_ascii=False)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _drop_peer(metadata: dict) -> None:
    identifier = str(metadata.get("session_id", ""))
    if re.fullmatch(r"[A-Za-z0-9_-]{1,80}", identifier):
        path = _registry_dir() / f"{identifier}.json"
        try:
            if json.loads(path.read_text()).get("socket") == metadata.get("socket"):
                path.unlink(missing_ok=True)
        except (OSError, ValueError):
            return


def _prune_windows(now: float) -> None:
    for peer in list(_rate):
        values = [timestamp for timestamp in _rate[peer] if now - timestamp < _RATE_WINDOW_S]
        if values:
            _rate[peer] = values
        else:
            del _rate[peer]
    for peer in list(_dedupe):
        values = [(digest, timestamp) for digest, timestamp in _dedupe[peer] if now - timestamp < _DEDUPE_WINDOW_S]
        if values:
            _dedupe[peer] = values
        else:
            del _dedupe[peer]


def _associated_data(envelope: dict) -> bytes:
    return json.dumps({field: envelope[field] for field in (
        "version", "sender", "destination", "type", "request_id", "timestamp")},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _seal(payload: dict, sender: str, destination: str) -> dict:
    envelope = {"version": 1, "sender": sender, "destination": destination,
                "type": payload["type"], "request_id": payload.get("request_id", ""),
                "timestamp": time.time()}
    nonce = os.urandom(12)
    plaintext = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    ciphertext = AESGCM(_state["key"]).encrypt(nonce, plaintext, _associated_data(envelope))
    return {**envelope, "nonce": nonce.hex(), "ciphertext": ciphertext.hex()}


def _open(envelope: dict, destination: str | None = None) -> dict:
    if envelope["version"] != 1:
        raise ValueError("unsupported protocol version")
    timestamp = envelope["timestamp"]
    now = time.time()
    if not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp) or abs(now - timestamp) > 120:
        raise ValueError("envelope timestamp outside allowed window")
    if not all(isinstance(envelope[field], str) and 0 < len(envelope[field]) <= 1024
               for field in ("sender", "destination", "type")):
        raise ValueError("invalid envelope identity")
    request_id = envelope["request_id"]
    if not isinstance(request_id, str) or (request_id and not re.fullmatch(r"[0-9a-f]{32}", request_id)):
        raise ValueError("invalid request id")
    if envelope["type"] in ("ask", "reply") and not request_id:
        raise ValueError("missing request id")
    nonce = bytes.fromhex(envelope["nonce"])
    if len(nonce) != 12:
        raise ValueError("invalid nonce")
    plaintext = AESGCM(_state["key"]).decrypt(nonce, bytes.fromhex(envelope["ciphertext"]), _associated_data(envelope))
    payload = json.loads(plaintext)
    if payload["type"] != envelope["type"] or payload.get("request_id", "") != request_id:
        raise ValueError("envelope metadata mismatch")
    if destination is None:
        from . import desktop

        metadata = list(desktop._metadata.values())
        matching = [peer for peer in metadata if envelope["destination"] in (
            peer.get("session_id"), peer.get("session_key"))]
        if not matching or payload.get("session_key", "") != matching[0].get("session_key", ""):
            raise ValueError("wrong destination")
    elif envelope["destination"] != destination:
        raise ValueError("wrong destination")
    with _lock:
        while _nonces and next(iter(_nonces.values())) < now:
            _nonces.popitem(last=False)
        if nonce in _nonces:
            raise ValueError("replayed nonce")
        if len(_nonces) >= _NONCE_CAP:
            raise ValueError("nonce cache full")
        _nonces[nonce] = now + 240
    return payload


def _read_exact(connection: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = connection.recv(length - len(chunks))
        if not chunk:
            raise EOFError("incomplete frame")
        chunks.extend(chunk)
    return bytes(chunks)


def _read_frame(connection: socket.socket) -> dict:
    length = struct.unpack("!I", _read_exact(connection, 4))[0]
    if length > _MAX_WIRE_BYTES or length == 0:
        raise ValueError("invalid wire size")
    return json.loads(_read_exact(connection, length))


def _send_frame(connection: socket.socket, envelope: dict) -> None:
    data = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode()
    if len(data) > _MAX_WIRE_BYTES:
        raise ValueError("invalid wire size")
    connection.sendall(struct.pack("!I", len(data)) + data)
