from __future__ import annotations

import atexit
from collections import Counter
import os
from pathlib import Path
import socket
import threading
import tempfile
import uuid

from . import inbox

_context = None
_stop = threading.Event()
_metadata = {}
_socket = None
_thread = None
_lifecycle_lock = threading.RLock()


def live_sessions():
    if _context is None or _socket is None:
        return {}
    sessions = {}
    for session in _context.live_sessions():
        session_key = session["session_key"]
        if session["surface"] not in {"cli", "tui", "desktop", "acp"} or session_key in sessions:
            continue
        sessions[session_key] = {
            "name": inbox._sanitize_name(session["title"] or session_key[-12:]),
            "pid": os.getpid(),
            "cwd": session["cwd"] or "?",
            "session_id": uuid.uuid5(uuid.NAMESPACE_URL, session_key).hex,
            "session_key": session_key,
            "socket": _socket.getsockname(),
            "turn_active": bool(session["busy"]),
            "surface": session["surface"],
        }
    names = Counter(metadata["name"] for metadata in sessions.values())
    for metadata in sessions.values():
        if names[metadata["name"]] > 1:
            metadata["name"] += "-" + metadata["session_id"][:8]
    return sessions


def refresh(active=None):
    global _metadata

    from gateway.session_context import get_session_env

    with _lifecycle_lock:
        sessions = live_sessions()
        key = get_session_env("HERMES_SESSION_KEY", "")
        if active is not None and key in sessions:
            sessions[key]["turn_active"] = active
        registry = inbox._registry_dir()
        for session_key, metadata in sessions.items():
            if _metadata.get(session_key) != metadata:
                inbox._write_registry(registry / f"{metadata['session_id']}.json", metadata)
        for session_key in _metadata.keys() - sessions.keys():
            inbox._drop_peer(_metadata[session_key])
        _metadata = sessions


def self_meta():
    from gateway.session_context import get_session_env

    with _lifecycle_lock:
        return dict(_metadata.get(get_session_env("HERMES_SESSION_KEY", "")) or {})


def deliver(session_key, text):
    if session_key not in live_sessions():
        return "rejected_retired_session"
    if not _context.inject_message(text, session_key=session_key):
        return "rejected_injection"
    return "submitted_as_turn"


def serve(listener):
    while not _stop.is_set():
        try:
            connection, _ = listener.accept()
        except OSError:
            break
        threading.Thread(target=inbox._serve, args=(connection,), daemon=True).start()


def stop():
    global _context, _socket, _thread, _metadata

    with _lifecycle_lock:
        _stop.set()
        listener, thread = _socket, _thread
        address = listener.getsockname() if listener is not None and listener.fileno() >= 0 else None
        if listener is not None:
            try:
                listener.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            listener.close()
        for metadata in _metadata.values():
            inbox._drop_peer(metadata)
        if address:
            Path(address).unlink(missing_ok=True)
        _context = None
        _socket = None
        _thread = None
        _metadata = {}
        inbox.stop()
        atexit.unregister(stop)
    if thread is not None and thread.is_alive():
        thread.join(timeout=2)


def start(context):
    global _context, _socket, _thread

    if _thread is not None and _thread.is_alive() and _context is context:
        return
    stop()
    with _lifecycle_lock:
        inbox._prepare_directories()
        _context = context
        registry = inbox._registry_dir()
        path = registry / f"gateway-{os.getpid()}.sock"
        if len(os.fsencode(path)) > 103:
            identifier = uuid.uuid5(uuid.NAMESPACE_URL, str(registry)).hex[:8]
            path = Path(tempfile.gettempdir()) / f"intercom-{os.getpid()}-{identifier}.sock"
        path.unlink(missing_ok=True)
        for stale in registry.glob("gateway-*.sock"):
            owner = stale.stem.removeprefix("gateway-")
            if not owner.isdigit():
                continue
            try:
                os.kill(int(owner), 0)
            except ProcessLookupError:
                stale.unlink(missing_ok=True)
            except PermissionError:
                continue
        _socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            _socket.bind(str(path))
            os.chmod(path, 0o600)
            _socket.listen(8)
            _stop.clear()
            _thread = threading.Thread(target=serve, args=(_socket,), name="intercom-desktop", daemon=True)
            _thread.start()
            atexit.register(stop)
            context.on_unload(stop)
        except Exception:
            _socket.close()
            _socket = None
            _context = None
            path.unlink(missing_ok=True)
            raise
