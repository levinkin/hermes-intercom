from __future__ import annotations

import importlib.util
import json
import sys
import threading
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any, cast
from unittest import mock


PACKAGE_DIR = Path(__file__).resolve().parents[1]
PACKAGE_NAME = "hermes_intercom_test"


def load_plugin(home: Path):
    constants = ModuleType("hermes_constants")
    setattr(constants, "get_hermes_home", lambda: str(home))
    sys.modules["hermes_constants"] = constants
    spec = importlib.util.spec_from_file_location(
        PACKAGE_NAME,
        PACKAGE_DIR / "__init__.py",
        submodule_search_locations=[str(PACKAGE_DIR)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[PACKAGE_NAME] = module
    spec.loader.exec_module(module)
    return module


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name) / ".hermes"
        self.home.mkdir()
        self.plugin = load_plugin(self.home)
        self.plugin.inbox._prepare_directories()

    def tearDown(self):
        self.plugin.inbox.stop()
        for name in list(sys.modules):
            if name == PACKAGE_NAME or name.startswith(f"{PACKAGE_NAME}."):
                sys.modules.pop(name, None)
        sys.modules.pop("hermes_constants", None)
        self.tmp.cleanup()

    def test_ask_sends_request_metadata(self):
        captured = {}

        def fake_send_to(target, message, from_name, from_cwd, payload_extra=None):
            assert payload_extra is not None
            captured.update(
                target=target,
                message=message,
                from_name=from_name,
                from_cwd=from_cwd,
                payload_extra=payload_extra,
            )
            self.plugin.inbox.resolve_ask(
                payload_extra["request_id"],
                "peer",
                "pong",
            )
            return {"ok": True, "receipt": "delivered"}

        with (
            mock.patch.object(
                self.plugin.inbox,
                "resolve_target",
                return_value={"ok": True, "name": "peer", "meta": {"session_id": "peer"}},
            ),
            mock.patch.object(self.plugin.inbox, "send_to", fake_send_to),
        ):
            result = self.plugin.inbox.ask_to("peer", "ping", "sender", "/tmp", timeout=10)

        self.assertEqual(
            result,
            {"ok": True, "reply": "pong", "from": "peer", "action": "ask"},
        )
        payload = cast(dict[str, Any], captured["payload_extra"])
        self.assertEqual(payload["type"], "ask")
        self.assertTrue(payload["request_id"])

    def test_request_ids_are_not_predictable_or_reused(self):
        request_ids = []

        def fake_send_to(target, message, from_name, from_cwd, payload_extra=None):
            assert payload_extra is not None
            request_ids.append(payload_extra["request_id"])
            self.plugin.inbox.resolve_ask(
                payload_extra["request_id"],
                "peer",
                "pong",
            )
            return {"ok": True, "receipt": "delivered"}

        with (
            mock.patch.object(
                self.plugin.inbox,
                "resolve_target",
                return_value={"ok": True, "name": "peer", "meta": {"session_id": "peer"}},
            ),
            mock.patch.object(self.plugin.inbox, "send_to", fake_send_to),
        ):
            first = self.plugin.inbox.ask_to("peer", "ping", timeout=10)
            second = self.plugin.inbox.ask_to("peer", "ping again", timeout=10)

        self.assertTrue(first["ok"] and second["ok"])
        self.assertEqual(len(set(request_ids)), 2)
        self.assertTrue(all(len(request_id) >= 32 for request_id in request_ids))

    def test_ask_binds_reply_to_resolved_peer_name_when_target_is_prefix(self):
        def fake_exchange(meta, payload):
            self.plugin.inbox.resolve_ask(
                payload["request_id"],
                "research-session-123",
                "pong",
            )
            return {
                "ok": True,
                "status": "submitted_as_turn",
                "to": "research-session-123",
            }

        with (
            mock.patch.object(
                self.plugin.inbox,
                "_load_peers",
                return_value={
                    "research-session-123": {"name": "research-session-123", "session_id": "research-session-123", "pid": 99999, "socket": "/tmp/peer.sock"}
                },
            ),
            mock.patch.object(self.plugin.inbox, "_exchange", fake_exchange),
        ):
            result = self.plugin.inbox.ask_to("research", "ping", timeout=10)

        self.assertEqual(result["reply"], "pong")

    def test_exact_target_wins_over_longer_prefix_match(self):
        with mock.patch.object(
            self.plugin.inbox,
            "_load_peers",
            return_value={
                "research": {"name": "research", "pid": 1},
                "research-long": {"name": "research-long", "pid": 2},
            },
        ):
            result = self.plugin.inbox.resolve_target("research")

        self.assertTrue(result["ok"])
        self.assertEqual(result["name"], "research")

    def test_ask_sends_to_once_resolved_exact_peer(self):
        sent_targets = []

        def fake_send_to(target, message, from_name, from_cwd, payload_extra=None):
            sent_targets.append(target)
            assert payload_extra is not None
            self.plugin.inbox.resolve_ask(
                payload_extra["request_id"],
                "research-full",
                "pong",
            )
            return {"ok": True, "receipt": "delivered"}

        with (
            mock.patch.object(
                self.plugin.inbox,
                "resolve_target",
                return_value={"ok": True, "name": "research-full", "meta": {"session_id": "research-full"}},
            ),
            mock.patch.object(self.plugin.inbox, "send_to", fake_send_to),
        ):
            result = self.plugin.inbox.ask_to("research", "ping", timeout=10)

        self.assertEqual(result["reply"], "pong")
        self.assertEqual(sent_targets, ["research-full"])

    def test_expected_peer_uses_same_name_normalization_as_reply(self):
        captured = {}

        def fake_send_to(target, message, from_name, from_cwd, payload_extra=None):
            captured["request_id"] = payload_extra["request_id"]
            self.plugin.inbox.resolve_ask(
                payload_extra["request_id"],
                "peername",
                "pong",
            )
            return {"ok": True, "receipt": "delivered"}

        with (
            mock.patch.object(
                self.plugin.inbox,
                "resolve_target",
                return_value={"ok": True, "name": "peer[name]", "meta": {"session_id": "peername"}},
            ),
            mock.patch.object(self.plugin.inbox, "send_to", fake_send_to),
        ):
            result = self.plugin.inbox.ask_to("peer", "ping", timeout=10)

        self.assertEqual(result["reply"], "pong")

    def test_reply_only_resolves_waiter_for_expected_peer(self):
        request_id = "0123456789abcdef0123456789abcdef"
        waiter = threading.Event()
        self.plugin.inbox._state["waiters"] = {
            request_id: {"evt": waiter, "reply": None, "expected_peer": "expected"}
        }

        rejected = self.plugin.inbox.resolve_ask(request_id, "attacker", "forged")
        accepted = self.plugin.inbox.resolve_ask(request_id, "expected", "real")

        self.assertFalse(rejected)
        self.assertTrue(accepted)
        self.assertEqual(
            self.plugin.inbox._state["waiters"][request_id]["reply"],
            "real",
        )

    def test_reply_action_routes_to_pending_request(self):
        captured = {}

        def fake_reply_to(target, request_id, message, from_name, from_cwd):
            captured.update(
                target=target,
                request_id=request_id,
                message=message,
                from_name=from_name,
                from_cwd=from_cwd,
            )
            return {"ok": True, "receipt": "delivered"}

        with (
            mock.patch.object(
                self.plugin.inbox,
                "self_meta",
                return_value={"name": "receiver", "cwd": "/tmp"},
            ),
            mock.patch.object(
                self.plugin.inbox,
                "reply_to",
                fake_reply_to,
                create=True,
            ),
        ):
            result = json.loads(
                self.plugin._handle(
                    {
                        "action": "reply",
                        "to": "sender",
                        "request_id": "0123456789abcdef0123456789abcdef",
                        "message": "pong",
                    }
                )
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            captured,
            {
                "target": "sender",
                "request_id": "0123456789abcdef0123456789abcdef",
                "message": "pong",
                "from_name": "receiver",
                "from_cwd": "/tmp",
            },
        )

    def test_ask_envelope_tells_receiver_to_use_reply_action(self):
        delivered = {}

        def capture_delivery(session_key, text):
            delivered["text"] = text
            return "submitted_as_turn"

        server, client = self.plugin.inbox.socket.socketpair()
        from hermes_intercom_test import desktop

        with mock.patch.object(desktop, "deliver", capture_delivery):
            worker = threading.Thread(target=self.plugin.inbox._serve, args=(server,))
            worker.start()
            desktop._metadata = {"receiver": {"session_id": "receiver"}}
            self.plugin.inbox._send_frame(client, self.plugin.inbox._seal({
                "type": "ask", "from_name": "sender", "from_cwd": "/tmp", "message": "ping",
                "request_id": "0123456789abcdef0123456789abcdef"}, "sender", "receiver"))
            response = self.plugin.inbox._open(self.plugin.inbox._read_frame(client), destination="sender")
            worker.join(timeout=2)
            client.close()

        self.assertTrue(response["ok"])
        self.assertIn('intercom(action="reply"', delivered["text"])
        self.assertIn('request_id="0123456789abcdef0123456789abcdef"', delivered["text"])
        self.assertIn('to="sender"', delivered["text"])

    def test_rejects_peer_with_different_uid(self):
        server, client = self.plugin.inbox.socket.socketpair()
        with mock.patch.object(
            self.plugin.inbox,
            "_peer_uid",
            return_value=self.plugin.inbox.os.getuid() + 1,
        ):
            worker = threading.Thread(target=self.plugin.inbox._serve, args=(server,))
            worker.start()
            response = client.recv(1)
            worker.join(timeout=2)
            client.close()

        self.assertEqual(response, b"")

    def test_key_and_registry_permissions_are_owner_only(self):
        self.plugin.inbox._write_registry(
            self.home / "intercom" / "sessions" / "endpoint-123.json",
            {"session_id": "endpoint-123"},
        )
        self.assertEqual((self.home / "intercom" / "key").stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.home / "intercom" / "sessions" / "endpoint-123.json").stat().st_mode & 0o777, 0o600)
        self.assertFalse((self.home / "intercom" / "pending").exists())

    def test_same_uid_socket_round_trip(self):
        delivered = {}

        def capture_delivery(session_key, text):
            delivered["text"] = text
            return "submitted_as_turn"

        from hermes_intercom_test import desktop

        desktop._metadata = {"receiver": {"session_id": "receiver-session"}}
        socket_path = str(Path(self.tmp.name) / "peer.sock")
        listener = self.plugin.inbox.socket.socket(self.plugin.inbox.socket.AF_UNIX, self.plugin.inbox.socket.SOCK_STREAM)
        listener.bind(socket_path)
        listener.listen(1)
        with mock.patch.object(desktop, "deliver", capture_delivery):
            with self.plugin.inbox.socket.socket(
                self.plugin.inbox.socket.AF_UNIX,
                self.plugin.inbox.socket.SOCK_STREAM,
            ) as client:
                client.connect(socket_path)
                connection, _ = listener.accept()
                worker = threading.Thread(target=self.plugin.inbox._serve, args=(connection,))
                worker.start()
                self.plugin.inbox._send_frame(client, self.plugin.inbox._seal({
                    "type": "send", "from_name": "sender", "from_cwd": "/tmp", "message": "ping"},
                    "sender", "receiver-session"))
                response = self.plugin.inbox._open(self.plugin.inbox._read_frame(client), destination="sender")
                worker.join(timeout=2)
        listener.close()

        self.assertTrue(response["ok"])
        self.assertEqual(response["status"], "submitted_as_turn")
        self.assertIn("ping", delivered["text"])

    def test_identical_replies_for_distinct_requests_are_not_deduplicated(self):
        captured = []

        def fake_exchange(meta, payload):
            captured.append(payload)
            return {"ok": True, "status": "reply_resolved"}

        with (
            mock.patch.object(
                self.plugin.inbox,
                "_load_peers",
                return_value={"peer": {"name": "peer", "pid": 99999, "socket": "/tmp/peer.sock"}},
            ),
            mock.patch.object(self.plugin.inbox, "_exchange", fake_exchange, create=True),
        ):
            first = self.plugin.inbox.reply_to("peer", "11111111111111111111111111111111", "same answer")
            second = self.plugin.inbox.reply_to("peer", "22222222222222222222222222222222", "same answer")

        self.assertTrue(first["ok"] and second["ok"])
        self.assertEqual([item["request_id"] for item in captured], ["11111111111111111111111111111111", "22222222222222222222222222222222"])

    def test_send_preserves_exchange_error(self):
        with (
            mock.patch.object(
                self.plugin.inbox,
                "resolve_target",
                return_value={
                    "ok": True,
                    "name": "peer",
                    "meta": {"pid": 99999, "socket": "/tmp/missing.sock"},
                },
            ),
            mock.patch.object(
                self.plugin.inbox,
                "_exchange",
                return_value={"ok": False, "error": "delivery failed: boom"},
            ),
        ):
            result = self.plugin.inbox.send_to("peer", "ping")

        self.assertEqual(result["error"], "delivery failed: boom")


if __name__ == "__main__":
    unittest.main()
