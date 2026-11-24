"""Smoke test for the installed abi3 ``zul_bridge`` extension.

Not collected by pytest (the filename does not start with ``test_``). Run it
against a CPython 3.14 environment that has the built wheel installed:

    python tests/python_smoke.py

The script starts a local stdlib HTTP fake Zulip server, isolates libzul
settings to a temporary HOME, and exercises import/version, whoami, sending,
typing, the event long-poll timeout, GIL release, close, and the
missing-configuration error.
"""

import json
import os
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def isolate_settings():
    """Keep ambient libzul config and ZULIP_* env out of the test."""
    root = tempfile.mkdtemp(prefix="zul-bridge-smoke-")
    os.environ["HOME"] = root
    os.environ["XDG_CONFIG_HOME"] = os.path.join(root, ".config")
    os.environ["XDG_CACHE_HOME"] = os.path.join(root, ".cache")
    for key in (
        "ZULIP_BASE_URL",
        "ZULIP_SITE",
        "ZULIP_EMAIL",
        "ZULIP_API_KEY",
        "ZUL_CACHE",
    ):
        os.environ.pop(key, None)
    return root


isolate_settings()

import zul_bridge  # noqa: E402  (imported after settings isolation)

LOCK = threading.Lock()
STATE = {
    "registered": 0,
    "deleted": 0,
    "sent": 0,
    "typing": 0,
    "events": 0,
    "message_form": None,
    "typing_forms": [],
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _json(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _form(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        return urllib.parse.parse_qs(body.decode())

    def do_POST(self):
        form = self._form()
        if self.path == "/api/v1/register":
            with LOCK:
                STATE["registered"] += 1
            self._json(
                {
                    "result": "success",
                    "msg": "",
                    "queue_id": "q-smoke",
                    "last_event_id": 0,
                    "event_queue_longpoll_timeout_seconds": 30,
                }
            )
        elif self.path == "/api/v1/messages":
            with LOCK:
                STATE["sent"] += 1
                STATE["message_form"] = form
            self._json({"result": "success", "msg": "", "id": 99})
        elif self.path == "/api/v1/typing":
            with LOCK:
                STATE["typing"] += 1
                STATE["typing_forms"].append(form)
            self._json({"result": "success", "msg": ""})
        else:
            self._json({"result": "error", "msg": "not found"}, 404)

    def do_GET(self):
        if self.path.startswith("/api/v1/users/me"):
            self._json(
                {
                    "result": "success",
                    "msg": "",
                    "email": "ada@example.com",
                    "full_name": "Ada Lovelace",
                }
            )
        elif self.path.startswith("/api/v1/events"):
            with LOCK:
                STATE["events"] += 1
            time.sleep(0.02)
            self._json(
                {
                    "result": "success",
                    "msg": "",
                    "queue_id": "q-smoke",
                    "events": [],
                }
            )
        else:
            self._json({"result": "error", "msg": "not found"}, 404)

    def do_DELETE(self):
        self._form()
        with LOCK:
            STATE["deleted"] += 1
        self._json({"result": "success", "msg": ""})


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass


def main():
    assert isinstance(zul_bridge.__version__, str) and zul_bridge.__version__

    status = zul_bridge.config_status()
    assert set(status) == {"base_url", "email", "api_key"}, status
    assert all(isinstance(value, bool) for value in status.values()), status
    assert not any(status.values()), "settings were not isolated: {status}"

    server = Server(("127.0.0.1", 0), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"

    bridge = zul_bridge.Bridge(
        base_url=base_url,
        email="ada@example.com",
        api_key="secret",
    )
    try:
        assert bridge.whoami() == {
            "email": "ada@example.com",
            "full_name": "Ada Lovelace",
        }
        assert bridge.send_stream("core", "ops", "hello") == 99
        bridge.typing_stream(17, "ops", "start")
        bridge.typing_direct([7, 8], "stop")
        assert bridge.next_event(50) is None

        ran = threading.Event()

        def set_ran():
            time.sleep(0.05)
            ran.set()

        threading.Thread(target=set_ran, daemon=True).start()
        assert bridge.next_event(300) is None
        assert ran.is_set(), "next_event held the GIL while waiting"

        bridge.close()
        bridge.close()
    finally:
        bridge.close()
        server.shutdown()
        server.server_close()

    with LOCK:
        registered = STATE["registered"]
        deleted = STATE["deleted"]
        sent = STATE["sent"]
        typing = STATE["typing"]
        message_form = STATE["message_form"]
        typing_forms = list(STATE["typing_forms"])

    assert registered >= 1, STATE
    assert deleted == 1, "close must delete the queue once and be idempotent"
    assert sent == 1, STATE
    assert message_form["type"] == ["stream"], message_form
    assert message_form["to"] == ["core"], message_form
    assert message_form["topic"] == ["ops"], message_form
    assert message_form["content"] == ["hello"], message_form
    assert typing == 2, STATE
    assert {
        "type": ["stream"],
        "op": ["start"],
        "to": ["17"],
        "topic": ["ops"],
    } in typing_forms, typing_forms
    assert {
        "type": ["direct"],
        "op": ["stop"],
        "to": ["[7,8]"],
    } in typing_forms, typing_forms

    try:
        zul_bridge.Bridge()
    except ValueError as exc:
        text = str(exc)
        for field in ("base_url", "email", "api_key"):
            assert field in text, text
    else:
        raise AssertionError("missing configuration must raise ValueError")

    print(f"python smoke ok (zul_bridge {zul_bridge.__version__})")


if __name__ == "__main__":
    main()
