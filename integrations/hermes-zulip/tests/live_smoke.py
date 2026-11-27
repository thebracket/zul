"""Live smoke test for the installed ``zul_bridge`` extension and real credentials.

Not collected by pytest (the filename does not start with ``test_``).  Load a
``.env`` that holds the Zulip credentials, then run it with the interpreter that
has the wheel installed:

    set -a; . ./.env; set +a
    integrations/hermes-zulip/.venv/bin/python integrations/hermes-zulip/tests/live_smoke.py

Explicit ``ZULIP_*`` environment values win; otherwise the bridge reads
libzul's settings file.  The script authenticates, prints the account display
name and the realm host, polls the event queue for one short timeout, and
closes.  It sends nothing and never prints the API key, the account email, or
message content.
"""

from __future__ import annotations

import os
import sys
from urllib.parse import urlsplit

import zul_bridge

POLL_TIMEOUT_MS = 1000


def _realm_host() -> str:
    """Host part of the configured realm URL, without credentials, path, or query."""
    base = (os.environ.get("ZULIP_BASE_URL") or os.environ.get("ZULIP_SITE") or "").strip()
    if not base:
        return "(from libzul settings)"
    return urlsplit(base).hostname or "(unparsed realm URL)"


def _redact(text: str, secret: str) -> str:
    if secret:
        text = text.replace(secret, "<redacted>")
    return text


def main() -> int:
    secret = os.environ.get("ZULIP_API_KEY", "")
    bridge = None
    try:
        bridge = zul_bridge.Bridge()
        account = bridge.whoami()
        name = str(account.get("full_name") or "(unnamed account)")
        print(f"live smoke: authenticated as {name} at {_realm_host()}")

        event = bridge.next_event(POLL_TIMEOUT_MS)
        if event is None:
            print(f"live smoke: no event within {POLL_TIMEOUT_MS} ms")
        else:
            kind = event.get("kind") if isinstance(event, dict) else None
            print(f"live smoke: event received ({kind or 'unknown kind'})")

        print("live smoke ok")
        return 0
    except Exception as exc:
        print(
            f"live smoke failed: {type(exc).__name__}: {_redact(str(exc), secret)}",
            file=sys.stderr,
        )
        return 1
    finally:
        if bridge is not None:
            try:
                bridge.close()
            except Exception as exc:  # noqa: BLE001 - report failures without failing the run
                print(f"live smoke: close failed: {type(exc).__name__}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
