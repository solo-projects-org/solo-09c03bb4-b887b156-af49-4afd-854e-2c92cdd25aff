"""Test-only crash injection points.

Hooks are no-ops unless CRASH_HOOK matches the hook name. Production
deployments never set CRASH_HOOK, so these calls cost a single dictionary
lookup and have no effect on the normal code paths.

CRASH_HOOK_AFTER optionally makes a hook fire only after that many hits
(1-based), which lets a test let the first matching operation succeed and
kill the process on the second.
"""
from __future__ import annotations

import os
import sys

_hits: dict[str, int] = {}


def crash_if(point: str) -> None:
    if os.environ.get("CRASH_HOOK") != point:
        return
    _hits[point] = _hits.get(point, 0) + 1
    after = int(os.environ.get("CRASH_HOOK_AFTER", "1"))
    if _hits[point] < after:
        return
    # Hard exit, mimicking SIGKILL / power loss: no cleanup, no flushing.
    sys.stderr.write(
        f"[crashhook] injected exit at {point} (hit {_hits[point]})\n")
    sys.stderr.flush()
    os._exit(137)
