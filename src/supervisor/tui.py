import re
import threading
import time

from .registry import NodeRegistry, Status

_RESET = "\033[0m"
_CLEAR = "\033[2J\033[3J\033[H"  # clear screen + scrollback, home cursor
_HIDE_CURSOR = "\033[?25l"
_SHOW_CURSOR = "\033[?25h"
_COLORS = {Status.ALIVE: "\033[32m", Status.DEAD: "\033[31m", Status.UNKNOWN: "\033[33m"}
_ANSI = re.compile(r"\033\[[0-9;]*m")

_NODE_W = 56  # visible width of the node column before the divider


def _vis_len(s: str) -> int:
    return len(_ANSI.sub("", s))


def _pad(s: str, width: int) -> str:
    return s + " " * max(0, width - _vis_len(s))


def _age(last_hb, now: float) -> str:
    return "-" if last_hb is None else f"{now - last_hb:.1f}s"


class Dashboard:
    """Live ANSI dashboard for the supervisor. Clears and redraws each frame in
    place, with the node table beside its recent events (the event column is
    capped to the node count so the frame keeps a fixed height). Dependency-free
    so it runs on the bare python:alpine image."""

    def __init__(self, registry: NodeRegistry, refresh: float = 1.0):
        self._registry = registry
        self._refresh = refresh

    def run(self, stop: threading.Event) -> None:
        print(_HIDE_CURSOR, end="", flush=True)
        try:
            while not stop.is_set():
                print(self.render(), end="", flush=True)
                stop.wait(self._refresh)
        finally:
            print(_SHOW_CURSOR, flush=True)

    def render(self) -> str:
        nodes, events = self._registry.snapshot()
        nodes.sort(key=lambda n: n.node_id)
        now = time.monotonic()
        alive = sum(1 for n in nodes if n.status is Status.ALIVE)
        dead = sum(1 for n in nodes if n.status is Status.DEAD)

        node_cells = [self._node_cell(n, now) for n in nodes]
        # only the newest events, as many as there are nodes, so both columns stay the same height
        event_cells = [self._event_cell(e) for e in reversed(events)][: len(node_cells)]

        rows = [
            "SUPERVISOR — node liveness (heartbeat-driven)   "
            f"total={len(nodes)}  alive={alive}  dead={dead}",
            "",
            _pad(f"{'NODE':<30}{'KIND':<11}{'STATUS':<8}{'HB':<7}", _NODE_W) + " │ EVENTS (newest first)",
            "-" * _NODE_W + "-┼-" + "-" * 40,
        ]
        for i in range(len(node_cells)):
            right = event_cells[i] if i < len(event_cells) else ""
            rows.append(_pad(node_cells[i], _NODE_W) + " │ " + right)

        return _CLEAR + "\n".join(rows)

    def _node_cell(self, node, now: float) -> str:
        color = _COLORS.get(node.status, "")
        status = f"{color}{node.status.value:<8}{_RESET}"
        return f"{node.node_id:<30.30}{node.kind:<11.11}{status}{_age(node.last_heartbeat, now):<7}"

    def _event_cell(self, event) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(event.at))
        return f"[{ts}] {event.node_id}: {event.message}"[:46]
