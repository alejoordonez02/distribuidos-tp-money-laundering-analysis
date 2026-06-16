import os


def restart_line() -> str:
    """Per-node restart policy. OFF by default so the SUPERVISOR is the sole
    reviver of crashed nodes (heartbeat detection + docker start). The ft e2e
    sets GEN_RESTART_ON_FAILURE=1 so its self-crash points recover via Docker."""
    if os.getenv("GEN_RESTART_ON_FAILURE") == "1":
        return "\n    restart: on-failure"
    return ""
