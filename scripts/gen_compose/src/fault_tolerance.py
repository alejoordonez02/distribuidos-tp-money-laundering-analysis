"""Single place to tune fault tolerance: heartbeats, detection, revival and the
chaos monkey. These are the defaults baked into the generated compose; each can
still be overridden at runtime via the matching env var (e.g. CHAOS_INTERVAL=10
make chaos)."""

# --- supervisor / heartbeats ---
SUPERVISOR_HOST = "supervisor"
SUPERVISOR_PORT = 9100
HEARTBEAT_INTERVAL = 1  # how often each node sends a heartbeat (s)
HEARTBEAT_TIMEOUT = 3   # no heartbeat for this long -> node marked dead (s)
REVIVE_INTERVAL = 2     # how often the supervisor scans and revives dead nodes (s); 0 = detect only

# --- chaos monkey (stays off unless CHAOS_ENABLED=1) ---
CHAOS_INTERVAL = 4   # seconds between kill waves
CHAOS_KILLS_MIN = 1  # fewest nodes killed per wave
CHAOS_KILLS_MAX = 8  # most nodes killed per wave
CHAOS_START_DELAY = 5  # grace before the first wave so the cluster can form (s)
CHAOS_EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
