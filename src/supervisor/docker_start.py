import subprocess


def _docker_start(node_id: str) -> None:
    # recovery (not detection) may use Docker: restart the container reusing its state volume to restore from checkpoint
    subprocess.run(
        ["docker", "start", node_id],
        check=False,
        capture_output=True,
        timeout=15,
    )
