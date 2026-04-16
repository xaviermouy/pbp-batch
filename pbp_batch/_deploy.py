"""
Infrastructure lifecycle management for pbp-batch.

Uses Prefect's flow.serve() model: a single background process registers the
deployment, polls for queued runs, and executes them (limit=1 = sequential queue).
No work pool, no remote storage, no separate worker needed.

PID files are stored in ~/.pbp_batch/ so processes survive terminal sessions.
"""

import asyncio
import os
import threading
import subprocess
import sys
import time
import webbrowser
from pathlib import Path
import urllib.request


def _run_async(coro):
    """Run an async coroutine safely from any thread context.

    asyncio.run() and loop.run_until_complete() both raise if the calling
    thread already has a running event loop (e.g. Panel's Tornado thread pool).
    Spawning a plain daemon thread guarantees a clean thread with no loop,
    and avoids the atexit conflicts that ThreadPoolExecutor introduces.
    """
    result = [None]
    exc    = [None]

    def _run():
        try:
            result[0] = asyncio.run(coro)
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join()

    if exc[0] is not None:
        raise exc[0]
    return result[0]

# All Prefect processes must connect to our persistent server (port 4200).
# PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=false prevents Prefect 3.x from silently
# starting its own ephemeral server on a random port — which would cause the
# CLI and the serve process to end up on different servers and never communicate.
PREFECT_API_URL = "http://127.0.0.1:4200/api"
os.environ["PREFECT_API_URL"] = PREFECT_API_URL
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "false"

SERVE_NAME = "pbp-batch"
FLOW_NAME = "pbp-job"
PID_DIR = Path.home() / ".pbp_batch"
_NTFY_TOPIC_FILE = PID_DIR / "ntfy_topic.txt"


_NTFY_DEFAULT_TOPIC = "pbp-runs-xavier-whoi"


def _read_ntfy_topic() -> str:
    """Return the saved ntfy topic.

    - File absent         → default topic (notifications on by default)
    - File present, empty → explicitly disabled
    - File present        → saved topic
    """
    try:
        return _NTFY_TOPIC_FILE.read_text().strip()
    except FileNotFoundError:
        return _NTFY_DEFAULT_TOPIC


def set_ntfy_topic(topic: str) -> None:
    """Save the ntfy topic to disk. Restart the serve process to apply."""
    PID_DIR.mkdir(exist_ok=True)
    if topic:
        _NTFY_TOPIC_FILE.write_text(topic.strip())
        print(f"Notification topic set to: {topic.strip()}")
    else:
        _NTFY_TOPIC_FILE.write_text("")  # empty file = explicitly disabled
        print("Notifications disabled.")
    if is_serve_running():
        print("Note: restart the serve process to apply — run 'pbp-batch stop' then resubmit a job.")


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

def is_server_running() -> bool:
    try:
        with urllib.request.urlopen(f"{PREFECT_API_URL}/health", timeout=2) as resp:
            return resp.status == 200
    except Exception:
        return False


def _log_path(name: str) -> Path:
    PID_DIR.mkdir(exist_ok=True)
    return PID_DIR / f"{name}.log"


def _start_background_process(cmd: list, log_name: str, extra_env: dict = None) -> int:
    """Start a detached process independent of the calling terminal. Returns PID."""
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    log_file = open(_log_path(log_name), "w")
    kwargs = dict(stdout=log_file, stderr=log_file, env=env)
    if os.name == "nt":
        kwargs["creationflags"] = (
            subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        )
    else:
        kwargs["start_new_session"] = True
    proc = subprocess.Popen(cmd, **kwargs)
    return proc.pid


def start_server_background():
    PID_DIR.mkdir(exist_ok=True)
    pid = _start_background_process(
        [sys.executable, "-m", "prefect", "server", "start"],
        log_name="server",
        extra_env={
            "PREFECT_FLOW_RUN_CRASH_ON_NO_HEARTBEAT": "true",
            "PREFECT_API_SERVICES_SCHEDULER_LOOP_SECONDS": "15",
            "PREFECT_API_SERVICES_LATE_RUNS_LOOP_SECONDS": "15",
        },
    )
    (PID_DIR / "server.pid").write_text(str(pid))
    print("Starting Prefect server", end="", flush=True)
    for _ in range(30):
        time.sleep(1)
        print(".", end="", flush=True)
        if is_server_running():
            print(" ready.")
            return
    print(" timed out. Check logs.")


def ensure_server_running():
    if not is_server_running():
        start_server_background()


# ---------------------------------------------------------------------------
# Process liveness
# ---------------------------------------------------------------------------

def _process_alive(pid: int) -> bool:
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True
        )
        return str(pid) in result.stdout
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


# ---------------------------------------------------------------------------
# Serve process  (replaces work pool + worker + deployment registration)
# ---------------------------------------------------------------------------

def is_serve_running() -> bool:
    pid_file = PID_DIR / "serve.pid"
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text().strip())
        if _process_alive(pid):
            return True
        pid_file.unlink(missing_ok=True)
        return False
    except Exception:
        return False


def start_serve_background():
    PID_DIR.mkdir(exist_ok=True)
    serve_script = str(Path(__file__).parent / "_serve.py")
    pid = _start_background_process(
        [sys.executable, serve_script],
        log_name="serve",
        extra_env={
            "PREFECT_API_URL": PREFECT_API_URL,
            "PREFECT_SERVER_ALLOW_EPHEMERAL_MODE": "false",
            "NTFY_TOPIC": _read_ntfy_topic(),
        },
    )
    (PID_DIR / "serve.pid").write_text(str(pid))
    print(f"Serve process started (PID {pid}).")


# ---------------------------------------------------------------------------
# Deployment readiness
# ---------------------------------------------------------------------------

async def _deployment_exists() -> bool:
    from prefect.client.orchestration import get_client
    async with get_client() as client:
        try:
            await client.read_deployment_by_name(f"{FLOW_NAME}/{SERVE_NAME}")
            return True
        except Exception:
            return False


def _wait_for_deployment(timeout: int = 30):
    """Poll until flow.serve() has registered the deployment with the server."""
    print("Waiting for deployment to register", end="", flush=True)
    for _ in range(timeout):
        time.sleep(1)
        print(".", end="", flush=True)
        if _run_async(_deployment_exists()):
            print(" ready.")
            return
    print(" timed out.")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _configure_prefect_profile():
    """
    Write settings into the active Prefect profile so they persist across all
    CLI invocations and spawned processes:
      - PREFECT_API_URL: point all clients at our persistent server (port 4200)
      - PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=false: forbid Prefect from silently
        starting its own ephemeral server on a random port
    """
    subprocess.run(
        [sys.executable, "-m", "prefect", "config", "set",
         f"PREFECT_API_URL={PREFECT_API_URL}"],
        capture_output=True,
    )
    subprocess.run(
        [sys.executable, "-m", "prefect", "config", "set",
         "PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=false"],
        capture_output=True,
    )


def ensure_infrastructure():
    """
    Silently bring up everything needed before queuing a run:
      1. Prefect profile — configured to point at port 4200
      2. Prefect server  — starts if not running, waits until healthy
      3. Serve process   — starts if not running, waits until deployment is registered
    Safe to call repeatedly — each step is a no-op when already in place.
    """
    _configure_prefect_profile()
    ensure_server_running()
    if not is_serve_running():
        start_serve_background()
        _wait_for_deployment()


# ---------------------------------------------------------------------------
# Queue a run
# ---------------------------------------------------------------------------

async def _run_deployment_async(name: str, parameters: dict, flow_run_name: str):
    from prefect.client.orchestration import get_client
    async with get_client() as client:
        deployment = await client.read_deployment_by_name(name)
        await client.create_flow_run_from_deployment(
            deployment.id,
            parameters=parameters,
            name=flow_run_name,
        )


def queue_run(yaml_file: Path, dask_workers: int, compress_netcdf: bool,
              add_quality_flag: bool, exclude_tone_calibration: int = 0, notifications: bool = True):
    _run_async(
        _run_deployment_async(
            name=f"{FLOW_NAME}/{SERVE_NAME}",
            parameters={
                "yaml_file": str(yaml_file),
                "dask_workers": dask_workers,
                "compress_netcdf": compress_netcdf,
                "add_quality_flag": add_quality_flag,
                "exclude_tone_calibration": exclude_tone_calibration,
                "notifications": notifications,
            },
            flow_run_name=yaml_file.stem,
        )
    )
    print(f"Run queued for '{yaml_file.name}'.")
    print(f"  Prefect UI:     http://127.0.0.1:4200")
    print(f"  Dask dashboard: http://localhost:8787  (live during batch processing step only)")


# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------

def _kill_pid_file(pid_file: Path, label: str):
    if not pid_file.exists():
        return
    try:
        pid = int(pid_file.read_text().strip())
        if os.name == "nt":
            # /T kills the full process tree; /F forces termination
            result = subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True,
            )
            if result.returncode != 0:
                print(f"Could not stop {label} (PID {pid}): {result.stderr.decode().strip()}")
                return
        else:
            import signal
            os.kill(pid, signal.SIGTERM)
        print(f"{label} stopped (PID {pid}).")
    except Exception as e:
        print(f"Could not stop {label}: {e}")
    pid_file.unlink(missing_ok=True)


def _kill_port(port: int, label: str) -> bool:
    """Kill whatever process is listening on localhost:port. Returns True if anything was killed."""
    if os.name == "nt":
        result = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
        pids = set()
        for line in result.stdout.splitlines():
            if f":{port} " in line and "LISTENING" in line:
                try:
                    pids.add(int(line.split()[-1]))
                except ValueError:
                    pass
        if not pids:
            return False
        for pid in pids:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True)
        print(f"{label} stopped (port {port}).")
        return True
    else:
        result = subprocess.run(["lsof", "-ti", f":{port}"], capture_output=True, text=True)
        pids = [int(p) for p in result.stdout.split() if p.strip()]
        if not pids:
            return False
        import signal
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        print(f"{label} stopped (port {port}).")
        return True


def stop_all():
    """Stop the GUI, serve process, and Prefect server."""
    _kill_port(5007, "GUI server")
    (PID_DIR / "gui.pid").unlink(missing_ok=True)
    _kill_pid_file(PID_DIR / "serve.pid", "Serve process")
    _kill_pid_file(PID_DIR / "server.pid", "Server")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

async def _get_flow_runs_async() -> None:
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import (
        FlowRunFilter,
        FlowRunFilterDeploymentId,
        FlowRunFilterState,
        FlowRunFilterStateType,
    )
    from prefect.client.schemas.objects import StateType

    async with get_client() as client:
        try:
            deployment = await client.read_deployment_by_name(f"{FLOW_NAME}/{SERVE_NAME}")
        except Exception:
            print("  Deployment not yet registered (serve process may still be starting).")
            return

        flow_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                deployment_id=FlowRunFilterDeploymentId(any_=[deployment.id]),
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(
                        any_=[StateType.RUNNING, StateType.SCHEDULED, StateType.PENDING]
                    )
                ),
            )
        )

    running = [r for r in flow_runs if r.state and r.state.type.value == "RUNNING"]
    queued  = [r for r in flow_runs if r.state and r.state.type.value in ("SCHEDULED", "PENDING")]

    print(f"  Running   ({len(running)}):")
    for r in running:
        print(f"    - {r.name}")
    if not running:
        print("    (none)")

    print(f"  Scheduled ({len(queued)}):")
    for r in queued:
        print(f"    - {r.name}")
    if not queued:
        print("    (none)")


def _is_port_open(port: int) -> bool:
    """Return True if something is listening on localhost:port."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            s.connect(("127.0.0.1", port))
            return True
        except (ConnectionRefusedError, OSError):
            return False


def status() -> None:
    server_ok = is_server_running()
    serve_ok  = is_serve_running()
    gui_ok    = _is_port_open(5007)
    dask_ok   = _is_port_open(8787)

    topic = _read_ntfy_topic()
    print(f"Prefect server : {'running  →  http://127.0.0.1:4200' if server_ok else 'stopped'}")
    print(f"Serve process  : {'running' if serve_ok else 'stopped'}")
    print(f"GUI            : {'running  →  http://localhost:5007'  if gui_ok  else 'stopped'}")
    print(f"Dask dashboard : {'running  →  http://localhost:8787'  if dask_ok else 'stopped  (live during batch processing only)'}")
    print(f"Notifications  : {topic if topic else 'disabled  (set with: pbp-batch set-ntfy-topic <topic>)'}")

    if not server_ok:
        return

    print("Flow runs:")
    _run_async(_get_flow_runs_async())


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

def show_logs(lines: int = 50, follow: bool = False):
    """Print the last N lines of the serve log, optionally following new output."""
    log_file = _log_path("serve")
    if not log_file.exists():
        print(f"No log file found at {log_file}. Has the serve process been started?")
        return

    with open(log_file, "r", errors="replace") as f:
        all_lines = f.readlines()

    for line in all_lines[-lines:]:
        print(line, end="")

    if follow:
        print(f"\n--- following {log_file} (Ctrl+C to stop) ---\n")
        with open(log_file, "r", errors="replace") as f:
            f.seek(0, 2)  # jump to end
            try:
                while True:
                    line = f.readline()
                    if line:
                        print(line, end="", flush=True)
                    else:
                        time.sleep(0.2)
            except KeyboardInterrupt:
                pass


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

def open_ui():
    webbrowser.open("http://127.0.0.1:4200")
    print("Prefect UI opened at http://127.0.0.1:4200")
